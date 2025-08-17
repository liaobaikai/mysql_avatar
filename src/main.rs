use std::{
    fs::File, io::{self, BufReader, Read, Seek, SeekFrom}, path::Path, sync::Arc, thread, time::{Duration, SystemTime, UNIX_EPOCH}
};

use anyhow::{Result, anyhow};
use bytes::{Buf, BufMut, BytesMut};
use chrono::{DateTime, Local, Utc};
use mysql_async::{
    binlog::{
        BinlogVersion, EventFlags,
        events::{BinlogEventHeader, EventData},
    },
    consts::{CapabilityFlags, StatusFlags},
};
use mysql_common::{
    binlog::{BinlogEvent, BinlogFile, BinlogFileHeader},
    io::ParseBuf,
    misc::raw::{RawBytes, RawInt, bytes::EofBytes},
    packets::{AuthPlugin, ComBinlogDump, ComBinlogDumpGtid, SqlState},
    proto::{MyDeserialize, MySerialize, codec::PacketCodec},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::sleep,
};

use crate::{
    auth::{handshake::Handshake, Scramble}, command::handle_command_query, consts::Command, packets::{ErrPacket, HandshakePacket, HandshakeResponse, OkPacket, ServerError}, variable::{Variable, SYSVARS}
};
// mod codec;
mod command;
mod consts;
mod packets;
mod variable;
mod com;
mod auth;

// const SCRAMBLE_BUFFER_SIZE: usize = 20;
// const SERVER_VERSION: &str = "8.0.41";

// 协议版本：10
const PROTOCOL_VERSION: u8 = 10;
// 默认语言
const SERVER_LANGUAGE: u8 = 8;
// 缓冲区大小
const READ_BUFFER_SIZE: usize = 0xFFFF;

// #[inline]
// fn server_def_capabilities() -> CapabilityFlags {
//     CapabilityFlags::CLIENT_PROTOCOL_41
//         | CapabilityFlags::CLIENT_PLUGIN_AUTH
//         | CapabilityFlags::CLIENT_SECURE_CONNECTION
//         | CapabilityFlags::CLIENT_CONNECT_WITH_DB
//         | CapabilityFlags::CLIENT_TRANSACTIONS
//         | CapabilityFlags::CLIENT_CONNECT_ATTRS
//         // | CapabilityFlags::CLIENT_QUERY_ATTRIBUTES
//         | CapabilityFlags::CLIENT_DEPRECATE_EOF
// }

fn get_server_status() -> StatusFlags {
    StatusFlags::SERVER_STATUS_AUTOCOMMIT
}

// Build Handshake Packet
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
// fn new_handshake_packet(scramble: &[u8; SCRAMBLE_BUFFER_SIZE], connection_id: u32) -> BytesMut {
//     let mut scramble_1 = [0u8; 8];
//     let (_scramble_1, scramble_2) = scramble.split_at(8);
//     scramble_1.copy_from_slice(&_scramble_1);

//     let packet = HandshakePacket::new(
//         PROTOCOL_VERSION,
//         SERVER_VERSION.as_bytes(),
//         connection_id,
//         scramble_1,
//         Some(scramble_2),
//         server_def_capabilities(),
//         SERVER_LANGUAGE,
//         get_server_status(),
//         AuthPlugin::MysqlNativePassword.as_bytes().into(),
//     );

//     let mut src = BytesMut::new();
//     let mut buf: Vec<u8> = Vec::new();
//     packet.serialize(&mut buf);
//     src.extend_from_slice(&buf);
//     src
// }

// 接收并解析客户端握手响应
async fn handle_handshake_response<'a>(
    src: &mut BytesMut,
    scramble: &[u8],
    client_capabilities: &mut CapabilityFlags,
) -> Result<bool> {
    println!("resp::src: {:?}", src);
    println!("resp::src:vec: {:?}", src.to_vec());

    let resp = HandshakeResponse::deserialize((), &mut ParseBuf(&src))?;

    println!("{:?}", resp);

    let mut valid_pass = false;
    let user = String::from_utf8_lossy(resp.user()).to_string();
    let empty_pass = if resp.scramble_buf().len() > 0 {
        "YES"
    } else {
        "NO"
    };

    client_capabilities.insert(resp.capabilities());

    if resp
        .capabilities()
        .contains(CapabilityFlags::CLIENT_PLUGIN_AUTH)
    {
        if let Some(auth_plugin) = resp.auth_plugin() {
            if let Some(data) = auth_plugin.gen_data(Some("123456"), scramble) {
                match data {
                    mysql_common::packets::AuthPluginData::Old(_p) => {}
                    mysql_common::packets::AuthPluginData::Native(p) => {
                        println!("local_passwd: {:?}", p);
                        println!("scramble_buf: {:?}", resp.scramble_buf());
                        valid_pass = p == resp.scramble_buf();
                    }
                    mysql_common::packets::AuthPluginData::Sha2(p) => {
                        println!("local_passwd: {:?}", p);
                        println!("scramble_buf: {:?}", resp.scramble_buf());
                        valid_pass = p == resp.scramble_buf()
                    }
                    mysql_common::packets::AuthPluginData::Clear(_p) => {}
                    mysql_common::packets::AuthPluginData::Ed25519(_p) => {}
                }
            }
        }
    }

    src.clear();
    let mut buf: Vec<u8> = Vec::new();
    if valid_pass {
        OkPacket::new(
            0,
            0,
            get_server_status(),
            0,
            String::new().as_bytes(),
            String::new().as_bytes(),
            *client_capabilities,
            false,
        )
        .serialize(&mut buf);
    } else {
        ErrPacket::Error(ServerError::new(
            1045,
            Some(SqlState::new(*b"28000")),
            format!(
                "Access denied for user '{}'@'localhost' (using password: {})",
                user, empty_pass
            )
            .as_bytes(),
            *client_capabilities,
        ))
        .serialize(&mut buf);
    }
    src.extend_from_slice(&buf);
    Ok(valid_pass)
}

// 处理命令
fn handle_command_response(
    src: &mut BytesMut,
    client_capabilities: &CapabilityFlags,
    session_vars: &mut Vec<Variable>,
) -> Result<(Vec<BytesMut>, Command)> {
    println!("src::.length::{}", src.len());
    let mut buf = ParseBuf(&src);
    let command: RawInt<u8> = buf.parse(())?;
    // println!("command: {:?}", *command);
    let command = Command::try_from(*command)?;
    println!("command: {:?}", command);
    match command {
        Command::COM_QUERY => {
            let query: RawBytes<EofBytes> = buf.parse(())?;
            let ret = handle_command_query(&query.as_str(), session_vars, client_capabilities)?;
            return Ok((ret, command));
        }
        Command::COM_QUIT => {
            // Closed
            return Err(anyhow!("Connection closed"));
        }
        Command::COM_REGISTER_SLAVE => {}
        Command::COM_BINLOG_DUMP | Command::COM_BINLOG_DUMP_GTID => return Ok((vec![], command)),
        _ => {}
    }

    let mut buf: Vec<u8> = Vec::new();
    OkPacket::new(
        0,
        0,
        get_server_status(),
        0,
        String::new().as_bytes(),
        String::new().as_bytes(),
        *client_capabilities,
        false,
    )
    .serialize(&mut buf);

    src.clear();
    src.extend_from_slice(&buf);
    Ok((vec![std::mem::take(src)], command))
}

enum ServerPhaseDesc {
    InitialHandshakePacket,
    ClientResponse, // Read
    ServerResponse, // Write
    ClientHandshakeResponse,
    #[allow(unused)]
    AuthenticationMethodSwitch,
    #[allow(unused)]
    AuthenticationExchangeContinuation,
    Command,
}

#[derive(Default, Debug)]
struct BinlogStreamState {
    filename: String,
    pos: u64,
    local_file_pos: u64,
    master_pos: u64,
}

fn rotate_binlog_filename(bss: &mut BinlogStreamState) -> Result<String> {
    let relaylog_index = match option_env!("RELAYLOG_INDEX") {
        Some(p) => p,
        None => "C:\\Users\\BK-liao\\Desktop\\mysql-semi-test\\data\\binlog\\mysql-bin.index",
    };

    let mut file = match File::open(&relaylog_index) {
        Ok(f) => f,
        Err(e) => {
            return Err(anyhow!("{relaylog_index}: {e}"));
        }
    };

    // 查找relaylog index文件判断 filename 是否存在
    let mut filename = String::new();
    let mut filename_buf = String::new();
    file.read_to_string(&mut filename_buf).unwrap();

    let mut matched = false;
    for p in filename_buf.lines() {
        let local_filename = Path::new(p).file_name().unwrap().to_string_lossy();
        if matched {
            filename.push_str(&local_filename);
            break;
        }
        if local_filename == bss.filename {
            matched = true;
        }
    }

    if filename.is_empty() {
        return Err(anyhow!("Filename rotate failed",));
    }

    Ok(filename)
}

fn get_filename_from_relaylog_index(request_filename: &str) -> Result<String> {
    let relaylog_index = match option_env!("RELAYLOG_INDEX") {
        Some(p) => p,
        None => "C:\\Users\\BK-liao\\Desktop\\mysql-semi-test\\data\\binlog\\mysql-bin.index",
    };

    let mut file = match File::open(&relaylog_index) {
        Ok(f) => f,
        Err(e) => {
            return Err(anyhow!("{relaylog_index}: {e}"));
        }
    };

    // 查找relaylog index文件判断 filename 是否存在
    let mut filename = String::new();
    let mut filename_buf = String::new();
    file.read_to_string(&mut filename_buf).unwrap();
    for p in filename_buf.lines() {
        let local_filename = Path::new(p).file_name().unwrap().to_string_lossy();
        if local_filename == request_filename {
            filename.push_str(&p);
            break;
        }
    }

    if filename.is_empty() {
        return Err(anyhow!(
            "Request filename {} not in index file list",
            request_filename
        ));
    }

    Ok(filename)
}

// 一直读不到有新的binlog的话，就退出等待，从库会进行重连
const BINLOG_IDLE_RETRY_COUNT: usize = 10;
const BINLOG_CHUNK_SIZE: usize = 0xFFFF;

async fn handle_binlog_dump(
    binlog_stream_state: &mut BinlogStreamState,
    packet_codec: &mut PacketCodec,
    socket: &mut TcpStream,
) -> Result<()> {
    println!("binlog_stream_state: {:?}", binlog_stream_state);
    // 等待超过多少次，则断开/退出，让从库重连

    let binlog_file = match File::open(&binlog_stream_state.filename) {
        Ok(f) => f,
        Err(e) => return Err(anyhow!("File {:}: {e}", binlog_stream_state.filename)),
    };

    binlog_stream_state.local_file_pos = binlog_file.metadata().unwrap().len();

    {
        let mut binlog_file_ =
            match BinlogFile::new(BinlogVersion::Version4, BufReader::new(binlog_file)) {
                Ok(reader) => reader,
                Err(e) => {
                    println!("BinlogFile::new failed, {e}");
                    return Err(anyhow!("BinlogFile::new failed, {e}"));
                }
            };

        while let Some(event) = binlog_file_.next() {
            let event = event?;

            binlog_stream_state.master_pos = event.header().log_pos() as u64;
            if event.header().log_pos() < binlog_stream_state.pos as u32 {
                println!("event:pos::{}", event.header().log_pos());
                println!("binlog_stream_state.pos::{}", binlog_stream_state.pos);
                continue;
            }
            if let Err(e) = event_to_stream(packet_codec, socket, event).await {
                eprintln!("event_to_stream: {e}");
                break;
            }
        }
    }

    println!("read all.");

    if let Err(e) = dump_increment_binlog_event(binlog_stream_state, packet_codec, socket).await {
        println!("dump_increment_binlog_event: e: {e}");
    }

    Ok(())
}

async fn dump_increment_binlog_event(
    binlog_stream_state: &mut BinlogStreamState,
    packet_codec: &mut PacketCodec,
    socket: &mut TcpStream,
) -> Result<(), anyhow::Error> {
    'outer: loop {
        // 增量读取文件
        let mut binlog_file = match File::open(&binlog_stream_state.filename) {
            Ok(f) => f,
            Err(e) => return Err(anyhow!("File {:}: {e}", binlog_stream_state.filename)),
        };

        if binlog_file.metadata().unwrap().len() <= binlog_stream_state.local_file_pos {
            // 判断客户端是否已经断开
            // if socket.p

            // 发送心跳事件
            // let now = Utc::now();
            // let start: SystemTime = now.into();
            // let now = start.duration_since(UNIX_EPOCH).unwrap().as_secs();

            // let header = BinlogEventHeader::new(
            //     now as u32,
            //     mysql_async::binlog::EventType::HEARTBEAT_EVENT,
            //     1000,
            //     19,
            //     binlog_stream_state.master_pos as u32,
            //     EventFlags::LOG_EVENT_ARTIFICIAL_F,
            // );

            // let mut data = vec![];
            // header.serialize(&mut data);

            // match socket.write(&mut data).await {
            //     Ok(0) => {
            //         println!("Network disconnected");
            //         break;
            //     }
            //     Err(e) => match e.kind() {
            //         io::ErrorKind::ConnectionAborted => {
            //             println!("连接中断");
            //             return Ok(());
            //         }
            //         io::ErrorKind::ConnectionReset => {
            //             println!("连接被重置");
            //             return Ok(());
            //         }
            //         io::ErrorKind::BrokenPipe => {
            //             println!("管道破裂（连接已关闭）");
            //             return Ok(());
            //         }
            //         _ => {
            //             println!("xxxxxxxxxxxxxxxxxxxxxxx");
            //             return Err(anyhow!("{e}"));
            //         },
            //     },
            //     Ok(_) => {
                    println!("sleep 3s...");
                    thread::sleep(Duration::from_secs(3));
                    println!("sleep 3s end");
                    continue;
        //         }
        //     }
        }

        println!("dump_increment_binlog_event...");

        binlog_file.seek(SeekFrom::Start(binlog_stream_state.local_file_pos))?;

        let mut buffer: Vec<u8> = Vec::new();
        buffer.put(&BinlogFileHeader::VALUE[..]);
        if let Err(e) = binlog_file.read_to_end(&mut buffer) {
            println!("binlog_file.read_to_end: {e}");
            break;
        }

        let mut binlog_file = match BinlogFile::new(BinlogVersion::Version4, &buffer[..]) {
            Ok(reader) => reader,
            Err(e) => {
                println!("BinlogFile::new failed, {e}");
                return Err(anyhow!("BinlogFile::new failed, {e}"));
            }
        };

        while let Some(event) = binlog_file.next() {
            let event = event?;
            binlog_stream_state.master_pos = event.header().log_pos() as u64;
            if event.header().log_pos() < binlog_stream_state.pos as u32 {
                println!("event:pos::{}", event.header().log_pos());
                println!("binlog_stream_state.pos::{}", binlog_stream_state.pos);
                continue;
            }
            if let Err(e) = event_to_stream(packet_codec, socket, event).await {
                eprintln!("event_to_stream: {e}");
                break 'outer;
            }
        }

        binlog_stream_state.local_file_pos += buffer.len() as u64;
    }

    Ok(())
}

async fn event_to_stream(
    packet_codec: &mut PacketCodec,
    socket: &mut TcpStream,
    ev: mysql_async::binlog::events::Event,
) -> Result<()> {
    let now: DateTime<Local> = Local::now();
    let formatted_time = now.format("%Y-%m-%d %H:%M:%S").to_string();
    println!("time={:?}", formatted_time);
    let _ = dbg!(ev.header().event_type());
    let binlog_version = ev.fde().binlog_version();
    let mut output = Vec::new();
    ev.write(binlog_version, &mut output).unwrap();
    let mut src = BytesMut::new();
    let mut dst = BytesMut::new();
    src.put_u8(0x00);
    src.extend_from_slice(&output);
    packet_codec.encode(&mut src, &mut dst).unwrap();

    if let Err(e) = socket.write_all(&dst).await {
        return Err(anyhow!("failed to write to socket; err = {:?}", e));
    } else {
        println!("Event dump.");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    println!("MySQL Server listen to 127.0.0.1:8080...");
    let connection_id = Arc::new(Mutex::new(10));
    loop {
        let (mut socket, _) = listener.accept().await?;
        let cloned_connection_id: Arc<Mutex<u32>> = Arc::clone(&connection_id);

        tokio::spawn(async move {
            let mut scramble = Scramble::new();
            let mut status = ServerPhaseDesc::InitialHandshakePacket;
            let mut next_status = ServerPhaseDesc::InitialHandshakePacket;
            let mut packet_codec = PacketCodec::default();
            let mut buffers: Vec<BytesMut> = Vec::new();
            let mut common_buffer = BytesMut::new();
            let mut client_capabilities = CapabilityFlags::empty();
            let mut binlog_stream_state = BinlogStreamState::default();

            let mut handshake = Handshake::new();

            let mut session_vars: Vec<Variable> = Vec::new();
            for var in SYSVARS.iter() {
                session_vars.push(var.clone());
            }

            loop {
                match status {
                    ServerPhaseDesc::InitialHandshakePacket => {

                        let mut current_conn_id = cloned_connection_id.try_lock().unwrap();
                        *current_conn_id += 1;

                        // 2. 发送初始握手包
                        common_buffer = handshake.init_plain_packet(*current_conn_id);
                        println!("buffer.len: {:?}", common_buffer.len());

                        // 3 发送挑战包给客户端
                        status = ServerPhaseDesc::ServerResponse;

                        // 4. 收到客户端握手包并响应给客户端
                        next_status = ServerPhaseDesc::ClientHandshakeResponse;
                    }
                    ServerPhaseDesc::ClientResponse => {
                        // 4. 接收客户端响应
                        let mut buf = [0; READ_BUFFER_SIZE];
                        // socket.readable().await.unwrap();

                        let n = match socket.read(&mut buf).await {
                            Ok(0) => {
                                println!("Socket closed");
                                break;
                            }
                            Ok(n) => n,
                            Err(e) => {
                                println!("failed to read from socket; err = {:?}", e);
                                break;
                            }
                        };

                        let data = &buf[0..n];
                        println!("recv:buf:{:?}", data);

                        common_buffer.clear();
                        let mut src = BytesMut::new();
                        src.extend_from_slice(data);
                        println!("recv:buffer:src:{:?}", src);
                        packet_codec.decode(&mut src, &mut common_buffer).unwrap();

                        println!("recv:buffer:{:?}", common_buffer);
                        println!("recv:buffer:vec:{:?}", common_buffer.to_vec());

                        match next_status {
                            // 5. 解析握手包
                            ServerPhaseDesc::ClientHandshakeResponse => {
                                let valid_pass = match handle_handshake_response(
                                    &mut common_buffer,
                                    handshake.scramble(),
                                    &mut client_capabilities,
                                )
                                .await
                                {
                                    Ok(buf) => buf,
                                    Err(e) => {
                                        println!("{e}");
                                        break;
                                    }
                                };

                                // 6.验证客户端身份后响应给客户端
                                status = ServerPhaseDesc::ServerResponse;
                                // 7. 登录成功，切换至命令阶段
                                if valid_pass {
                                    next_status = ServerPhaseDesc::Command;
                                }
                            }
                            ServerPhaseDesc::Command => {
                                // 7. 登录成功，切换至命令阶段
                                println!("Command: {:?}", common_buffer);
                                let mut bufs = match handle_command_response(
                                    &mut common_buffer,
                                    &client_capabilities,
                                    &mut session_vars,
                                ) {
                                    Ok((buf, com)) => {
                                        match com {
                                            Command::COM_BINLOG_DUMP => {
                                                let com_binlog_dump = ComBinlogDump::deserialize(
                                                    (),
                                                    &mut ParseBuf(&common_buffer),
                                                )
                                                .unwrap();
                                                println!("com_binlog_dump: {:?}", com_binlog_dump);

                                                // 命令处理结束
                                                //
                                                // 开始进入循环dump日志
                                                binlog_stream_state.filename =
                                                    get_filename_from_relaylog_index(
                                                        &com_binlog_dump.filename().to_string(),
                                                    )
                                                    .unwrap();
                                                binlog_stream_state.pos =
                                                    com_binlog_dump.pos() as u64;

                                                handle_binlog_dump(
                                                    &mut binlog_stream_state,
                                                    &mut packet_codec,
                                                    &mut socket,
                                                )
                                                .await
                                                .unwrap();

                                                vec![]
                                            }
                                            Command::COM_BINLOG_DUMP_GTID => {
                                                // 按位点找文件
                                                let com_binlog_dump_gtid =
                                                    ComBinlogDumpGtid::deserialize(
                                                        (),
                                                        &mut ParseBuf(&common_buffer),
                                                    )
                                                    .unwrap();
                                                println!(
                                                    "com_binlog_dump_gtid: {:?}",
                                                    com_binlog_dump_gtid
                                                );

                                                // 如果server_id与本机以及和其他slave相同，则报错

                                                let file_data = std::fs::read("C:\\Users\\BK-liao\\mysqltest\\data\\relay-bin\\relay-bin.000000001").unwrap();
                                                let mut binlog_file = BinlogFile::new(
                                                    BinlogVersion::Version4,
                                                    &file_data[..],
                                                )
                                                .unwrap();

                                                while let Some(ev) = binlog_file.next() {
                                                    let ev = ev.unwrap();
                                                    let _ = dbg!(ev.header().event_type());
                                                    let binlog_version = ev.fde().binlog_version();

                                                    let mut output = Vec::new();
                                                    ev.write(binlog_version, &mut output).unwrap();

                                                    let mut src = BytesMut::new();
                                                    let mut dst = BytesMut::new();
                                                    src.put_u8(0x00);
                                                    src.extend_from_slice(&output);

                                                    packet_codec
                                                        .encode(&mut src, &mut dst)
                                                        .unwrap();
                                                    if let Err(e) = socket.write_all(&dst).await {
                                                        eprintln!(
                                                            "failed to write to socket; err = {:?}",
                                                            e
                                                        );
                                                        break;
                                                    } else {
                                                        println!("Event dump.");
                                                    }
                                                }

                                                vec![]
                                            }

                                            _ => buf,
                                        }
                                    }
                                    Err(e) => {
                                        println!("{e}");
                                        break;
                                    }
                                };
                                if bufs.len() > 0 {
                                    common_buffer.clear();
                                    buffers.append(&mut bufs);

                                    // 8.处理命令后响应给客户端
                                    status = ServerPhaseDesc::ServerResponse;
                                }
                            }
                            _ => {}
                        }
                    }

                    ServerPhaseDesc::ServerResponse => {
                        // 3 发送挑战包给客户端
                        // 6.验证客户端身份后响应给客户端
                        // Write the data back
                        if common_buffer.has_remaining() {
                            let mut src = common_buffer.clone();
                            common_buffer.clear();
                            packet_codec.encode(&mut src, &mut common_buffer).unwrap();

                            println!("ServerResponse:: buffer:: {:?}", common_buffer);
                            println!(
                                ">>>>ServerResponse:: buffer:vec:: {:?}",
                                common_buffer.to_vec()
                            );
                            if let Err(e) = socket.write_all(&common_buffer).await {
                                eprintln!("failed to write to socket; err = {:?}", e);
                                return;
                            }
                            common_buffer.clear();
                            socket.flush().await.unwrap();
                        } else {
                            common_buffer.clear();

                            // 兼容mariadb
                            // let is_mariadb = false;
                            // let mut packets: Vec<BytesMut> = vec![];

                            loop {
                                if buffers.is_empty() {
                                    break;
                                }
                                let mut src = buffers.remove(0);
                                let mut dst = BytesMut::new();
                                packet_codec.encode(&mut src, &mut dst).unwrap();
                                // common_buffer.extend_from_slice(&dst);
                                // if is_mariadb {
                                //     packets.push(dst);
                                // } else {
                                println!("ServerResponse:: buffer:: {:?}", dst);
                                println!(">>>>ServerResponse:: buffer:vec:: {:?}", dst.to_vec());
                                if let Err(e) = socket.write_all(&dst).await {
                                    eprintln!("failed to write to socket; err = {:?}", e);
                                    return;
                                }
                                socket.flush().await.unwrap();
                                // }
                            }

                            // 包合并
                            // if is_mariadb {
                            //     loop {
                            //         common_buffer.clear();

                            //         if packets.is_empty() {
                            //             break;
                            //         }
                            //         let src = packets.remove(0);
                            //         common_buffer.extend_from_slice(&src);
                            //         for p in packets.iter() {
                            //             common_buffer.extend_from_slice(&p);
                            //         }

                            //         println!("ServerResponse:: buffer:: {:?}", common_buffer);
                            //         println!(
                            //             ">>>>ServerResponse:: buffer:vec:: {:?}",
                            //             common_buffer.to_vec()
                            //         );
                            //         if let Err(e) = socket.write_all(&common_buffer).await {
                            //             eprintln!("failed to write to socket; err = {:?}", e);
                            //             return;
                            //         }
                            //         socket.flush().await.unwrap();
                            //     }
                            // }
                        }

                        // 等待客户端响应
                        status = ServerPhaseDesc::ClientResponse;
                    }

                    _ => {}
                }
            }
        });
    }
}
