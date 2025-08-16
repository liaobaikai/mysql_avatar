use std::{collections::HashMap, sync::Arc};

use anyhow::{Result, anyhow};
use bytes::{Buf, BufMut, BytesMut};
use mysql_async::{
    binlog::BinlogVersion,
    consts::{CapabilityFlags, StatusFlags},
};
use mysql_common::{
    binlog::BinlogFile,
    io::ParseBuf,
    misc::raw::{RawBytes, RawInt, bytes::EofBytes},
    packets::{AuthPlugin, ComBinlogDump, ComBinlogDumpGtid, SqlState},
    proto::{MyDeserialize, MySerialize, codec::PacketCodec},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};

use crate::{
    command::handle_command_query,
    constants::Command,
    packets::{ErrPacket, HandshakePacket, HandshakeResponse, OkPacket, ServerError},
    vars::{SYSVARS, Variable},
};
// mod codec;
mod command;
mod constants;
mod packets;
mod vars;

const SCRAMBLE_BUFFER_SIZE: usize = 20;
const SERVER_VERSION: &str = "8.0.41";

// 协议版本：10
const PROTOCOL_VERSION: u8 = 10;
// 默认语言
const SERVER_LANGUAGE: u8 = 8;
// 缓冲区大小
const READ_BUFFER_SIZE: usize = 0xFFFF;

fn get_capabilities() -> CapabilityFlags {
    CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_PLUGIN_AUTH
        | CapabilityFlags::CLIENT_SECURE_CONNECTION
        | CapabilityFlags::CLIENT_CONNECT_WITH_DB
        | CapabilityFlags::CLIENT_TRANSACTIONS
        | CapabilityFlags::CLIENT_CONNECT_ATTRS
        // | CapabilityFlags::CLIENT_QUERY_ATTRIBUTES
        | CapabilityFlags::CLIENT_DEPRECATE_EOF
}

fn get_server_status() -> StatusFlags {
    StatusFlags::SERVER_STATUS_AUTOCOMMIT
}

// Build Handshake Packet
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
fn new_handshake_packet(scramble: &[u8; SCRAMBLE_BUFFER_SIZE], connection_id: u32) -> BytesMut {
    let mut scramble_1 = [0u8; 8];
    let (_scramble_1, scramble_2) = scramble.split_at(8);
    scramble_1.copy_from_slice(&_scramble_1);

    let packet = HandshakePacket::new(
        PROTOCOL_VERSION,
        SERVER_VERSION.as_bytes(),
        connection_id,
        scramble_1,
        Some(scramble_2),
        get_capabilities(),
        SERVER_LANGUAGE,
        get_server_status(),
        AuthPlugin::CachingSha2Password.as_bytes().into(),
    );

    let mut src = BytesMut::new();
    let mut buf: Vec<u8> = Vec::new();
    packet.serialize(&mut buf);
    src.extend_from_slice(&buf);
    src
}

// 接收并解析客户端握手响应
async fn handle_handshake_response<'a>(
    src: &mut BytesMut,
    scramble: &[u8; SCRAMBLE_BUFFER_SIZE],
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
            get_capabilities(),
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
            get_capabilities(),
        ))
        .serialize(&mut buf);
    }
    src.extend_from_slice(&buf);
    Ok(valid_pass)
}

// 处理命令
fn handle_command_response(
    src: &mut BytesMut,
    _client_capabilities: &CapabilityFlags,
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
            let ret = handle_command_query(&query.as_str(), session_vars)?;
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
        get_capabilities(),
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("MySQL Server listen to 127.0.0.1:8080...");
    let connection_id = Arc::new(Mutex::new(10));
    loop {
        let (mut socket, _) = listener.accept().await?;
        let cloned_connection_id: Arc<Mutex<u32>> = Arc::clone(&connection_id);

        tokio::spawn(async move {
            let mut scramble = [0u8; SCRAMBLE_BUFFER_SIZE];
            let mut status = ServerPhaseDesc::InitialHandshakePacket;
            let mut next_status = ServerPhaseDesc::InitialHandshakePacket;
            let mut packet_codec = PacketCodec::default();
            let mut buffers: Vec<BytesMut> = Vec::new();
            let mut common_buffer = BytesMut::new();
            let mut client_capabilities = CapabilityFlags::empty();

            let mut session_vars: Vec<Variable> = Vec::new();
            for var in SYSVARS.iter() {
                session_vars.push(var.clone());
            }

            loop {
                match status {
                    ServerPhaseDesc::InitialHandshakePacket => {
                        // 1. 生成随机挑战值
                        #[allow(deprecated)]
                        rand::Rng::fill(&mut rand::thread_rng(), &mut scramble);

                        let mut current_conn_id = cloned_connection_id.try_lock().unwrap();
                        *current_conn_id += 1;

                        // 2. 发送初始握手包
                        common_buffer = new_handshake_packet(&scramble, *current_conn_id);
                        println!("buffer.len: {:?}", common_buffer.len());

                        // 3 发送挑战包给客户端
                        status = ServerPhaseDesc::ServerResponse;

                        // 4. 收到客户端握手包并响应给客户端
                        next_status = ServerPhaseDesc::ClientHandshakeResponse;
                    }
                    ServerPhaseDesc::ClientResponse => {
                        // 4. 接收客户端响应
                        let mut buf = [0; READ_BUFFER_SIZE];
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
                                    &scramble,
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
                                                

                                                let file_data = std::fs::read("/Users/lbk/mysqltest/data/relay-bin/relay-bin.000000001").unwrap();
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

                                                    packet_codec.encode(&mut src, &mut dst).unwrap();
                                                    if let Err(e) = socket.write_all(&dst).await {
                                                        eprintln!("failed to write to socket; err = {:?}", e);
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
                            loop {
                                if buffers.len() == 0 {
                                    break;
                                }
                                let mut src = buffers.remove(0);
                                let mut dst = BytesMut::new();
                                packet_codec.encode(&mut src, &mut dst).unwrap();
                                common_buffer.extend_from_slice(&dst);

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
                            }
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

#[allow(unused)]
#[cfg(test)]
mod tests {
    use std::io;

    use bytes::{BufMut, BytesMut};
    use mysql_async::{
        OkPacket,
        binlog::BinlogVersion,
        consts::{CapabilityFlags, StatusFlags},
    };
    use mysql_common::{
        binlog::BinlogFile,
        packets::{AuthPlugin, HandshakePacket},
        proto::{MySerialize, codec::PacketCodec},
    };

    fn test_ok_packet() {
        let a = b"\x00\x01\x00\x02\x00\x00\x00";
    }

    #[test]
    fn test_handshake_packet() {
        let mut scramble = [0u8; 20];
        #[allow(deprecated)]
        rand::Rng::fill(&mut rand::thread_rng(), &mut scramble);
        let mut scramble_1 = [0u8; 8];
        let (_scramble_1, scramble_2) = scramble.split_at(8);
        scramble_1.copy_from_slice(&_scramble_1);

        let packet = HandshakePacket::new(
            10,
            "8.0.39".as_bytes(),
            123,
            scramble_1,
            Some(scramble_2),
            CapabilityFlags::all(),
            0x21,
            StatusFlags::all(),
            Some(b"mysql_native_password"),
        );

        let mut src = BytesMut::new();
        {
            let mut buf: Vec<u8> = Vec::new();
            packet.serialize(&mut buf);
            src.extend_from_slice(&buf);
        }
        println!("src: {src:?}");
        println!("src.len: {}", src.len());

        let mut dst = BytesMut::new();
        let mut codec = PacketCodec::default();
        codec.encode(&mut src, &mut dst).unwrap();

        println!("dst: {dst:?}");
    }

    #[test]
    fn test_parse_handshake_packet() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // use mysql::prelude::*;
        // use mysql::*;

        // let url = "mysql://root:123456@gzdsg.baikai.top:4407/mysql";
        // let pool = Pool::new(url)?;

        // let mut conn = pool.get_conn()?;
        // conn.query_drop(
        //     r"select @@server_id",
        // )?;

        Ok(())
    }

    #[test]
    fn binlog_event_roundtrip() -> io::Result<()> {
        const PATH: &str = "./test-data/binlogs";

        let binlogs = std::fs::read_dir(PATH)?
            .filter_map(|path| path.ok())
            .map(|entry| entry.path())
            .filter(|path| path.file_name().is_some());

        'outer: for file_path in binlogs {
            let file_data =
                std::fs::read("/Users/lbk/mysqltest/data/relay-bin/relay-bin.000000001")?;
            let mut binlog_file = BinlogFile::new(BinlogVersion::Version4, &file_data[..])?;

            while let Some(ev) = binlog_file.next() {
                let ev = ev?;
                let _ = dbg!(ev.header().event_type());
                let binlog_version = ev.fde().binlog_version();

                let mut output = Vec::new();
                ev.write(binlog_version, &mut output)?;
            }
        }

        Ok(())
    }
}
