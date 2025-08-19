use std::{path::Path, time::Duration};

use anyhow::{Result, anyhow};
use bytes::BytesMut;
use mysql_async::consts::StatusFlags;
use mysql_common::{
    io::ParseBuf,
    packets::ComBinlogDump,
    proto::{MyDeserialize, codec::PacketCodec},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
    sync::watch, time::timeout,
};

use crate::{
    auth::handshake::Handshake,
    com::{
        SqlCommand,
        binlog::{MyBinlogFileReader, index::BinlogIndex, match_binlog_command},
    },
    consts::Command,
    variable::{SYSVARS, Variable, get_global_var},
};

// 缓冲区大小
const READ_BUFFER_SIZE: usize = 0xFFFF;
pub const INTERVAL: Duration = Duration::from_secs(1);
pub const START_AFTER: Duration = Duration::from_secs(3);

//
pub fn server_status_flags() -> StatusFlags {
    StatusFlags::SERVER_STATUS_AUTOCOMMIT
}

//
enum ConnectionLifecycle {
    // 认证：首次握手，发送握手包
    ConnectionPhaseInitialHandshake,
    // 握手响应
    ConnectionPhaseInitialHandshakeResponse,
    // 认证通过响应
    ConnectionPhaseAuthenticationResponse,

    //
    CommandPhase,
    CommandBinlogDumpPhase,
}

// 初始化服务端
pub async fn init_server(port: u16) -> Result<()> {
    let addr = ("0.0.0.0", port);
    let listener = TcpListener::bind(addr).await?;
    println!("MySQL Server listen to {}:{}...", addr.0, addr.1);

    let relay_log_basename = get_global_var("relay_log_basename").unwrap();
    let relay_log_index = get_global_var("relay_log_index").unwrap();
    let relay_log_index_path = Path::new(relay_log_basename.value()).join(relay_log_index.value());
    let cloned_relay_log_index_path = relay_log_index_path.clone();

    // 启动另一个线程，监控binlog日志是否有变化，主要是判断大小与relaylog文件大小
    // 如果有变化，通过另一个消息通道通知读取并往stream发送。
    // 如果没有变化，则继续等待。
    let (tx, rx) = watch::channel(String::new());
    let cloned_tx = tx.clone();
    tokio::spawn(async move {
        let mut intv =
            tokio::time::interval_at(tokio::time::Instant::now() + START_AFTER, INTERVAL);
        let start_secs = START_AFTER.as_secs();
        let intv_secs = START_AFTER.as_secs();
        log::info!(
            "Binlog watcher inited, start after {start_secs} secs, running every {intv_secs} secs"
        );

        let mut index = BinlogIndex::try_from(cloned_relay_log_index_path.to_path_buf()).unwrap();
        let mut reader: MyBinlogFileReader = MyBinlogFileReader::new();
        let mut binlog_filename = String::new();
        loop {
            select! {
                _ = intv.tick() => {
                    // 一直监控relaylog文件，以及最后一个文件的大小，如果有变化，则发送通知，让子线程更新状态
                    match index.changed().unwrap() {
                        Some(filename) => {
                            if binlog_filename != filename {
                                log::debug!("Binlog filename changed...{filename}");
                                binlog_filename = filename.clone();
                                cloned_tx.send(filename.clone()).unwrap();
                            } else {
                                // 监控文件是否变化
                                // log::debug!("filename: {filename}");
                                reader.with_filename(Path::new(&filename).to_path_buf()).unwrap();
                                if reader.changed().unwrap() {
                                    log::debug!("Binlog file changed...{filename}");
                                    cloned_tx.send(filename.clone()).unwrap();
                                }
                            }
                        },
                        None => {
                            // relaylog文件无变化
                            // 监控 binlog 是否有更新
                            match index.last().unwrap() {
                                Some(filename) => {
                                    // 监控文件是否变化
                                    reader.with_filename(Path::new(&filename).to_path_buf()).unwrap();
                                    if reader.changed().unwrap() {
                                        log::debug!("Binlog file changed...{filename}");
                                        cloned_tx.send(filename.clone()).unwrap();
                                    }
                                }
                                None => {
                                    log::error!("Binlog index file {} is empty?", cloned_relay_log_index_path.display())
                                }
                            }
                            continue;
                        },
                    }

                    log::debug!("Binlog watcher running...");
                }
            }
        }
    });

    loop {
        let (mut socket, _) = listener.accept().await?;
        let mut cloned_rx = rx.clone();

        let relay_log_index_path =
            Path::new(relay_log_basename.value()).join(relay_log_index.value());
        let cloned_relay_log_index_path = relay_log_index_path.clone();
        tokio::spawn(async move {
            let mut handshake = Handshake::new();
            let mut session_vars: Vec<Variable> = Vec::new();
            let mut packet_codec = PacketCodec::default();

            for var in SYSVARS.iter() {
                session_vars.push(var.clone());
            }
            let mut sql_command = SqlCommand::new(session_vars);
            let mut phase = ConnectionLifecycle::ConnectionPhaseInitialHandshake;

            let mut binlog_index =
                BinlogIndex::try_from(cloned_relay_log_index_path.to_path_buf()).unwrap();
            let mut binlog_reader = MyBinlogFileReader::default();
            loop {
                select! {
                    Ok(_) = cloned_rx.changed() => {
                        // 收到binlog变更的事件
                        log::debug!("cloned_rx.changed...");
                        let filename = format!("{:?}", cloned_rx.borrow_and_update());
                        handle_binlog_changed(&filename, &mut binlog_reader, &mut socket, &mut packet_codec, &mut phase).await.unwrap();
                    }

                    Ok(_) = socket.readable() => {
                        // 可以读数据
                        if let Err(e) = handle_read(&mut socket, &mut packet_codec, &mut handshake, &mut phase, &mut sql_command, &mut binlog_index, &mut binlog_reader).await {
                            log::debug!("Read data failed, {:?}", e);
                        }
                        log::debug!("socket.readable...");
                    }
                    Ok(_) = socket.writable() => {
                        // 可以写数据
                        handle_write(&mut socket, &mut packet_codec, &mut handshake, &mut phase, &mut sql_command).await.unwrap();
                        log::debug!("socket.writable...");
                    }

                    

                }
            }
        });
    }
}

async fn handle_binlog_changed(
    filename: &String,
    binlog_reader: &mut MyBinlogFileReader,
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
    phase: &mut ConnectionLifecycle,
) -> Result<()> {
    println!("handle_binlog_changed:: filename: {filename}");
    match phase {
        ConnectionLifecycle::CommandBinlogDumpPhase => {
            // 文件名不一样
            if Path::new(&filename) != binlog_reader.filename() {
                // reader
                //     .with_filename(Path::new(filename).to_path_buf())
                //     .unwrap();
                // reader.read_all_to_stream(0, stream, packet_codec).await?;
                binlog_dump(stream, packet_codec, binlog_reader, filename.clone()).await?;
            }
        }
        _ => {}
    }
    Ok(())
}

// 写数据
async fn handle_write(
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
    handshake: &mut Handshake,
    phase: &mut ConnectionLifecycle,
    _sql_command: &mut SqlCommand,
) -> Result<()> {
    match phase {
        ConnectionLifecycle::ConnectionPhaseInitialHandshake => {
            let mut packet = handshake.init_plain_packet();
            net_write(stream, packet_codec, &mut packet).await?;
            *phase = ConnectionLifecycle::ConnectionPhaseInitialHandshakeResponse;
        }
        _ => {}
    }

    Ok(())
}

//
pub async fn net_write(
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
    src: &mut BytesMut,
) -> Result<()> {
    // println!("net_write: {:?}", src);
    let mut buffer = BytesMut::with_capacity(src.len() + 4);
    packet_codec.encode(src, &mut buffer).unwrap();
    println!("buffer: {:?}", buffer);
    if let Err(e) = stream.write_all(&buffer).await {
        return Err(anyhow!("failed to write to stream; err = {:?}", e));
    }
    stream.flush().await.unwrap();
    Ok(())
}

async fn net_read<'a>(stream: &mut TcpStream) -> Result<BytesMut> {
    let mut buf = [0; READ_BUFFER_SIZE];
    let n = match timeout(Duration::from_millis(500), stream.read(&mut buf)).await? {
        Ok(0) => {
            return Err(anyhow!("Socket closed"));
        }
        Ok(n) => n,
        Err(e) => {
            return Err(anyhow!("failed to read from socket; err = {:?}", e));
        }
    };
    let data = &buf[0..n];
    println!("recv:buf:{:?}", data);

    let mut src = BytesMut::new();
    src.extend_from_slice(data);

    Ok(src)
}

// 写数据
async fn handle_read(
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
    handshake: &mut Handshake,
    phase: &mut ConnectionLifecycle,
    sql_command: &mut SqlCommand,
    binlog_index: &mut BinlogIndex,
    binlog_reader: &mut MyBinlogFileReader,
) -> Result<()> {
    let mut src = net_read(stream).await?;
    let mut buffer = BytesMut::with_capacity(src.len() - 4);
    println!("recv:buffer:src:{:?}", src);
    packet_codec.decode(&mut src, &mut buffer).unwrap();
    println!("recv:buffer:{:?}", buffer);
    println!("recv:buffer:vec:{:?}", buffer.to_vec());

    match phase {
        ConnectionLifecycle::ConnectionPhaseInitialHandshakeResponse => {
            // 验证密码
            let (varify_pass, mut data) = handshake.auth_with_response(&mut buffer)?;
            if varify_pass {
                // 验证通过
                *phase = ConnectionLifecycle::ConnectionPhaseAuthenticationResponse;
            }
            sql_command.set_client_capabilities(*handshake.client_capabilities());
            // 回复客户端
            net_write(stream, packet_codec, &mut data).await?;
        }
        ConnectionLifecycle::ConnectionPhaseAuthenticationResponse
        | ConnectionLifecycle::CommandPhase => {
            // 是否收到其他命令
            if let Some(com) = match_binlog_command(&mut buffer)? {
                handle_binlog_command(
                    stream,
                    packet_codec,
                    com,
                    &mut buffer,
                    binlog_index,
                    binlog_reader,
                )
                .await?;
                *phase = ConnectionLifecycle::CommandBinlogDumpPhase;
                return Ok(());
            }

            let mut data = sql_command.read(&mut buffer)?;
            loop {
                if data.is_empty() {
                    break;
                }
                let mut packet = data.remove(0);
                net_write(stream, packet_codec, &mut packet).await?;
            }
            *phase = ConnectionLifecycle::CommandPhase;
        }
        _ => {}
    }
    Ok(())
}

async fn handle_binlog_command(
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
    com: Command,
    buffer: &mut BytesMut,
    binlog_index: &mut BinlogIndex,
    binlog_reader: &mut MyBinlogFileReader,
) -> Result<()> {
    match com {
        Command::COM_BINLOG_DUMP => {
            let com_binlog_dump = ComBinlogDump::deserialize((), &mut ParseBuf(&buffer))?;
            let request_filename = com_binlog_dump.filename().to_string();
            log::info!(
                "handle_binlog_command:: request_filename: {request_filename}, request_pos: {}",
                com_binlog_dump.pos()
            );
            binlog_index.with_request_binlog_filename(request_filename.clone());

            let filename = match binlog_index.find()? {
                Some(name) => name,
                None => {
                    return Err(anyhow!(
                        "handle_binlog_command:: binlog file {} is not in binlog index file",
                        request_filename
                    ));
                }
            };

            // 读取当前请求的文件
            log::debug!("handle_binlog_command: filename: {filename}");
            // 读取客户端需要的日志文件
            binlog_dump(stream, packet_codec, binlog_reader, filename).await?;

            // 读取剩余的文件
            while let Some(filename) = binlog_index.find_next()? {
                log::debug!("handle_binlog_command: filename: {filename}");
                log::info!("handle_binlog_command:: rotate to filename: {}", filename);
                binlog_dump(stream, packet_codec, binlog_reader, filename).await?;
            }
        }
        Command::COM_BINLOG_DUMP_GTID => {}
        _ => {}
    }
    Ok(())
}

async fn binlog_dump(
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
    binlog_reader: &mut MyBinlogFileReader,
    filename: String,
) -> Result<(), anyhow::Error> {
    binlog_reader.with_filename(Path::new(&filename).to_path_buf())?;
    binlog_reader
        .read_all_to_stream(0, stream, packet_codec)
        .await?;
    Ok(())
}
