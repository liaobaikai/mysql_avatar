use std::{ffi::OsStr, path::Path, time::Duration};

use anyhow::{Result, anyhow};
use bytes::BytesMut;
use mysql_async::{
    binlog::events::{Event, RotateEvent},
    consts::StatusFlags,
};
use mysql_common::{
    io::ParseBuf,
    packets::{ComBinlogDump, SemiSyncAckPacket},
    proto::{MyDeserialize, codec::PacketCodec},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, Interest},
    net::{TcpListener, TcpStream},
    select,
    sync::watch,
};

use crate::{
    auth::handshake::Handshake,
    com::{
        SqlCommand,
        binlog::{
            MyBinlogFileReader, event::EventExt, event_to_bytes, index::BinlogIndex,
            match_binlog_command,
        },
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

#[derive(Debug)]
enum ConnectionLifecycle {
    // 认证：首次握手，发送握手包
    ConnectionPhaseInitialHandshake,
    // 握手响应
    ConnectionPhaseInitialHandshakeResponse,
    // 认证通过响应
    // ConnectionPhaseAuthenticationResponse,

    //
    CommandPhase,
    CommandBinlogDumpPhase,
}

// 初始化服务端
pub async fn init_server(port: u16) -> Result<()> {
    let addr = ("0.0.0.0", port);
    let listener = TcpListener::bind(addr).await?;
    log::info!("MySQL Server listen to {}:{}...", addr.0, addr.1);

    let relay_log_basename = get_global_var("relay_log_basename").unwrap();
    let relay_log_index = get_global_var("relay_log_index").unwrap();
    let relay_log_index_path = Path::new(relay_log_basename.value()).join(relay_log_index.value());
    let cloned_relay_log_index_path = relay_log_index_path.clone();

    // 是否启用半同步复制模式
    let var1 = get_global_var("rpl_semi_sync_master_enabled").unwrap();
    let var2 = get_global_var("rpl_semi_sync_source_enabled").unwrap();
    let mut rpl_semi_sync_master_enabled =
        *var1.value() == "1".to_owned() || *var2.value() == "1".to_owned();

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
                        },
                    }
                    // log::debug!("Binlog watcher running...");
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
            let mut sql_command = SqlCommand::new(&mut session_vars);

            let mut binlog_index =
                BinlogIndex::try_from(cloned_relay_log_index_path.to_path_buf()).unwrap();
            let mut binlog_reader = MyBinlogFileReader::default();

            let mut phase = ConnectionLifecycle::ConnectionPhaseInitialHandshake;
            let mut buffer = handshake.init_plain_packet();
            // 是否开始dump binlog
            let mut open_binlog_dump = false;
            let mut open_need_ack = false;

            'outer: loop {
                // let cloned_session_vars = session_vars.clone();
                select! {
                        Ok(_) = cloned_rx.changed(), if open_binlog_dump => {
                            // 收到binlog变更的事件
                            log::debug!("cloned_rx.changed...::: {:?}", open_binlog_dump);
                            let filename = format!("{}", *cloned_rx.borrow_and_update());
                            handle_binlog_changed(&filename, &mut binlog_reader, rpl_semi_sync_master_enabled, &mut open_need_ack, &mut socket, &mut packet_codec).await.unwrap();
                        }

                        // Ok(ready) = socket.ready(Interest::READABLE) => {
                        //     if ready.is_readable() {

                        //     }
                        // }
                        // Ok(_) = tokio::time::timeout(Duration::from_millis(500), socket.writable()) => {
                        Ok(ready) = socket.ready(Interest::WRITABLE) => {
                        // Ok(ready) = tokio::time::timeout(Duration::from_millis(500), socket.ready(Interest::WRITABLE)) => {
                            if ready.is_writable() && !buffer.is_empty() {
                                log::debug!("socket.writable...");
                                // 可以写数据
                                // 添加包头
                                match phase {
                                    ConnectionLifecycle::ConnectionPhaseInitialHandshake => {
                                        phase = ConnectionLifecycle::ConnectionPhaseInitialHandshakeResponse;
                                    }
                                    _ => {}
                                }

                                let mut dst = encode_packet(&mut buffer, &mut packet_codec).unwrap();
                                buffer.clear();
                                if let Err(e) = handle_write(&mut dst, &mut socket).await {
                                    // log::error!("Write error: {e}");
                                    log::info!("Broken pipe? {e}");
                                    // break 'outer;
                                    continue;
                                }
                                
                            }
                        }

                        // Ok(ready) = tokio::time::timeout(Duration::from_millis(500), socket.writable()) => {
                        Ok(ready) = socket.ready(Interest::READABLE) => {
                            if ready.is_readable() {
                        // Ok(_) = tokio::time::timeout(Duration::from_millis(500), socket.readable()) => {
                                log::debug!("socket.readable...{:?}", phase);
                                let mut timeout = false;
                                // 可以读数据
                                {
                                    let mut packet = match net_read(&mut socket).await {
                                        Ok(pack) => pack,
                                        Err(_e) => {
                                            // log::info!("Connection closed: {e}");
                                            // break 'outer;
                                            timeout = true;
                                        }
                                    };
                                    let dst = decode_packet(&mut packet, &mut packet_codec).unwrap();
                                    buffer.extend_from_slice(&dst);
                                }
                                log::debug!("socket.readable...net_read...ok....{:?}", phase);

                                match phase {
                                    ConnectionLifecycle::ConnectionPhaseInitialHandshakeResponse => {
                                        // 验证密码
                                        let (varify_pass, mut packet) = handshake.auth_with_response(&mut buffer).unwrap();
                                        if varify_pass {
                                            // 验证通过
                                            phase = ConnectionLifecycle::CommandPhase;
                                        }
                                        sql_command.set_client_capabilities(*handshake.client_capabilities());
                                        //
                                        // buffer.clear();
                                        // buffer.extend_from_slice(&data);

                                        let mut dst = encode_packet(&mut packet, &mut packet_codec).unwrap();
                                        net_write(&mut socket, &mut dst).await.unwrap();

                                        // 不往下走
                                        // continue;
                                    }
                                    ConnectionLifecycle::CommandPhase => {
                                        // 是否收到其他命令
                                        if let Some(com) = match_binlog_command(&mut buffer).unwrap() {
                                            phase = ConnectionLifecycle::CommandBinlogDumpPhase;
                                            // 开始接收监听事件
                                            open_binlog_dump = true;
                                            match com {
                                                Command::COM_BINLOG_DUMP => {
                                                    if open_need_ack {
                                                        // 解析ack
                                                        let (filename, pos) = check_ack(&mut buffer, &mut binlog_reader).unwrap();
                                                        log::info!("Ack {} {} recv from slave", filename, pos);

                                                    } else {
                                                        // 解析命令
                                                        let var1 = sql_command.get_session_var("@rpl_semi_sync_slave").unwrap();
                                                        let var2 = sql_command.get_session_var("@rpl_semi_sync_replica").unwrap();
                                                        rpl_semi_sync_master_enabled = rpl_semi_sync_master_enabled & (*var1.value() == "1".to_owned() || *var2.value() == "1".to_owned());

                                                        handle_com_binlog_dump(&mut socket,
                                                                &mut packet_codec,
                                                                &mut buffer,
                                                                &mut binlog_index,
                                                                &mut binlog_reader,
                                                                rpl_semi_sync_master_enabled,
                                                                &mut open_need_ack
                                                        ).await.unwrap();
                                                    }
                                                },
                                                // Command::COM_BINLOG_DUMP_GTID => handle_com_binlog_dump_gtid(),
                                                _ => {}
                                            }
                                            buffer.clear();
                                        } else {
                                            let mut data = sql_command.read(&mut buffer).unwrap();
                                            'inner: loop {
                                                if data.is_empty() {
                                                    break 'inner;
                                                }
                                                let mut packet = data.remove(0);
                                                let mut dst = encode_packet(&mut packet, &mut packet_codec).unwrap();
                                                net_write(&mut socket, &mut dst).await.unwrap();
                                            }
                                            phase = ConnectionLifecycle::CommandPhase;
                                            buffer.clear();
                                        }
                                    }
                                    _ => {
                                        // 其他阶段
                                    }
                                }
                            }

                        }


                        _ = tokio::time::sleep(Duration::from_secs(1)) => {
                            log::trace!("sleep 1s");
                        }
                        

                    }
            }
            log::info!("Connection shutdown");
        });
    }
}

fn check_ack(
    buffer: &mut BytesMut,
    binlog_reader: &mut MyBinlogFileReader,
) -> Result<(String, u64)> {
    let ack = SemiSyncAckPacket::deserialize((), &mut ParseBuf(&buffer))?;
    if ack.position() == binlog_reader.pos()
        && binlog_reader.filename().file_name() == Some(OsStr::new(&ack.filename().to_string()))
    {
        return Ok((ack.filename().to_string(), ack.position()));
    }
    Ok((String::new(), 0))
}

async fn handle_binlog_changed(
    filename: &String,
    binlog_reader: &mut MyBinlogFileReader,
    rpl_semi_sync_master_enabled: bool,
    need_ack: &mut bool,
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
) -> Result<()> {
    log::debug!("handle_binlog_changed::filename: {filename}");
    log::debug!("rpl_semi_sync_master_enabled={rpl_semi_sync_master_enabled}, need_ack={need_ack}");
    // 文件名不一样
    binlog_dump(
        stream,
        packet_codec,
        binlog_reader,
        rpl_semi_sync_master_enabled,
        need_ack,
        filename.clone(),
        0,
    )
    .await
}

// 写数据
async fn handle_write(buffer: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
    log::debug!("handle_write...");
    net_write(stream, buffer).await?;
    buffer.clear();

    Ok(())
}

//
pub async fn net_write(stream: &mut TcpStream, src: &mut BytesMut) -> Result<()> {
    log::trace!("net_write::src: {:?}", src);
    log::trace!("net_write::src:vec: {:?}", src.to_vec());
    if let Err(e) = tokio::time::timeout(Duration::from_millis(500), stream.write_all(&src)).await? {
        return Err(anyhow!("failed to write to stream; err = {:?}", e));
    }
    stream.flush().await.unwrap();
    src.clear();
    Ok(())
}

async fn net_read<'a>(stream: &mut TcpStream) -> Result<BytesMut> {
    let mut buf = [0; READ_BUFFER_SIZE];
    // let n = match stream.read(&mut buf).await {
    let n = match tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await? {
        Ok(0) => {
            return Err(anyhow!("Socket closed"));
        }
        Ok(n) => n,
        Err(e) => {
            return Err(anyhow!("failed to read from socket; err = {:?}", e));
        }
    };
    let data = &buf[0..n];
    log::trace!("net_read::buf:{:?}", data);

    let mut src = BytesMut::new();
    src.extend_from_slice(data);

    Ok(src)
}

// 去掉包头
fn decode_packet(src: &mut BytesMut, packet_codec: &mut PacketCodec) -> Result<BytesMut> {
    let mut dst = BytesMut::with_capacity(src.len() - 4);
    packet_codec.decode(src, &mut dst)?;
    Ok(dst)
}

// 添加包头
fn encode_packet(src: &mut BytesMut, packet_codec: &mut PacketCodec) -> Result<BytesMut> {
    let mut dst = BytesMut::with_capacity(src.len() + 4);
    packet_codec.encode(src, &mut dst).unwrap();
    Ok(dst)
}

// 处理binlog dump命令
async fn handle_com_binlog_dump(
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
    buffer: &mut BytesMut,
    binlog_index: &mut BinlogIndex,
    binlog_reader: &mut MyBinlogFileReader,
    rpl_semi_sync_master_enabled: bool,
    need_ack: &mut bool,
) -> Result<()> {
    let com_binlog_dump = ComBinlogDump::deserialize((), &mut ParseBuf(&buffer))?;
    let request_filename = com_binlog_dump.filename().to_string();
    log::info!(
        "handle_binlog_command::request_filename: {request_filename}, request_pos: {}",
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
    log::debug!("handle_binlog_command:: filename: {filename}");
    // 读取客户端需要的日志文件
    binlog_dump(
        stream,
        packet_codec,
        binlog_reader,
        rpl_semi_sync_master_enabled,
        need_ack,
        filename,
        com_binlog_dump.pos(),
    )
    .await?;

    // 读取剩余的文件
    while let Some(filename) = binlog_index.find_next()? {
        log::debug!("handle_binlog_command:: filename: {filename}");
        log::info!("handle_binlog_command:: rotate to filename: {}", filename);

        // 新建一个rotate事件
        // let name = Path::new(&filename).file_name().unwrap();
        // let rotate_event = RotateEvent::new(4, name.as_encoded_bytes());
        // log::debug!("write rotate_event: {:?}", rotate_event);
        // let ev = Event::try_from_rotate_event(
        //     binlog_reader.server_id(),
        //     rotate_event,
        // )?;
        // let mut src = event_to_bytes(ev, rpl_semi_sync_master_enabled, *need_ack);
        // let mut dst = encode_packet(&mut src, packet_codec)?;
        // net_write(stream, &mut dst).await?;

        binlog_dump(
            stream,
            packet_codec,
            binlog_reader,
            rpl_semi_sync_master_enabled,
            need_ack,
            filename,
            0,
        )
        .await?;
    }

    // 已经追上日志了。
    log::info!("Binlog catched up latest pos");
    *need_ack = true;
    log::info!("Master need_ack enable.");

    Ok(())
}

async fn binlog_dump(
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
    binlog_reader: &mut MyBinlogFileReader,
    rpl_semi_sync_master_enabled: bool,
    need_ack: &mut bool,
    filename: String,
    skip_pos: u32,
) -> Result<(), anyhow::Error> {
    binlog_reader.with_filename(Path::new(&filename).to_path_buf())?;
    let mut binlog_file = binlog_reader.get_binlog_file()?;
    while let Some(event) = binlog_file.next() {
        let event = event?;
        binlog_reader.with_pos(event.header().log_pos() as u64);
        if skip_pos > 0 && event.header().log_pos() < skip_pos {
            continue;
        }
        binlog_reader.with_server_id(event.header().server_id());
        // 将事件发给客户端
        let mut src = event_to_bytes(event, rpl_semi_sync_master_enabled, *need_ack);
        let mut dst = encode_packet(&mut src, packet_codec)?;
        net_write(stream, &mut dst).await?;
    }

    Ok(())
}
