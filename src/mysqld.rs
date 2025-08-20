use std::{ffi::OsStr, io, path::Path, time::Duration};

use anyhow::{Result, anyhow};
use bytes::BytesMut;
use mysql_async::{binlog::EventType, consts::StatusFlags};
use mysql_common::{
    io::ParseBuf,
    packets::{ComBinlogDump, SemiSyncAckPacket},
    proto::{MyDeserialize, codec::PacketCodec},
};
use tokio::{
    io::{AsyncWriteExt, Interest},
    net::{TcpListener, TcpStream},
    select,
    sync::watch,
};

use crate::{
    auth::handshake::Handshake,
    com::{
        SqlCommand,
        binlog::{
            MasterInfo, MyBinlogFileReader, ReplicaInfo, event_to_bytes, index::BinlogIndex,
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
    ConnectionPhaseAuthenticationResponse,

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
    let mut master_info = MasterInfo::new();
    {
        master_info.with_rpl_semi_sync_source(
            *var1.value() == "1".to_owned() || *var2.value() == "1".to_owned(),
        );
    }

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
                                // 如果发完这个,然后再发心跳,最终心跳会把这个冲掉
                                continue;
                            } else {
                                // 监控文件是否变化
                                // log::debug!("filename: {filename}");
                                reader.with_filename(Path::new(&filename).to_path_buf()).unwrap();
                                if reader.changed().unwrap() {
                                    log::debug!("Binlog file changed...{filename}");
                                    cloned_tx.send(filename.clone()).unwrap();
                                    // 如果发完这个,然后再发心跳,最终心跳会把这个冲掉
                                    continue;
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

                                        // 如果发完这个,然后再发心跳,最终心跳会把这个冲掉
                                        continue;
                                    }
                                }
                                None => {
                                    log::error!("Binlog index file {} is empty?", cloned_relay_log_index_path.display())
                                }
                            }
                        },
                    }
                    // 心跳
                    cloned_tx.send(format!("{}", EventType::HEARTBEAT_EVENT as u8)).unwrap();
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
            let mut sql_command = SqlCommand::new(&mut session_vars);

            let mut binlog_index =
                BinlogIndex::try_from(cloned_relay_log_index_path.to_path_buf()).unwrap();
            let mut binlog_reader = MyBinlogFileReader::default();

            let mut phase = ConnectionLifecycle::ConnectionPhaseInitialHandshake;
            let mut buffer = handshake.init_plain_packet();
            // 是否开始dump binlog
            // let mut open_binlog_dump = false;
            // let mut open_need_ack = false;

            let mut replica = ReplicaInfo::new();

            'outer: loop {
                select! {
                // biased;
                    Ok(_) = cloned_rx.changed() => {
                    // Ok(_) = cloned_rx.changed(), if open_binlog_dump => {
                        // 收到binlog变更的事件
                        log::debug!("cloned_rx.changed...::: binlog_dump:{}, binlog_dump_gtid:{}", replica.binlog_dump(), replica.binlog_dump_gtid());
                        let filename = format!("{}", *cloned_rx.borrow_and_update());
                        handle_binlog_changed(&filename, &mut binlog_reader, &master_info, &mut replica, &mut socket, &mut packet_codec).await.unwrap();
                    },

                    Ok(ready) = socket.ready(Interest::READABLE | Interest::WRITABLE) => {

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

                            let mut packet = encode_packet(&mut buffer, &mut packet_codec).unwrap();
                            match net_write(&mut socket, &mut packet).await {
                                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                    continue;
                                }
                                Err(e) => {
                                    log::info!("Connection closed: {e}");
                                    break 'outer;
                                }
                                _ => {}
                            };


                        } else if ready.is_readable() {
                            log::debug!("socket.readable...{:?}", phase);
                            // let mut timeout = false;
                            // 可以读数据
                            match net_read(&mut socket).await {
                                Ok(mut packet) => {
                                    buffer.extend_from_slice(&decode_packet(&mut packet, &mut packet_codec).unwrap());
                                },
                                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                    continue;
                                }
                                Err(e) => {
                                    log::info!("Connection closed: {e}");
                                    break 'outer;
                                }
                            };
                            log::debug!("socket.readable...net_read...ok....{:?}", phase);

                            // if !timeout {
                            match phase {
                                ConnectionLifecycle::ConnectionPhaseInitialHandshakeResponse => {
                                    // 验证密码
                                    let (varify_pass, mut packet) = handshake.auth_with_response(&mut buffer).unwrap();
                                    if varify_pass {
                                        // 验证通过
                                        phase = ConnectionLifecycle::ConnectionPhaseAuthenticationResponse;
                                    }
                                    sql_command.set_client_capabilities(*handshake.client_capabilities());
                                    let mut dst = encode_packet(&mut packet, &mut packet_codec).unwrap();
                                    net_write(&mut socket, &mut dst).await.unwrap();

                                    log::debug!("varify_pass: {varify_pass}");
                                }
                                ConnectionLifecycle::ConnectionPhaseAuthenticationResponse | ConnectionLifecycle::CommandPhase => {
                                    // 是否收到其他命令
                                    if let Some(com) = match_binlog_command(&buffer).unwrap() {
                                        phase = ConnectionLifecycle::CommandBinlogDumpPhase;
                                        match com {
                                            Command::COM_BINLOG_DUMP => {
                                                // 开始接收监听事件
                                                // open_binlog_dump = true;
                                                if replica.need_ack() {
                                                    // 解析ack
                                                    let (filename, pos) = check_ack(&mut buffer, &mut binlog_reader).unwrap();
                                                    log::info!("Ack {} {} recv from slave", filename, pos);

                                                } else {
                                                    // 解析命令
                                                    let var1 = sql_command.get_session_var("@rpl_semi_sync_slave").unwrap();
                                                    let var2 = sql_command.get_session_var("@rpl_semi_sync_replica").unwrap();
                                                    // rpl_semi_sync_master_enabled = rpl_semi_sync_master_enabled & (*var1.value() == "1".to_owned() || *var2.value() == "1".to_owned());
                                                    replica.with_rpl_semi_sync_replica(*var1.value() == "1".to_owned() || *var2.value() == "1".to_owned());

                                                    handle_com_binlog_dump(&mut socket,
                                                            &mut packet_codec,
                                                            &mut buffer,
                                                            &mut binlog_index,
                                                            &mut binlog_reader,
                                                            &master_info,
                                                            &mut replica
                                                    ).await.unwrap();
                                                }
                                            },
                                            // Command::COM_BINLOG_DUMP_GTID => handle_com_binlog_dump_gtid(),
                                            _ => {}
                                        }
                                        buffer.clear();
                                    } else {
                                        let mut data = sql_command.read(&mut buffer, &mut replica).unwrap();
                                        buffer.clear();
                                        'inner: loop {
                                            if data.is_empty() {
                                                break 'inner;
                                            }
                                            let mut packet = data.remove(0);
                                            let mut dst = encode_packet(&mut packet, &mut packet_codec).unwrap();
                                            net_write(&mut socket, &mut dst).await.unwrap();
                                        }
                                        phase = ConnectionLifecycle::CommandPhase;
                                    }
                                }
                                _ => {
                                    // 其他阶段
                                }
                            // }
                            }
                        }
                    }
                    // _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    //     log::trace!("sleep 1s");
                    // }


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
    master: &MasterInfo,
    replica: &mut ReplicaInfo,
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
) -> Result<()> {
    log::debug!(
        "rpl_semi_sync_master_enabled={}, need_ack={}, filename: {}",
        master.rpl_semi_sync_master_enabled() & replica.rpl_semi_sync_replica(),
        replica.need_ack(), filename
    );
    // 发送心跳包
    if *filename == format!("{}", EventType::HEARTBEAT_EVENT as u8) {
        log::info!("HEARTBEAT_EVENT...");
        return Ok(());
    }
    // 文件名不一样
    binlog_dump(
        stream,
        packet_codec,
        binlog_reader,
        master,
        replica,
        filename.clone(),
        0,
    )
    .await
}

//
async fn net_write(stream: &mut TcpStream, src: &mut BytesMut) -> io::Result<()> {
    log::trace!("net_write::src: {:?}", src);
    log::trace!("net_write::src:vec: {:?}", src.to_vec());
    // match stream.write_all(&src).await {
    match stream.try_write(&src) {
        Ok(0) => return Err(io::ErrorKind::ConnectionAborted.into()),
        Ok(_) => stream.flush().await?,
        Err(e) => return Err(e),
    }
    Ok(())
}

async fn net_read<'a>(stream: &mut TcpStream) -> io::Result<BytesMut> {
    let mut buf = [0; READ_BUFFER_SIZE];
    // let n = match stream.read(&mut buf).await {
    let n = match stream.try_read(&mut buf) {
        Ok(0) => return Err(io::ErrorKind::ConnectionAborted.into()),
        Ok(n) => n,
        Err(e) => return Err(e),
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
    master: &MasterInfo,
    replica: &mut ReplicaInfo,
) -> Result<()> {
    let com_binlog_dump = ComBinlogDump::deserialize((), &mut ParseBuf(&buffer))?;
    let request_filename = com_binlog_dump.filename().to_string();
    log::info!(
        "handle_binlog_command::request_filename: {request_filename}, request_pos: {}",
        com_binlog_dump.pos()
    );
    binlog_index.with_request_binlog_filename(request_filename.clone());
    replica.with_filename_pos(request_filename.clone(), com_binlog_dump.pos());

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
        master,
        replica,
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
            master,
            replica,
            filename,
            0,
        )
        .await?;
    }

    // 已经追上日志了。
    log::info!("Binlog catched up latest pos");
    replica.with_need_ack(true);
    log::info!("Master need_ack enable.");

    Ok(())
}

async fn binlog_dump(
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
    binlog_reader: &mut MyBinlogFileReader,
    master: &MasterInfo,
    replica: &mut ReplicaInfo,
    filename: String,
    skip_pos: u32,
) -> Result<()> {
    binlog_reader.with_filename(Path::new(&filename).to_path_buf())?;
    let mut binlog_file = binlog_reader.get_binlog_file()?;
    while let Some(event) = binlog_file.next() {
        let event = event?;
        binlog_reader.with_pos(event.header().log_pos() as u64);
        if skip_pos > 0 && event.header().log_pos() < skip_pos {
            continue;
        }

        log::trace!("event::header::server_id:{}, replica::server_id:{}", event.header().server_id(), replica.server_id());
        if event.header().server_id() == replica.server_id() {
            return Err(anyhow!(
                "Slave has same server-id as master ({})",
                replica.server_id()
            ));
        }
        binlog_reader.with_server_id(event.header().server_id());
        // master.with_server_id(server_id);
        // 将事件发给客户端
        let mut src = event_to_bytes(
            event,
            master.rpl_semi_sync_master_enabled() & replica.rpl_semi_sync_replica(),
            replica.need_ack(),
        );
        let mut dst = encode_packet(&mut src, packet_codec)?;
        net_write(stream, &mut dst).await?;
    }

    Ok(())
}
