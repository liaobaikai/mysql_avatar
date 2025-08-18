use core::time;
use std::{path::Path, thread, time::Duration};

use anyhow::{Result, anyhow};
use bytes::BytesMut;
use mysql_async::{binlog::events::Event, consts::StatusFlags};
use mysql_common::{
    io::ParseBuf,
    packets::ComBinlogDump,
    proto::{MyDeserialize, codec::PacketCodec},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
    sync::{broadcast, mpsc},
};
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

use crate::{
    auth::handshake::Handshake,
    com::{
        SqlCommand,
        binlog::{MyBinlogFileReader, match_binlog_command},
    },
    consts::Command,
    variable::{SYSVARS, Variable},
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
}

// 初始化服务端
pub async fn init_server(port: u16) -> Result<()> {
    let addr = ("0.0.0.0", port);
    let listener = TcpListener::bind(addr).await?;
    println!("MySQL Server listen to {}:{}...", addr.0, addr.1);

    // 启动另一个线程，监控binlog日志是否有变化，主要是判断大小与relaylog文件大小
    // 如果有变化，通过另一个消息通道通知读取并往stream发送。
    // 如果没有变化，则继续等待。
    let (tx, _) = broadcast::channel(0xFFFF);

    let daemon_tx = tx.clone();
    tokio::spawn(async move {
        let mut stream = daemon_tx.subscribe();
        // 只是保持接收者活跃，不做实际处理
        loop {
            if let Err(e) = stream.recv().await {
                println!("e: {e}");
                // 通道真正关闭时退出
                break;
            }
        }
        println!("守护接收者: 通道已关闭");
    });

    // 启动一个专门用于发送广播消息的任务
    let cloned_tx = tx.clone();
    tokio::spawn(async move {
        // let mut intv =
        //     tokio::time::interval_at(tokio::time::Instant::now() + START_AFTER, INTERVAL);
        // let start_secs = START_AFTER.as_secs();
        // let intv_secs = START_AFTER.as_secs();
        log::info!(
            "Binlog watcher inited..."
        );

        let mut reader: MyBinlogFileReader;
        loop {
            // select! {
            //     // _ = cloned_token.cancelled() => {
            //     //     log::info!("Sender#{server_id} cancelled");
            //     //     take_back_from_scratch(None);
            //     //     break;
            //     // },
            //     // Some(v) = rx.next() => {

            //     // },
            //     _ = intv.tick() => {
                    println!("intv.tick =====");
                    // 发送日志
                    reader = MyBinlogFileReader::try_from(Path::new("/Users/lbk/mysqltest/data/relay-bin/relay-bin.000000001")).unwrap();
                    reader.read_all_fn(0, |ev: Event| {
                        if let Err(e) = cloned_tx.send(ev) {
                            log::error!("send: {e}")
                        }
                        Ok(())
                    }).await.unwrap();

                    log::debug!("intv.tick ...");
                // }
            // }
            tokio::time::sleep(INTERVAL).await;
        }
    });

    loop {
        let mut rx = tx.subscribe();
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut handshake = Handshake::new();
            let mut session_vars: Vec<Variable> = Vec::new();
            let mut packet_codec = PacketCodec::default();

            for var in SYSVARS.iter() {
                session_vars.push(var.clone());
            }
            let mut sql_command = SqlCommand::new(session_vars);
            let mut phase = ConnectionLifecycle::ConnectionPhaseInitialHandshake;
            loop {
                select! {
                    Ok(_) = socket.readable() => {
                        // 可以读数据
                        if let Err(e) = handle_read(&mut socket, &mut packet_codec, &mut handshake, &mut phase, &mut sql_command).await {
                            println!("Read data failed, {e}");
                            break;
                        }
                    }
                    Ok(_) = socket.writable() => {
                        // 可以写数据
                        handle_write(&mut socket, &mut packet_codec, &mut handshake, &mut phase, &mut sql_command).await.unwrap();
                    }

                    // 定时发送过来的
                    Ok(event) = rx.recv() => {
                        // 如何通知我要取哪些日志？
                        log::debug!("recv>>>>>>event: {:?}", event);
                    }
                }
            }
        });
    }
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
async fn net_write(
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
    src: &mut BytesMut,
) -> Result<()> {
    println!("net_write: {:?}", src);
    let mut buffer = BytesMut::with_capacity(src.len() + 4);
    packet_codec.encode(src, &mut buffer).unwrap();
    if let Err(e) = stream.write_all(&buffer).await {
        return Err(anyhow!("failed to write to stream; err = {:?}", e));
    }
    stream.flush().await.unwrap();
    Ok(())
}

async fn net_read<'a>(stream: &mut TcpStream) -> Result<BytesMut> {
    let mut buf = [0; READ_BUFFER_SIZE];
    let n = match stream.read(&mut buf).await {
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
            // if let Some(com) = match_binlog_command(&mut buffer)? {
            //     handle_binlog_command(com, &mut buffer)?;
            //     return Ok(());
            // }

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

fn handle_binlog_command(com: Command, buffer: &mut BytesMut) -> Result<()> {
    match com {
        Command::COM_BINLOG_DUMP => {
            let com_binlog_dump = ComBinlogDump::deserialize((), &mut ParseBuf(&buffer))?;
            let _filename = com_binlog_dump.filename().to_string();
            println!("_filename: {_filename}");
            let mut reader = MyBinlogFileReader::try_from(Path::new(
                "/Users/lbk/mysqltest/data/relay-bin/relay-bin.000000001",
            ))?;
            reader.run_dumper(com_binlog_dump.pos())?;
        }
        Command::COM_BINLOG_DUMP_GTID => {}
        _ => {}
    }
    Ok(())
}
