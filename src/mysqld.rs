use anyhow::{Result, anyhow};
use bytes::BytesMut;
use mysql_async::consts::StatusFlags;
use mysql_common::proto::codec::PacketCodec;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
};

use crate::{
    auth::handshake::Handshake,
    com::SqlCommand,
    variable::{SYSVARS, Variable},
};

// 缓冲区大小
const READ_BUFFER_SIZE: usize = 0xFFFF;

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

    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            // let (tx, _rx) = mpsc::unbounded_channel::<BytesMut>();
            // let mut rx = UnboundedReceiverStream::new(_rx);

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
                        read_from_stream(&mut socket, &mut packet_codec, &mut handshake, &mut phase, &mut sql_command).await.unwrap();
                    }
                    Ok(_) = socket.writable() => {
                        // 可以写数据
                        write_to_stream(&mut socket, &mut packet_codec, &mut handshake, &mut phase, &mut sql_command).await.unwrap();
                    }
                }
            }
        });
    }
}

// 写数据
async fn write_to_stream(
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
    let mut buffer = BytesMut::with_capacity(src.len() + 4);
    packet_codec.encode(src, &mut buffer).unwrap();
    if let Err(e) = stream.write_all(&buffer).await {
        return Err(anyhow!("failed to write to stream; err = {:?}", e));
    }
    stream.flush().await.unwrap();
    Ok(())
}

// 写数据
async fn read_from_stream(
    stream: &mut TcpStream,
    packet_codec: &mut PacketCodec,
    handshake: &mut Handshake,
    phase: &mut ConnectionLifecycle,
    sql_command: &mut SqlCommand,
) -> Result<()> {
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

    let mut buffer = BytesMut::with_capacity(data.len() - 4);
    let mut src = BytesMut::new();
    src.extend_from_slice(data);

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
            // 回复客户端
            net_write(stream, packet_codec, &mut data).await?;
        }
        ConnectionLifecycle::ConnectionPhaseAuthenticationResponse => {
            // 是否收到其他命令
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
