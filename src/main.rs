use bytes::{Buf, BufMut, BytesMut};
use mysql_async::consts::{CapabilityFlags, StatusFlags};
use mysql_common::{
    io::ParseBuf,
    packets::AuthPlugin,
    proto::{MyDeserialize, MySerialize, codec::PacketCodec},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::packets::{HandshakePacket, HandshakeResponse};
// mod codec;
mod packets;

const SCRAMBLE_BUFFER_SIZE: usize = 20;
const PLAIN_OK: &[u8] = b"\x00\x01\x00\x02\x00\x00\x00";

const SERVER_VERSION: &str = "8.0.41";

// 协议版本：10
const PROTOCOL_VERSION: u8 = 10;
// 默认语言
const SERVER_LANGUAGE: u8 = 8;
// 缓冲区大小
const READ_BUFFER_SIZE: usize = 0xFFFF;

// Build Handshake Packet
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
fn new_handshake_packet(scramble: &[u8; SCRAMBLE_BUFFER_SIZE]) -> BytesMut {
    let mut scramble_1 = [0u8; 8];
    let (_scramble_1, scramble_2) = scramble.split_at(8);
    scramble_1.copy_from_slice(&_scramble_1);

    let capabilities = CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_PLUGIN_AUTH
        | CapabilityFlags::CLIENT_SECURE_CONNECTION
        | CapabilityFlags::CLIENT_CONNECT_WITH_DB
        | CapabilityFlags::CLIENT_TRANSACTIONS
        | CapabilityFlags::CLIENT_CONNECT_ATTRS
        | CapabilityFlags::CLIENT_DEPRECATE_EOF;

    let packet = HandshakePacket::new(
        PROTOCOL_VERSION,
        SERVER_VERSION.as_bytes(),
        0x0000000b,
        scramble_1,
        Some(scramble_2),
        capabilities,
        SERVER_LANGUAGE,
        StatusFlags::SERVER_STATUS_AUTOCOMMIT,
        AuthPlugin::MysqlNativePassword.as_bytes().into(),
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
) -> Result<BytesMut, Box<dyn std::error::Error>> {
    println!("resp::src: {:?}", src);
    println!("resp::src:vec: {:?}", src.to_vec());

    let resp = HandshakeResponse::deserialize((), &mut ParseBuf(&src))?;

    println!("{:?}", resp);

    let mut auth = false;
    let user = String::from_utf8_lossy(resp.user()).to_string();
    let empty_pass = if resp.scramble_buf().len() > 0 {
        "YES"
    } else {
        "NO"
    };

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
                        if p == resp.scramble_buf() {
                            src.clear();
                            src.extend_from_slice(PLAIN_OK);
                            auth = true;
                        }
                    }
                    mysql_common::packets::AuthPluginData::Sha2(p) => {
                        println!("p: {:?}", p);
                        println!("scramble_buf: {:?}", resp.scramble_buf());
                        if p == resp.scramble_buf() {
                            src.clear();
                            src.extend_from_slice(PLAIN_OK);
                            auth = true;
                        }
                    }
                    mysql_common::packets::AuthPluginData::Clear(_p) => {}
                    mysql_common::packets::AuthPluginData::Ed25519(_p) => {}
                }
            }
        }
    }

    if !auth {
        src.clear();
        // 错误包标志
        src.put_u8(0xff);
        // 错误代码 (1045 = 访问被拒绝)
        src.extend_from_slice(&0x0415u16.to_le_bytes());
        // SQL状态标志
        src.put_u8(0x23); // '#'
        src.extend_from_slice(b"28000"); // 访问被拒绝的SQL状态码
        // 错误消息
        src.extend_from_slice(
            format!(
                "Access denied for user '{}'@'localhost' (using password: {})",
                user, empty_pass
            )
            .as_bytes(),
        );
    }

    Ok(std::mem::take(src))
}

// 发送OK包
// async fn send_ok_packet(
//     stream: &mut TcpStream,
//     packet_codec: &mut PacketCodec,
// ) -> Result<(), Box<dyn std::error::Error>> {
//     let mut packet = BytesMut::new();

//     // OK包标志
//     packet.put_u8(0x00);

//     // 受影响的行数 (0)
//     packet.extend_from_slice(&[0x00]);

//     // 最后插入的ID (0)
//     packet.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

//     // 服务器状态
//     packet.extend_from_slice(&SERVER_STATUS.to_le_bytes());

//     // 警告数 (0)
//     packet.extend_from_slice(&[0x00, 0x00]);

//     send_packet(stream, &mut packet, packet_codec).await?;
//     Ok(())
// }

// // 发送错误包
// async fn send_error_packet(
//     stream: &mut TcpStream,
//     message: &str,
//     packet_codec: &mut PacketCodec,
// ) -> Result<(), Box<dyn std::error::Error>> {
//     let mut packet = BytesMut::new();
//     // 错误包标志
//     packet.put_u8(0xff);

//     // 错误代码 (1045 = 访问被拒绝)
//     packet.extend_from_slice(&0x0415u16.to_le_bytes());

//     // SQL状态标志
//     packet.put_u8(0x23); // '#'
//     packet.extend_from_slice(b"28000"); // 访问被拒绝的SQL状态码

//     // 错误消息
//     packet.extend_from_slice(message.as_bytes());

//     send_packet(stream, &mut packet, packet_codec).await?;
//     Ok(())
// }

// // 握手响应数据结构
// #[derive(Debug)]
// struct HandshakeResponse {
//     capabilities: u32,
//     username: String,
//     auth_response: Vec<u8>,
// }

enum ServerPhaseDesc {
    InitialHandshakePacket,
    ClientResponse, // Read
    ServerResponse, // Write
    ClientHandshakeResponse,
    #[allow(unused)]
    AuthenticationMethodSwitch,
    #[allow(unused)]
    AuthenticationExchangeContinuation,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("MySQL Server listen to 127.0.0.1:8080...");
    // let user_db = UserDB::new();

    loop {
        let (mut socket, _) = listener.accept().await?;
        // let cloned_user_db = user_db.clone();

        tokio::spawn(async move {
            let mut scramble = [0u8; SCRAMBLE_BUFFER_SIZE];
            let mut status = ServerPhaseDesc::InitialHandshakePacket;
            let mut next_status = ServerPhaseDesc::InitialHandshakePacket;
            let mut packet_codec = PacketCodec::default();
            let mut buffer = BytesMut::new();
            loop {
                match status {
                    ServerPhaseDesc::InitialHandshakePacket => {
                        // 1. 生成随机挑战值
                        #[allow(deprecated)]
                        rand::Rng::fill(&mut rand::thread_rng(), &mut scramble);

                        // 2. 发送初始握手包
                        buffer = new_handshake_packet(&scramble);
                        println!("buffer.len: {:?}", buffer.len());

                        // 3 发送挑战包给客户端
                        status = ServerPhaseDesc::ServerResponse;

                        // 4. 收到客户端握手包并响应给客户端
                        next_status = ServerPhaseDesc::ClientHandshakeResponse;
                    }
                    ServerPhaseDesc::ClientResponse => {
                        // 4. 接收客户端响应
                        let mut buf = [0; READ_BUFFER_SIZE];
                        let n = match socket.read(&mut buf).await {
                            Ok(0) => return,
                            Ok(n) => n,
                            Err(e) => {
                                eprintln!("failed to read from socket; err = {:?}", e);
                                return;
                            }
                        };

                        let data = &buf[0..n];
                        println!("recv:buf:{:?}", data);

                        buffer.clear();
                        let mut src = BytesMut::new();
                        src.extend_from_slice(data);
                        println!("recv:buffer:src:{:?}", src);
                        // !!!!!!! decode后buffer没有数据，且没有报错，待查
                        packet_codec.decode(&mut src, &mut buffer).unwrap();
                        // 临时用切片的方式，因为还要用packet_codec.seq_id
                        // buffer.clear();
                        // buffer.extend_from_slice(&data[4..]);

                        println!("recv:buffer:{:?}", buffer);
                        println!("recv:buffer:vec:{:?}", buffer.to_vec());

                        match next_status {
                            // 5. 解析握手包
                            ServerPhaseDesc::ClientHandshakeResponse => {
                                buffer =
                                    match handle_handshake_response(&mut buffer, &scramble).await {
                                        Ok(buf) => buf,
                                        Err(e) => {
                                            println!("{e}");
                                            return;
                                        }
                                    };

                                // 6.验证客户端身份后响应给客户端
                                status = ServerPhaseDesc::ServerResponse;
                            }
                            _ => {}
                        }
                    }

                    ServerPhaseDesc::ServerResponse => {
                        // 3 发送挑战包给客户端
                        // 6.验证客户端身份后响应给客户端
                        // Write the data back
                        if buffer.has_remaining() {
                            let mut src = BytesMut::new();
                            src.extend_from_slice(&buffer);

                            buffer.clear();
                            packet_codec.encode(&mut src, &mut buffer).unwrap();

                            println!("ServerResponse:: buffer:: {:?}", buffer);
                            println!(">>>>ServerResponse:: buffer:vec:: {:?}", buffer.to_vec());
                            if let Err(e) = socket.write_all(&buffer).await {
                                eprintln!("failed to write to socket; err = {:?}", e);
                                return;
                            }
                            buffer.clear();
                            socket.flush().await.unwrap();
                        }

                        // 等待客户端响应
                        status = ServerPhaseDesc::ClientResponse;
                    }

                    _ => {}
                }
                // 4. 验证客户端身份
                // if verify_credentials(&cloned_user_db, &response, &scramble).unwrap() {
                //     println!("用户认证成功: {}", response.username);
                //     // 发送认证成功包
                //     send_ok_packet(&mut socket).await.unwrap();
                //     println!("已发送认证成功响应，等待从库同步请求");
                // } else {
                //     println!("用户认证失败: {}", response.username);
                //     // 发送认证失败包
                //     send_error_packet(&mut socket, "Access denied for user")
                //         .await
                //         .unwrap();
                // }
            }
        });
    }
}

#[allow(unused)]
#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};
    use mysql_async::{
        OkPacket,
        consts::{CapabilityFlags, StatusFlags},
    };
    use mysql_common::{
        packets::HandshakePacket,
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
    fn test_parse_handshake_packet() {
        let data = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];

        let raw_bytes = [
            141, 166, 255, 25, 0, 0, 0, 1, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 114, 111, 111, 116, 0, 0, 99, 97, 99, 104, 105, 110, 103, 95, 115,
            104, 97, 50, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0,
        ];
    }
}
