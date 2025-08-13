use bytes::{Buf, BufMut, BytesMut};
use mysql_async::{
    OkPacket,
    consts::{CapabilityFlags, StatusFlags},
};
use mysql_common::{
    io::ParseBuf,
    packets::{
        AuthSwitchRequest, ErrPacket, HandshakePacket, OkPacketBody, SqlState, SqlStateMarker,
        SslRequest,
    },
    proto::{MyDeserialize, MySerialize, codec::PacketCodec},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{
    codec::PlainPacketCodec,
    packets::{HandshakeResponse, ServerError},
};
mod codec;
mod packets;

const SCRAMBLE_BUFFER_SIZE: usize = 20;
const PLAIN_OK: &[u8] = b"\x00\x01\x00\x02\x00\x00\x00";

// 调整能力集常量，增加常见的认证相关能力
const SERVER_CAPABILITIES: u32 = 0x0000_0200 | 0x0000_8000 | 0x0008_0000; // 增加 CLIENT_PLUGIN_AUTH 能力
// const SCRAMBLE_BUFFER_SIZE: usize = 20;
const PROTOCOL_VERSION: u8 = 10;
// const SERVER_CAPABILITIES: u32 = 0x800001ff; // 基本能力集
const SERVER_LANGUAGE: u8 = 8; // utf8
const SERVER_STATUS: u16 = 2;
fn build_handshake_packet(scramble: &[u8; 20]) -> BytesMut {
    let mut packet = BytesMut::new();

    // 协议版本
    packet.put_u8(PROTOCOL_VERSION);

    // 服务器版本 (例如: 5.7.30)
    packet.extend_from_slice(b"8.0.41");
    packet.put_u8(0x00);

    // 连接ID
    packet.extend_from_slice(&0x01234567u32.to_le_bytes());

    // 挑战值前8字节
    packet.extend_from_slice(&scramble[0..8]);
    packet.put_u8(0x00); // 填充

    // 服务器能力标志 (低2字节)
    packet.extend_from_slice(&(SERVER_CAPABILITIES as u16).to_le_bytes());

    // 服务器字符集
    packet.put_u8(SERVER_LANGUAGE);

    // 服务器状态
    packet.extend_from_slice(&SERVER_STATUS.to_le_bytes());

    // 服务器能力标志 (高2字节)
    packet.extend_from_slice(&((SERVER_CAPABILITIES >> 16) as u16).to_le_bytes());

    // 挑战值长度
    packet.put_u8(SCRAMBLE_BUFFER_SIZE as u8);

    // 保留字节
    packet.extend_from_slice(&[0; 10]);

    // 挑战值剩余部分
    packet.extend_from_slice(&scramble[8..]);
    packet.put_u8(0x00); // 终止符

    packet
}

// 构建握手包
fn new_handshake_packet(scramble: &[u8; 20]) -> BytesMut {
    let mut scramble_1 = [0u8; 8];
    let (_scramble_1, scramble_2) = scramble.split_at(8);
    scramble_1.copy_from_slice(&_scramble_1);

    let packet = HandshakePacket::new(
        PROTOCOL_VERSION,
        "8.0.41".as_bytes(),
        0x01234567u32,
        scramble_1,
        Some(scramble_2),
        CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_SECURE_CONNECTION,
        SERVER_LANGUAGE,
        StatusFlags::default(),
        Some(b"mysql_native_password"),
    );

    let mut src = BytesMut::new();
    let mut buf: Vec<u8> = Vec::new();
    packet.serialize(&mut buf);
    src.extend_from_slice(&buf);
    src
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
// 发送握手初始化包
async fn send_handshake_packet(
    scramble: &[u8; 20],
    _packet_codec: &mut PacketCodec,
) -> Result<BytesMut, Box<dyn std::error::Error>> {
    // 发送数据包 (长度前缀 + 序列号 + 数据)

    // sent::dst: b"4\0\0\0\n8.0.41\0gE#\x01\x1c!\x8bH\xe1h\x83\xf6\0\0\x82\x08\x02\0\x08\0\x14\0\0\0\0\0\0\0\0\0\0I\x0f\x06\x15\x03\xf4x&F\xee\x92g\0"
    // sent::dst: b"I\0\0\0\n8.0.41\0gE#\x01\xd7\xe4\xe7z\xfa\x1c\xe8\xeb\0\0\x82\x08\0\0\x08\0\x14\0\0\0\0\0\0\0\0\0\03LC\xd4\xae\xf1\xcc\0\x18\xc8\xd1Omysql_native_password\0"

    // ServerResponse:: buffer:: b"4\0\0\0\n5.7.30\0gE#\x01\xda\x13g\x1e\x9c\xe5\xa8\xec\0\xff\x01\x08\x02\0\x08\x80\x14\0\0\0\0\0\0\0\0\0\0\xaf\xf6\xfc\xdd\x98\xefC\x92\x02D\xf9\xd6\0"
    let src = new_handshake_packet(scramble);
    // let mut src = build_handshake_packet(scramble);
    // let mut dst = BytesMut::new();

    // println!("sent::src.first: {:?}", src.to_vec());
    // packet_codec.encode(&mut src, &mut dst).unwrap();
    // println!("sent::dst: {dst:?}");

    // stream.write_all(&dst).await?;
    // stream.flush().await?;

    // send_packet(stream, &mut src).await?;
    Ok(src)
}

// 接收并解析客户端握手响应
async fn handle_handshake_response<'a>(
    src: &mut BytesMut,
    scramble: &[u8; 20],
) -> Result<BytesMut, Box<dyn std::error::Error>> {
    println!("resp::src: {:?}", src);
    println!("resp::src:vec: {:?}", src.to_vec());

    // No SSL Exchange
    // resp::src: b"P\0\0\x01\x81\xa2\x0f\x01\0\0@\0-\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0root\0\x14\x8e\x1cp\n\r\x15\x01O\xd5\xe2\xc7$p\xda\xcfGs\x04q\x1bmysql_native_password\0"
    // resp::dst: b"\x81\xa2\x0f\x01\0\0@\0-\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0root\0\x14\x8e\x1cp\n\r\x15\x01O\xd5\xe2\xc7$p\xda\xcfGs\x04q\x1bmysql_native_password\0"

    // SSL Exchange
    // resp::src: b" \0\0\x01\x85\xae\xff\x19\0\0\0\x01\x1c\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
    // resp::dst: b"\x85\xae\xff\x19\0\0\0\x01\x1c\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"

    // resp::src: b"&\0\0\x01\x85\xa6\xff\x19\0\0\0\x01\xff\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0root\0\0"
    // resp::dst: b"\x85\xa6\xff\x19\0\0\0\x01\xff\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0root\0\0"

    let mut buf = ParseBuf(&src);
    // match OldHandshakeResponse::deserialize((), &mut buf) {
    //     Ok(ok) => {
    //         println!("ok: {:?}", ok);
    //     },
    //     Err(e) => {
    //         println!("{:?}", e);
    //     }
    // }

    // if let Ok(ssl_request) = SslRequest::deserialize((), &mut buf) {
    //     // ssl_request:SslRequest { capabilities: Const(CapabilityFlags(CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_LOCAL_FILES | CLIENT_PROTOCOL_41 | CLIENT_INTERACTIVE | CLIENT_SSL | CLIENT_TRANSACTIONS | CLIEN0\x16\0\0\0\x17\0\0\0\r\0*\0(\x04\x03\x05\x03\x06\x03\x08\x07\x08\x08\x0T_SECURE_CONNECTION | CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS | CLIENT_PS_MULTI_RESULTS | CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_ATTRS | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_CAN_HANDLE_EXPIREDxcb\xbd\x1eqsXk\xee\xf1\xf4\x9e\x1e/\xe5WU%\xba\0c\xba\xa1J\x13\x9f\xd2\_PASSWORDS | CLIENT_SESSION_TRACK | CLIENT_DEPRECATE_EOF | CLIENT_QUERY_ATTRIBUTES | MULTI_FACTOR_AUTHENTICATION), PhantomData<mysql_common::misc::raw::int::LeU32>),
    //     // max_packet_size: 16777216, character_set: 28, __skip: Skip }
    //     println!("ssl_request:{:?}", ssl_request);

    //     if ssl_request
    //         .capabilities()
    //         .contains(CapabilityFlags::CLIENT_SSL)
    //     {
    //         // 同意SSL
    //         // src.clear();
    //         dst.clear();
    //         // src.put_u8(0x00);
    //         // packet_codec.encode(src, &mut dst).unwrap();

    //         // 不支持SSL
    //         // 返回Err_Packet
    //         let server_error = ServerError::new(
    //             2026,
    //             None,
    //             "ER_SSL_NOT_SUPPORTED".as_bytes(),
    //             ssl_request.capabilities(),
    //         );
    //         let mut buf = Vec::new();
    //         server_error.serialize(&mut buf);
    //         dst.extend_from_slice(&buf);
    //     }
    //     return Ok(dst);
    // }

    let response = HandshakeResponse::deserialize((), &mut buf)?;

    println!("{:?}", response);
    // response: HandshakeResponse { capabilities: Const(CapabilityFlags(CLIENT_LONG_PASSWORD | CLIENT_LOCAL_FILES | CLIENT_PROTOCOL_41 | CLIENT_TRANSACTIONS | CLIENT_SECURE_CONNECTION | CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS | CLIENT_PS_MULTI_RESULTS | CLIENT_PLUGIN_AUTH | CLIENT_DEPRECATE_EOF), PhantomData<mysql_common::misc::raw::int::LeU32>),
    // max_packet_size: 4194304, collation: 45, scramble_buf: Right(Left(RawBytes { value: "�iRc�$���ǠD�b�'��j\u{19}", max_len: "255" })),
    // user: RawBytes { value: "root", max_len: "18446744073709551615" }, db_name: None, auth_plugin: Some(MysqlNativePassword),
    // connect_attributes: None, mariadb_ext_capabilities: Const(MariadbCapabilities(0x0), PhantomData<mysql_common::misc::raw::int::LeU32>) }
    if response
        .capabilities()
        .contains(CapabilityFlags::CLIENT_PLUGIN_AUTH)
    {
        if let Some(auth_plugin) = response.auth_plugin() {
            if let Some(data) = auth_plugin.gen_data(Some("123456"), scramble) {
                match data {
                    mysql_common::packets::AuthPluginData::Old(_p) => {}
                    mysql_common::packets::AuthPluginData::Native(p) => {
                        println!("p: {:?}", p);
                        println!("scramble_buf: {:?}", response.scramble_buf());
                        if p == response.scramble_buf() {
                            src.clear();
                            src.extend_from_slice(PLAIN_OK);
                        }
                    }
                    mysql_common::packets::AuthPluginData::Sha2(p) => {
                        println!("p: {:?}", p);
                        println!("scramble_buf: {:?}", response.scramble_buf());
                        if p == response.scramble_buf() {
                            src.clear();
                            src.extend_from_slice(PLAIN_OK);
                        }
                    }
                    mysql_common::packets::AuthPluginData::Clear(_p) => {}
                    mysql_common::packets::AuthPluginData::Ed25519(_p) => {}
                }
            }
        }
    }

    // src.extend_from_slice(PLAIN_OK);
    // packet_codec.encode(src, &mut dst).unwrap();

    let mut dst = BytesMut::new();
    dst.extend_from_slice(PLAIN_OK);
    Ok(dst)
}

// 验证客户端凭据
// fn verify_credentials(
//     user_db: &UserDB,
//     response: &HandshakeResponse,
//     scramble: &[u8; 20],
// ) -> Result<bool, Box<dyn std::error::Error>> {
// let password_hash = match user_db.get_password_hash(&response.username) {
//     Some(hash) => hash,
//     None => return Ok(false),
// };

// if response.auth_response.is_empty() {
//     return Ok(false);
// }

// MySQL认证算法：
// 1. 服务器：生成随机挑战值 scramble
// 2. 客户端：计算 SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password)))
// 3. 服务器：使用存储的 SHA1(password) 重新计算并比较

// 将存储的哈希转换为字节
// let stored_hash = password_hash.as_bytes();
// if stored_hash.len() != 20 {
//     return Ok(false);
// }

// // 计算 SHA1(scramble + stored_hash)
// let mut sha1 = Sha1::new();
// sha1.update(scramble);
// sha1.update(&stored_hash);
// let hash_stage2 = sha1.finalize();

// // 计算客户端应有的响应：stored_hash XOR hash_stage2
// let expected_response: Vec<u8> = stored_hash.iter()
//     .zip(hash_stage2.as_slice().iter())
//     .map(|(a, b)| a ^ b)
//     .collect();

// 比较客户端响应与计算结果
// Ok(expected_response == response.auth_response)

//     Ok(true)
// }

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
    AuthenticationMethodSwitch,
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
                        buffer = send_handshake_packet(&scramble, &mut packet_codec)
                            .await
                            .unwrap();

                        // 3 发送挑战包给客户端
                        status = ServerPhaseDesc::ServerResponse;

                        // 4. 收到客户端握手包并响应给客户端
                        next_status = ServerPhaseDesc::ClientHandshakeResponse;
                    }
                    ServerPhaseDesc::ClientResponse => {
                        // 4. 接收客户端响应
                        let mut buf = [0; 4096];
                        let n = match socket.read(&mut buf).await {
                            Ok(0) => return,
                            Ok(n) => n,
                            Err(e) => {
                                eprintln!("failed to read from socket; err = {:?}", e);
                                return;
                            }
                        };

                        let mut src = BytesMut::new();
                        src.extend_from_slice(&buf[0..n].to_vec());
                        packet_codec.decode(&mut src, &mut buffer).unwrap();

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
