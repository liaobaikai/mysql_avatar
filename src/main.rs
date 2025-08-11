use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use mysql_async::consts::{CapabilityFlags, StatusFlags};
use mysql_common::{packets::HandshakePacket, proto::{MySerialize}};
use sha1::{Digest, Sha1};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

// 调整能力集常量，增加常见的认证相关能力
const SERVER_CAPABILITIES: u32 = 0x800001ff | 0x00080000; // 增加 CLIENT_PLUGIN_AUTH 能力
const SCRAMBLE_BUFFER_SIZE: usize = 20;
const PROTOCOL_VERSION: u8 = 10;
const SERVER_LANGUAGE: u8 = 0x21; // utf8mb3_general_ci
const SERVER_STATUS: u16 = 2;

// 模拟的用户数据库
#[derive(Clone, Debug)]
struct UserDB {
    users: HashMap<String, String>, // 用户名 -> 密码哈希
}

impl UserDB {
    fn new() -> Self {
        let mut users = HashMap::new();
        // 添加测试用户: slave_user 密码: slave_pass
        // 密码哈希是 "*7B17A20F66666D69D7B2044F5C564D7A7B5C5A5E5D" (模拟值)
        users.insert(
            "root".to_string(),
            "*7B17A20F66666D69D7B2044F5C564D7A7B5C5A5E5D".to_string(),
        );
        UserDB { users }
    }

    fn get_password_hash(&self, username: &str) -> Option<&String> {
        self.users.get(username)
    }
}

// 构建握手包
fn new_handshake_packet(scramble: &[u8; 20]) -> Vec<u8> {
    let mut scramble_1 = [0u8; 8];
    let (_scramble_1, scramble_2) = scramble.split_at(8);
    scramble_1.copy_from_slice(&_scramble_1);

    let packet = HandshakePacket::new(
        0xa,
        "8.0.39".as_bytes(),
        0xb,
        scramble_1,
        Some(scramble_2),
        CapabilityFlags::all(),
        0x21,
        StatusFlags::all(),
        Some(b"mysql_native_password"),
    );

    let mut buf: Vec<u8> = Vec::new();
    packet.serialize(&mut buf);

    println!("{:?}", packet);
    println!("{:?}", buf);
    buf
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
// 发送握手初始化包
async fn send_handshake_packet(
    stream: &mut TcpStream,
    scramble: &[u8; 20],
) -> Result<(), Box<dyn std::error::Error>> {
    
    // 发送数据包 (长度前缀 + 序列号 + 数据)
    send_packet(stream, 0, &new_handshake_packet(scramble)).await?;
    Ok(())
}

struct PlainPacketCodec<'a> {
    /// Chunk sequence id.
    pub seq_id: u8,
    pub data: &'a [u8]
}

// 发送MySQL格式的数据包
async fn send_packet(
    stream: &mut TcpStream,
    sequence: u8,
    data: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let len = data.len();


    // 长度前缀 (3字节)
    stream
        .write_all(&[
            (len & 0xff) as u8,
            ((len >> 8) & 0xff) as u8,
            ((len >> 16) & 0xff) as u8,
        ])
        .await?;

    // 序列号 (1字节)
    stream.write_all(&[sequence]).await?;

    // 数据包内容
    stream.write_all(data).await?;
    stream.flush().await?;

    Ok(())
}

// 接收MySQL格式的数据包
async fn receive_packet(stream: &mut TcpStream) -> Result<(u8, Bytes), Box<dyn std::error::Error>> {
    // 读取长度前缀 (3字节)
    let mut len_buf = [0u8; 3];
    stream.read_exact(&mut len_buf).await?;
    let len = (len_buf[0] as usize) | ((len_buf[1] as usize) << 8) | ((len_buf[2] as usize) << 16);

    // 读取序列号 (1字节)
    let mut seq_buf = [0u8; 1];
    stream.read_exact(&mut seq_buf).await?;
    let sequence = seq_buf[0];

    // 读取数据包内容
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;

    Ok((sequence, Bytes::from(data)))
}

// 接收并解析客户端握手响应
async fn receive_handshake_response(
    stream: &mut TcpStream,
) -> Result<HandshakeResponse, Box<dyn std::error::Error>> {
    let (_, data) = receive_packet(stream).await?;
    println!("data: {:?}", data);
    let data_len = data.len();
    let mut cursor = 0;

    // 1. 解析能力标志（4字节）
    if cursor + 4 > data_len {
        return Err("数据包太短，无法解析能力标志".into());
    }
    let capabilities = u32::from_le_bytes(data[cursor..cursor + 4].try_into()?);
    cursor += 4;

    // 2. 最大包大小（4字节）
    if cursor + 4 > data_len {
        return Err("数据包太短，无法解析最大包大小".into());
    }
    cursor += 4; // 跳过最大包大小

    // 3. 字符集（1字节）
    if cursor + 1 > data_len {
        return Err("数据包太短，无法解析字符集".into());
    }
    cursor += 1; // 跳过后字符集

    // 4. 保留字节（关键修复：不硬编码23字节，而是根据实际剩余数据处理）
    let reserved_bytes = std::cmp::min(23, data_len - cursor); // 最多取23字节，避免越界
    cursor += reserved_bytes;

    // 5. 解析用户名（NULL终止）
    if cursor >= data_len {
        return Err("数据包太短，无法解析用户名".into());
    }
    let username_end = data[cursor..]
        .iter()
        .position(|&x| x == 0x00)
        .ok_or("用户名未以NULL终止")?;
    let username = String::from_utf8(data[cursor..cursor + username_end].to_vec())?;
    cursor += username_end + 1; // 移动到用户名后的NULL之后

    // 6. 解析认证响应
    let auth_response = if (capabilities & 0x800000) != 0 {
        // 现代客户端：带1字节长度前缀
        if cursor >= data_len {
            Vec::new() // 没有认证响应
        } else {
            let auth_len = data[cursor] as usize;
            cursor += 1;
            // 确保不越界
            let actual_len = std::cmp::min(auth_len, data_len - cursor);
            data[cursor..cursor + actual_len].to_vec()
        }
    } else {
        // 旧版客户端：NULL终止
        if cursor >= data_len {
            Vec::new()
        } else {
            let auth_end = data[cursor..]
                .iter()
                .position(|&x| x == 0x00)
                .unwrap_or(data_len - cursor);
            let auth = data[cursor..cursor + auth_end].to_vec();
            cursor += auth_end + 1;
            auth
        }
    };

    Ok(HandshakeResponse {
        capabilities,
        username,
        auth_response,
    })
}

// 验证客户端凭据
fn verify_credentials(
    user_db: &UserDB,
    response: &HandshakeResponse,
    scramble: &[u8; 20],
) -> Result<bool, Box<dyn std::error::Error>> {
    let password_hash = match user_db.get_password_hash(&response.username) {
        Some(hash) => hash,
        None => return Ok(false),
    };

    if response.auth_response.is_empty() {
        return Ok(false);
    }

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

    Ok(true)
}

// 发送OK包
async fn send_ok_packet(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut packet = BytesMut::new();

    // OK包标志
    packet.put_u8(0x00);

    // 受影响的行数 (0)
    packet.extend_from_slice(&[0x00]);

    // 最后插入的ID (0)
    packet.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

    // 服务器状态
    packet.extend_from_slice(&SERVER_STATUS.to_le_bytes());

    // 警告数 (0)
    packet.extend_from_slice(&[0x00, 0x00]);

    send_packet(stream, 1, &packet).await?;
    Ok(())
}

// 发送错误包
async fn send_error_packet(
    stream: &mut TcpStream,
    message: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut packet = BytesMut::new();

    // 错误包标志
    packet.put_u8(0xff);

    // 错误代码 (1045 = 访问被拒绝)
    packet.extend_from_slice(&0x0415u16.to_le_bytes());

    // SQL状态标志
    packet.put_u8(0x23); // '#'
    packet.extend_from_slice(b"28000"); // 访问被拒绝的SQL状态码

    // 错误消息
    packet.extend_from_slice(message.as_bytes());

    send_packet(stream, 1, &packet).await?;
    Ok(())
}

// 握手响应数据结构
#[derive(Debug)]
struct HandshakeResponse {
    capabilities: u32,
    username: String,
    auth_response: Vec<u8>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let user_db = UserDB::new();
    loop {
        let (mut socket, _) = listener.accept().await?;
        let cloned_user_db = user_db.clone();
        tokio::spawn(async move {
            let mut buf = [0; 1024];

            // In a loop, read data from the socket and write the data back.
            loop {
                // let n = match socket.read(&mut buf).await {
                //     // socket closed
                //     Ok(0) => return,
                //     Ok(n) => n,
                //     Err(e) => {
                //         eprintln!("failed to read from socket; err = {:?}", e);
                //         return;
                //     }
                // };

                // println!("data: {:?}", buf);

                // 1. 生成随机挑战值
                let mut scramble = [0u8; SCRAMBLE_BUFFER_SIZE];
                rand::Rng::fill(&mut rand::thread_rng(), &mut scramble);

                // 2. 发送初始握手包
                send_handshake_packet(&mut socket, &scramble).await.unwrap();

                // 3. 接收客户端响应
                let response = receive_handshake_response(&mut socket).await.unwrap();

                println!("{:?}", response);

                // 4. 验证客户端身份
                if verify_credentials(&cloned_user_db, &response, &scramble).unwrap() {
                    println!("用户认证成功: {}", response.username);
                    // 发送认证成功包
                    send_ok_packet(&mut socket).await.unwrap();
                    println!("已发送认证成功响应，等待从库同步请求");
                } else {
                    println!("用户认证失败: {}", response.username);
                    // 发送认证失败包
                    send_error_packet(&mut socket, "Access denied for user")
                        .await
                        .unwrap();
                }

                // Write the data back
                // if let Err(e) = socket.write_all(&buf[0..n]).await {
                //     eprintln!("failed to write to socket; err = {:?}", e);
                //     return;
                // }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use mysql_async::{consts::{CapabilityFlags, StatusFlags}, OkPacket};
    use mysql_common::{packets::HandshakePacket, proto::MySerialize};

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

        let mut buf: Vec<u8> = Vec::new();
        packet.serialize(&mut buf);

        println!("{:?}", packet);
        println!("{:?}", buf);


        

    }


    
}
