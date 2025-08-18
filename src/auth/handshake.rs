use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use lazy_static::lazy_static;
use mysql_async::consts::{CapabilityFlags};
use mysql_common::{
    io::ParseBuf,
    packets::{AuthPlugin, SqlState},
    proto::{MyDeserialize, MySerialize},
};
use parking_lot::{Condvar, Mutex};

use crate::{
    auth::Scramble, consts::Charset, mysqld::server_status_flags, packets::{ErrPacket, HandshakePacket, HandshakeResponse, OkPacket, ServerError}, variable::{
        get_global_var, VARIABLE_DEFAULT_AUTHENTICATION_PLUGIN, VARIABLE_PROTOCOL_VERSION, VARIABLE_VERSION
    }
};

lazy_static! {
    pub static ref USER_CONN_ID: Arc<(
        parking_lot::lock_api::Mutex<parking_lot::RawMutex, u32>,
        Condvar
    )> = Arc::new((Mutex::new(10), Condvar::new()));
}

pub fn server_capabilities() -> CapabilityFlags {
    CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_PLUGIN_AUTH
        | CapabilityFlags::CLIENT_SECURE_CONNECTION
        | CapabilityFlags::CLIENT_CONNECT_WITH_DB
        | CapabilityFlags::CLIENT_TRANSACTIONS
        | CapabilityFlags::CLIENT_CONNECT_ATTRS
        // | CapabilityFlags::CLIENT_QUERY_ATTRIBUTES
        | CapabilityFlags::CLIENT_DEPRECATE_EOF
}


pub struct Handshake {
    // 连接ID
    conn_id: u32,
    // 挑战值
    scramble: Scramble,
    // 客户端选项
    client_capabilities: CapabilityFlags,
}


impl Handshake {
    pub fn new() -> Self {
        let mut conn_id = USER_CONN_ID.0.lock();
        *conn_id += 1;
        Self {
            conn_id: *conn_id,
            scramble: Scramble::new(),
            client_capabilities: CapabilityFlags::empty(),
        }
    }

    pub fn scramble(&self) -> &[u8] {
        &self.scramble.data
    }

    #[allow(unused)]
    pub fn client_capabilities(&self) -> &CapabilityFlags {
        &self.client_capabilities
    }

    pub fn init_plain_packet(&mut self) -> BytesMut {
        let plugin_var = get_global_var(VARIABLE_DEFAULT_AUTHENTICATION_PLUGIN).unwrap();
        let auth_plugin_data = plugin_var.value().as_bytes();
        let auth_plugin = AuthPlugin::from_bytes(auth_plugin_data);

        let packet = HandshakePacket::new(
            VARIABLE_PROTOCOL_VERSION,
            VARIABLE_VERSION.as_bytes(),
            self.conn_id,
            self.scramble.scramble_1,
            Some(&self.scramble.scramble_2),
            server_capabilities(),
            Charset::LATIN1_SWEDISH_CI as u8,
            server_status_flags(),
            auth_plugin.as_bytes().into(),
        );

        let mut src = BytesMut::new();
        let mut buf: Vec<u8> = Vec::new();
        packet.serialize(&mut buf);
        src.extend_from_slice(&buf);
        src
    }

    // 解析并验证客户端返回的握手包：
    // Client then returns Protocol::HandshakeResponse:
    // Option<bool> = None -> empty_pass
    // Option<bool> = false -> auth_failed
    // Option<bool> = true -> auth_pass
    pub fn auth(&mut self, input: &mut BytesMut) -> Result<(String, Option<bool>)> {
        let resp = HandshakeResponse::deserialize((), &mut ParseBuf(&input))?;
        self.client_capabilities.insert(resp.capabilities());

        let mut valid_pass = None;
        let user = String::from_utf8_lossy(resp.user()).to_string();
        if resp.scramble_buf().len() > 0 {
            if resp
                .capabilities()
                .contains(CapabilityFlags::CLIENT_PLUGIN_AUTH)
            {
                if let Some(auth_plugin) = resp.auth_plugin() {
                    if let Some(data) = auth_plugin.gen_data(Some("123456"), self.scramble()) {
                        let pass = match data {
                            mysql_common::packets::AuthPluginData::Old(p) => {
                                p == resp.scramble_buf()
                            }
                            mysql_common::packets::AuthPluginData::Native(p) => {
                                p == resp.scramble_buf()
                            }
                            mysql_common::packets::AuthPluginData::Sha2(p) => {
                                p == resp.scramble_buf()
                            }
                            mysql_common::packets::AuthPluginData::Clear(p) => {
                                p == resp.scramble_buf()
                            }
                            mysql_common::packets::AuthPluginData::Ed25519(p) => {
                                p == resp.scramble_buf()
                            }
                        };
                        valid_pass = Some(pass)
                    }
                }
            }
        }
        Ok((user, valid_pass))
    }

    // 验证后并返回响应包
    pub fn auth_with_response(&mut self, input: &mut BytesMut) -> Result<(bool, BytesMut)> {
        let mut data = vec![];
        let (user, valid_pass) = self.auth(input)?;
        match valid_pass {
            Some(false) | None => {
                ErrPacket::Error(ServerError::new(
                    1045,
                    Some(SqlState::new(*b"28000")),
                    format!(
                        "Access denied for user '{}'@'localhost' (using password: {})",
                        user,
                        if valid_pass.is_none() { "NO" } else { "YES" }
                    )
                    .as_bytes(),
                    self.client_capabilities,
                ))
                .serialize(&mut data);
            }
            Some(true) => {
                OkPacket::new(
                    0,
                    0,
                    server_status_flags(),
                    0,
                    String::new().as_bytes(),
                    String::new().as_bytes(),
                    self.client_capabilities,
                    false,
                )
                .serialize(&mut data);
            }
        }
        let mut buffer = BytesMut::with_capacity(data.len());
        buffer.extend_from_slice(&data);
        Ok((valid_pass == Some(true), buffer))
    }
}
