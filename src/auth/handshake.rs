use bytes::BytesMut;
use mysql_async::consts::{CapabilityFlags, StatusFlags};
use mysql_common::{packets::AuthPlugin, proto::MySerialize};

use crate::{
    auth::Scramble,
    consts::Charset,
    packets::HandshakePacket,
    variable::{
        VARIABLE_DEFAULT_AUTHENTICATION_PLUGIN, VARIABLE_PROTOCOL_VERSION, VARIABLE_VERSION,
        get_global_var,
    },
};

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

pub fn server_status_flags() -> StatusFlags {
    StatusFlags::SERVER_STATUS_AUTOCOMMIT
}

pub struct Handshake {
    // 连接ID
    conn_id: u32,
    // 挑战值
    scramble: Scramble,
}

impl Handshake {
    pub fn new() -> Self {
        Self {
            conn_id: 0,
            scramble: Scramble::new(),
        }
    }

    pub fn scramble(&self) -> &[u8] {
        &self.scramble.data
    }

    pub fn init_plain_packet(&mut self, conn_id: u32) -> BytesMut {
        self.conn_id = conn_id;

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
}
