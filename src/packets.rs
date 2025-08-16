use std::{borrow::Cow, collections::HashMap, io};

use bytes::{BufMut};
use mysql_async::consts::{CapabilityFlags, MariadbCapabilities, StatusFlags};
use mysql_common::{
    collations::CollationId,
    io::ParseBuf,
    misc::raw::{
        Const, Either, RawBytes, RawConst, RawInt, Skip,
        bytes::{BareBytes, EofBytes, NullBytes, U8Bytes},
        int::{LeU16, LeU32, LeU32LowerHalf, LeU32UpperHalf, LenEnc},
    },
    packets::{AuthPlugin, ErrPacketHeader, ProgressReport, SqlState},
    proto::{MyDeserialize, MySerialize},
};

/// Represents MySql's initial handshake packet.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HandshakePacket<'a> {
    protocol_version: RawInt<u8>,
    server_version: RawBytes<'a, NullBytes>,
    connection_id: RawInt<LeU32>,
    scramble_1: [u8; 8],
    __filler: Skip<1>,
    // lower 16 bytes
    capabilities_1: Const<CapabilityFlags, LeU32LowerHalf>,
    default_collation: RawInt<u8>,
    status_flags: Const<StatusFlags, LeU16>,
    // upper 16 bytes
    capabilities_2: Const<CapabilityFlags, LeU32UpperHalf>,
    auth_plugin_data_len: RawInt<u8>,
    __reserved: Skip<6>,
    // MariaDB uses last 4 reserved bytes to pass its extended capabilities.
    mariadb_ext_capabilities: Const<MariadbCapabilities, LeU32>,
    scramble_2: Option<RawBytes<'a, BareBytes<{ (u8::MAX as usize) - 8 }>>>,
    auth_plugin_name: Option<RawBytes<'a, NullBytes>>,
}

impl<'de> MyDeserialize<'de> for HandshakePacket<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let protocol_version = buf.parse(())?;
        let server_version = buf.parse(())?;

        // includes trailing 10 bytes filler
        let mut sbuf: ParseBuf<'_> = buf.parse(31)?;
        let connection_id = sbuf.parse_unchecked(())?;
        let scramble_1 = sbuf.parse_unchecked(())?;
        let __filler = sbuf.parse_unchecked(())?;
        let capabilities_1: RawConst<LeU32LowerHalf, CapabilityFlags> = sbuf.parse_unchecked(())?;
        let default_collation = sbuf.parse_unchecked(())?;
        let status_flags = sbuf.parse_unchecked(())?;
        let capabilities_2: RawConst<LeU32UpperHalf, CapabilityFlags> = sbuf.parse_unchecked(())?;
        let auth_plugin_data_len: RawInt<u8> = sbuf.parse_unchecked(())?;
        let __reserved = sbuf.parse_unchecked(())?;
        // If the server is MariaDB, it will pass its extended capabilities
        // in the last 4 reserved bytes.
        let mariadb_capabiities: RawConst<LeU32, MariadbCapabilities> = sbuf.parse_unchecked(())?;
        let mut scramble_2 = None;
        if capabilities_1.0 & CapabilityFlags::CLIENT_SECURE_CONNECTION.bits() > 0 {
            let len = core::cmp::max(13, auth_plugin_data_len.0 as i8 - 8) as usize;
            scramble_2 = buf.parse(len).map(Some)?;
        }
        let mut auth_plugin_name = None;
        if capabilities_2.0 & CapabilityFlags::CLIENT_PLUGIN_AUTH.bits() > 0 {
            auth_plugin_name = match buf.eat_all() {
                [head @ .., 0] => Some(RawBytes::new(head)),
                // missing trailing `0` is a known bug in mysql
                all => Some(RawBytes::new(all)),
            }
        }

        Ok(Self {
            protocol_version,
            server_version,
            connection_id,
            scramble_1,
            __filler,
            capabilities_1: Const::new(CapabilityFlags::from_bits_truncate(capabilities_1.0)),
            default_collation,
            status_flags,
            capabilities_2: Const::new(CapabilityFlags::from_bits_truncate(capabilities_2.0)),
            auth_plugin_data_len,
            __reserved,
            mariadb_ext_capabilities: Const::new(MariadbCapabilities::from_bits_truncate(
                mariadb_capabiities.0,
            )),
            scramble_2,
            auth_plugin_name,
        })
    }
}

impl MySerialize for HandshakePacket<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.protocol_version.serialize(&mut *buf);
        self.server_version.serialize(&mut *buf);
        self.connection_id.serialize(&mut *buf);
        self.scramble_1.serialize(&mut *buf);
        buf.put_u8(0x00);
        self.capabilities_1.serialize(&mut *buf);
        self.default_collation.serialize(&mut *buf);
        self.status_flags.serialize(&mut *buf);
        self.capabilities_2.serialize(&mut *buf);

        if self
            .capabilities_2
            .contains(CapabilityFlags::CLIENT_PLUGIN_AUTH)
        {
            buf.put_u8(
                self.scramble_2
                    .as_ref()
                    .map(|x| (core::cmp::max(13, x.len()) + 8) as u8)
                    .unwrap_or_default(),
            );
        } else {
            buf.put_u8(0);
        }

        self.__reserved.serialize(&mut *buf);
        self.mariadb_ext_capabilities.serialize(&mut *buf);

        // Assume that the packet is well formed:
        // * the CLIENT_SECURE_CONNECTION is set.
        if let Some(scramble_2) = &self.scramble_2 {
            scramble_2.serialize(&mut *buf);
            let len = scramble_2.len();
            if core::cmp::max(13, len) == 13 {
                buf.put_u8(0);
            }
        }

        // Assume that the packet is well formed:
        // * the CLIENT_PLUGIN_AUTH is set.
        if let Some(client_plugin_auth) = &self.auth_plugin_name {
            client_plugin_auth.serialize(buf);
        }
    }
}

#[allow(unused)]
impl<'a> HandshakePacket<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        protocol_version: u8,
        server_version: impl Into<Cow<'a, [u8]>>,
        connection_id: u32,
        scramble_1: [u8; 8],
        scramble_2: Option<impl Into<Cow<'a, [u8]>>>,
        capabilities: CapabilityFlags,
        default_collation: u8,
        status_flags: StatusFlags,
        auth_plugin_name: Option<impl Into<Cow<'a, [u8]>>>,
    ) -> Self {
        // Safety:
        // * capabilities are given as a valid CapabilityFlags instance
        // * the BitAnd operation can't set new bits
        let (capabilities_1, capabilities_2) = (
            CapabilityFlags::from_bits_retain(capabilities.bits() & 0x0000_FFFF),
            CapabilityFlags::from_bits_retain(capabilities.bits() & 0xFFFF_0000),
        );

        let scramble_2 = scramble_2.map(RawBytes::new);

        HandshakePacket {
            protocol_version: RawInt::new(protocol_version),
            server_version: RawBytes::new(server_version),
            connection_id: RawInt::new(connection_id),
            scramble_1,
            __filler: Skip,
            capabilities_1: Const::new(capabilities_1),
            default_collation: RawInt::new(default_collation),
            status_flags: Const::new(status_flags),
            capabilities_2: Const::new(capabilities_2),
            auth_plugin_data_len: RawInt::new(
                scramble_2
                    .as_ref()
                    .map(|x| x.len() as u8)
                    .unwrap_or_default(),
            ),
            __reserved: Skip,
            mariadb_ext_capabilities: Const::new(MariadbCapabilities::empty()),
            scramble_2,
            auth_plugin_name: auth_plugin_name.map(RawBytes::new),
        }
    }

    pub fn with_mariadb_ext_capabilities(mut self, flags: MariadbCapabilities) -> Self {
        self.mariadb_ext_capabilities = Const::new(flags);
        self
    }

    pub fn into_owned(self) -> HandshakePacket<'static> {
        HandshakePacket {
            protocol_version: self.protocol_version,
            server_version: self.server_version.into_owned(),
            connection_id: self.connection_id,
            scramble_1: self.scramble_1,
            __filler: self.__filler,
            capabilities_1: self.capabilities_1,
            default_collation: self.default_collation,
            status_flags: self.status_flags,
            capabilities_2: self.capabilities_2,
            auth_plugin_data_len: self.auth_plugin_data_len,
            __reserved: self.__reserved,
            mariadb_ext_capabilities: self.mariadb_ext_capabilities,
            scramble_2: self.scramble_2.map(|x| x.into_owned()),
            auth_plugin_name: self.auth_plugin_name.map(RawBytes::into_owned),
        }
    }

    /// Value of the protocol_version field of an initial handshake packet.
    pub fn protocol_version(&self) -> u8 {
        self.protocol_version.0
    }

    /// Value of the server_version field of an initial handshake packet as a byte slice.
    pub fn server_version_ref(&self) -> &[u8] {
        self.server_version.as_bytes()
    }

    /// Value of the server_version field of an initial handshake packet as a string
    /// (lossy converted).
    pub fn server_version_str(&self) -> Cow<'_, str> {
        self.server_version.as_str()
    }

    /// Value of the connection_id field of an initial handshake packet.
    pub fn connection_id(&self) -> u32 {
        self.connection_id.0
    }

    /// Value of the scramble_1 field of an initial handshake packet as a byte slice.
    pub fn scramble_1_ref(&self) -> &[u8] {
        self.scramble_1.as_ref()
    }

    /// Value of the scramble_2 field of an initial handshake packet as a byte slice.
    ///
    /// Note that this may include a terminating null character.
    pub fn scramble_2_ref(&self) -> Option<&[u8]> {
        self.scramble_2.as_ref().map(|x| x.as_bytes())
    }

    /// Returns concatenated auth plugin nonce.
    pub fn nonce(&self) -> Vec<u8> {
        let mut out = Vec::from(self.scramble_1_ref());
        out.extend_from_slice(self.scramble_2_ref().unwrap_or(&[][..]));

        // Trim zero terminator. Fill with zeroes if nonce
        // is somehow smaller than 20 bytes.
        out.resize(20, 0);
        out
    }

    /// Value of a server capabilities.
    pub fn capabilities(&self) -> CapabilityFlags {
        self.capabilities_1.0 | self.capabilities_2.0
    }

    /// Value of MariaDB specific server capabilities
    pub fn mariadb_ext_capabilities(&self) -> MariadbCapabilities {
        self.mariadb_ext_capabilities.0
    }

    /// Value of the default_collation field of an initial handshake packet.
    pub fn default_collation(&self) -> u8 {
        self.default_collation.0
    }

    /// Value of a status flags.
    pub fn status_flags(&self) -> StatusFlags {
        self.status_flags.0
    }

    /// Value of the auth_plugin_name field of an initial handshake packet as a byte slice.
    pub fn auth_plugin_name_ref(&self) -> Option<&[u8]> {
        self.auth_plugin_name.as_ref().map(|x| x.as_bytes())
    }

    /// Value of the auth_plugin_name field of an initial handshake packet as a string
    /// (lossy converted).
    pub fn auth_plugin_name_str(&self) -> Option<Cow<'_, str>> {
        self.auth_plugin_name.as_ref().map(|x| x.as_str())
    }

    /// Auth plugin of a handshake packet
    pub fn auth_plugin(&self) -> Option<AuthPlugin<'_>> {
        self.auth_plugin_name.as_ref().map(|x| match x.as_bytes() {
            [name @ .., 0] => ParseBuf(name).parse_unchecked(()).expect("infallible"),
            all => ParseBuf(all).parse_unchecked(()).expect("infallible"),
        })
    }
}

//
// pub type HandshakeResponse41 = HandshakeResponse;

/// Actual serialization of this field depends on capability flags values.
// type ScrambleBuf<'a> =
//     Either<RawBytes<'a, LenEnc>, Either<RawBytes<'a, U8Bytes>, RawBytes<'a, NullBytes>>>;
type ScrambleBuf<'a> =
    Either<RawBytes<'a, LenEnc>, Either<RawBytes<'a, U8Bytes>, RawBytes<'a, NullBytes>>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeResponse<'a> {
    capabilities: Const<CapabilityFlags, LeU32>,
    max_packet_size: RawInt<LeU32>,
    collation: RawInt<u8>,
    scramble_buf: ScrambleBuf<'a>,
    user: RawBytes<'a, NullBytes>,
    db_name: Option<RawBytes<'a, NullBytes>>,
    auth_plugin: Option<AuthPlugin<'a>>,
    connect_attributes: Option<HashMap<RawBytes<'a, LenEnc>, RawBytes<'a, LenEnc>>>,
    mariadb_ext_capabilities: Const<MariadbCapabilities, LeU32>,
}

#[allow(unused)]
impl<'a> HandshakeResponse<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        scramble_buf: Option<impl Into<Cow<'a, [u8]>>>,
        server_version: (u16, u16, u16),
        user: Option<impl Into<Cow<'a, [u8]>>>,
        db_name: Option<impl Into<Cow<'a, [u8]>>>,
        auth_plugin: Option<AuthPlugin<'a>>,
        mut capabilities: CapabilityFlags,
        connect_attributes: Option<HashMap<String, String>>,
        max_packet_size: u32,
    ) -> Self {
        let scramble_buf =
            if capabilities.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
                Either::Left(RawBytes::new(
                    scramble_buf.map(Into::into).unwrap_or_default(),
                ))
            } else if capabilities.contains(CapabilityFlags::CLIENT_SECURE_CONNECTION) {
                Either::Right(Either::Left(RawBytes::new(
                    scramble_buf.map(Into::into).unwrap_or_default(),
                )))
            } else {
                Either::Right(Either::Right(RawBytes::new(
                    scramble_buf.map(Into::into).unwrap_or_default(),
                )))
            };

        if db_name.is_some() {
            capabilities.insert(CapabilityFlags::CLIENT_CONNECT_WITH_DB);
        } else {
            capabilities.remove(CapabilityFlags::CLIENT_CONNECT_WITH_DB);
        }

        if auth_plugin.is_some() {
            capabilities.insert(CapabilityFlags::CLIENT_PLUGIN_AUTH);
        } else {
            capabilities.remove(CapabilityFlags::CLIENT_PLUGIN_AUTH);
        }

        if connect_attributes.is_some() {
            capabilities.insert(CapabilityFlags::CLIENT_CONNECT_ATTRS);
        } else {
            capabilities.remove(CapabilityFlags::CLIENT_CONNECT_ATTRS);
        }

        Self {
            scramble_buf,
            collation: if server_version >= (5, 5, 3) {
                RawInt::new(CollationId::UTF8MB4_GENERAL_CI as u8)
            } else {
                RawInt::new(CollationId::UTF8MB3_GENERAL_CI as u8)
            },
            user: user.map(RawBytes::new).unwrap_or_default(),
            db_name: db_name.map(RawBytes::new),
            auth_plugin,
            capabilities: Const::new(capabilities),
            connect_attributes: connect_attributes.map(|attrs| {
                attrs
                    .into_iter()
                    .map(|(k, v)| (RawBytes::new(k.into_bytes()), RawBytes::new(v.into_bytes())))
                    .collect()
            }),
            max_packet_size: RawInt::new(max_packet_size),
            mariadb_ext_capabilities: Const::new(MariadbCapabilities::empty()),
        }
    }

    pub fn with_mariadb_ext_capabilities(
        mut self,
        mariadb_ext_capabilities: MariadbCapabilities,
    ) -> Self {
        self.mariadb_ext_capabilities = Const::new(mariadb_ext_capabilities);
        self
    }

    pub fn capabilities(&self) -> CapabilityFlags {
        self.capabilities.0
    }

    pub fn mariadb_ext_capabilities(&self) -> MariadbCapabilities {
        self.mariadb_ext_capabilities.0
    }

    pub fn collation(&self) -> u8 {
        self.collation.0
    }

    pub fn scramble_buf(&self) -> &[u8] {
        match &self.scramble_buf {
            Either::Left(x) => x.as_bytes(),
            Either::Right(x) => match x {
                Either::Left(x) => x.as_bytes(),
                Either::Right(x) => x.as_bytes(),
            },
        }
    }

    pub fn user(&self) -> &[u8] {
        self.user.as_bytes()
    }

    pub fn db_name(&self) -> Option<&[u8]> {
        self.db_name.as_ref().map(|x| x.as_bytes())
    }

    pub fn auth_plugin(&self) -> Option<&AuthPlugin<'a>> {
        self.auth_plugin.as_ref()
    }

    #[must_use = "entails computation"]
    pub fn connect_attributes(&self) -> Option<HashMap<String, String>> {
        self.connect_attributes.as_ref().map(|attrs| {
            attrs
                .iter()
                .map(|(k, v)| (k.as_str().into_owned(), v.as_str().into_owned()))
                .collect()
        })
    }
}

impl<'de> MyDeserialize<'de> for HandshakeResponse<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut sbuf: ParseBuf<'_> = buf.parse(4 + 4 + 1 + 23)?;
        let client_flags: RawConst<LeU32, CapabilityFlags> = sbuf.parse_unchecked(())?;
        let max_packet_size: RawInt<LeU32> = sbuf.parse_unchecked(())?;
        let collation = sbuf.parse_unchecked(())?;
        sbuf.parse_unchecked::<Skip<19>>(())?;
        let mariadb_flags: RawConst<LeU32, MariadbCapabilities> = sbuf.parse_unchecked(())?;
        let user = buf.parse(())?;

        let like_db_name = buf.0.first() == Some(&0);
        let scramble_buf =
            if client_flags.0 & CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.bits() > 0 {
                Either::Left(buf.parse(())?)
            } else if client_flags.0 & CapabilityFlags::CLIENT_SECURE_CONNECTION.bits() > 0 {
                Either::Right(Either::Left(buf.parse(())?))
            } else {
                Either::Right(Either::Right(buf.parse(())?))
            };

        let mut db_name = None;
        if !like_db_name {
            if client_flags.0 & CapabilityFlags::CLIENT_CONNECT_WITH_DB.bits() > 0 {
                db_name = buf.parse(()).map(Some)?;
            }
        }

        let mut auth_plugin = None;
        if client_flags.0 & CapabilityFlags::CLIENT_PLUGIN_AUTH.bits() > 0 && !buf.is_empty() {
            let auth_plugin_name = buf.eat_null_str();
            auth_plugin = Some(AuthPlugin::from_bytes(auth_plugin_name));
        }

        let mut connect_attributes = None;
        if client_flags.0 & CapabilityFlags::CLIENT_CONNECT_ATTRS.bits() > 0 && !buf.is_empty() {
            connect_attributes = Some(deserialize_connect_attrs(&mut *buf)?);
        }

        Ok(Self {
            capabilities: Const::new(CapabilityFlags::from_bits_truncate(client_flags.0)),
            max_packet_size,
            collation,
            scramble_buf,
            user,
            db_name,
            auth_plugin,
            connect_attributes,
            mariadb_ext_capabilities: Const::new(MariadbCapabilities::from_bits_truncate(
                mariadb_flags.0,
            )),
        })
    }
}

// Helper that deserializes connect attributes.
fn deserialize_connect_attrs<'de>(
    buf: &mut ParseBuf<'de>,
) -> io::Result<HashMap<RawBytes<'de, LenEnc>, RawBytes<'de, LenEnc>>> {
    let data_len = buf.parse::<RawInt<LenEnc>>(())?;
    let mut data: ParseBuf<'_> = buf.parse(data_len.0 as usize)?;
    let mut attrs = HashMap::new();
    while !data.is_empty() {
        let key = data.parse::<RawBytes<'_, LenEnc>>(())?;
        let value = data.parse::<RawBytes<'_, LenEnc>>(())?;
        attrs.insert(key, value);
    }
    Ok(attrs)
}

/// Represents MySql's Ok packet.
// #[derive(Debug, Clone, Eq, PartialEq)]
// pub struct OkPacket<'a> {
//     affected_rows: u64,
//     last_insert_id: Option<u64>,
//     status_flags: StatusFlags,
//     warnings: u16,
//     info: Option<RawBytes<'a, LenEnc>>,
//     session_state_info: Option<RawBytes<'a, LenEnc>>,
// }

// #[allow(unused)]
// impl OkPacket<'_> {
//     pub fn new(
//         affected_rows: u64,
//         last_insert_id: Option<u64>,
//         status_flags: StatusFlags,
//         warnings: u16,
//     ) -> Self {
//         OkPacket {
//             affected_rows,
//             last_insert_id,
//             status_flags,
//             warnings,
//             info: None,
//             session_state_info: None,
//         }
//     }

//     pub fn into_owned(self) -> OkPacket<'static> {
//         OkPacket {
//             affected_rows: self.affected_rows,
//             last_insert_id: self.last_insert_id,
//             status_flags: self.status_flags,
//             warnings: self.warnings,
//             info: self.info.map(|x| x.into_owned()),
//             session_state_info: self.session_state_info.map(|x| x.into_owned()),
//         }
//     }

//     /// Value of the affected_rows field of an Ok packet.
//     pub fn affected_rows(&self) -> u64 {
//         self.affected_rows
//     }

//     /// Value of the last_insert_id field of an Ok packet.
//     pub fn last_insert_id(&self) -> Option<u64> {
//         self.last_insert_id
//     }

//     /// Value of the status_flags field of an Ok packet.
//     pub fn status_flags(&self) -> StatusFlags {
//         self.status_flags
//     }

//     /// Value of the warnings field of an Ok packet.
//     pub fn warnings(&self) -> u16 {
//         self.warnings
//     }
// }

// impl<'a> TryFrom<OkPacketBody<'a>> for OkPacket<'a> {
//     type Error = io::Error;

//     fn try_from(body: OkPacketBody<'a>) -> io::Result<Self> {
//         Ok(OkPacket {
//             affected_rows: *body.affected_rows,
//             last_insert_id: if *body.last_insert_id == 0 {
//                 None
//             } else {
//                 Some(*body.last_insert_id)
//             },
//             status_flags: *body.status_flags,
//             warnings: *body.warnings,
//             info: if !body.info.is_empty() {
//                 Some(body.info)
//             } else {
//                 None
//             },
//             session_state_info: if !body.session_state_info.is_empty() {
//                 Some(body.session_state_info)
//             } else {
//                 None
//             },
//         })
//     }
// }

/// Represents MySql's EOF packet.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EofPacket {
    status_flags: Const<StatusFlags, LeU16>,
    warnings: RawInt<LeU16>,

    capabilities: CapabilityFlags,
}

#[allow(unused)]
impl EofPacket {
    const HEADER: u8 = 0xfe;
    pub fn new(status_flags: StatusFlags, warnings: u16, capabilities: CapabilityFlags) -> Self {
        Self {
            status_flags: Const::new(status_flags),
            warnings: RawInt::new(warnings),
            capabilities,
        }
    }
}

impl<'a> MySerialize for EofPacket {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_u8(EofPacket::HEADER);

        if self
            .capabilities
            .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
        {
            self.warnings.serialize(buf);
            self.status_flags.serialize(buf);
        }
    }
}

/// Represents MySql's Ok packet.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OkPacket<'a> {
    affected_rows: RawInt<LenEnc>,
    last_insert_id: RawInt<LenEnc>,
    status_flags: Const<StatusFlags, LeU16>,
    warnings: RawInt<LeU16>,
    info: RawBytes<'a, LenEnc>,
    session_state_info: RawBytes<'a, LenEnc>,

    capabilities: CapabilityFlags,
    is_eof: bool,
}

// OK: header = 0 and length of packet >= 7
// EOF: header = 0xfe and length of packet < 8
#[allow(unused)]
impl<'a> OkPacket<'a> {
    const HEADER: u8 = 0x00;
    pub fn new(
        affected_rows: u64,
        last_insert_id: u64,
        status_flags: StatusFlags,
        warnings: u16,
        info: impl Into<Cow<'a, [u8]>>,
        session_state_info: impl Into<Cow<'a, [u8]>>,
        capabilities: CapabilityFlags,
        is_eof: bool,
    ) -> Self {
        Self {
            affected_rows: RawInt::new(affected_rows),
            last_insert_id: RawInt::new(last_insert_id),
            status_flags: Const::new(status_flags),
            warnings: RawInt::new(warnings),
            info: RawBytes::new(info.into()),
            session_state_info: RawBytes::new(session_state_info.into()),

            capabilities,
            is_eof,
        }
    }
}

// sql/protocol_classics.cc `net_send_ok`
impl<'a> MySerialize for OkPacket<'a> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        if self.is_eof {
            buf.put_u8(0xFE);
        } else {
            buf.put_u8(0x00);
        }

        self.affected_rows.serialize(buf);
        self.last_insert_id.serialize(buf);

        if self
            .capabilities
            .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
        {
            self.status_flags.serialize(buf);
            self.warnings.serialize(buf);
        } else if self
            .capabilities
            .contains(CapabilityFlags::CLIENT_TRANSACTIONS)
        {
            self.status_flags.serialize(buf);
        }

        if self
            .capabilities
            .contains(CapabilityFlags::CLIENT_SESSION_TRACK)
        {
            if self
                .status_flags
                .contains(StatusFlags::SERVER_SESSION_STATE_CHANGED)
            {
                self.info.serialize(buf);
            } else {
                self.session_state_info.serialize(buf);
            }
        } else {
            self.info.serialize(buf);
        }
    }
}

// /// OK send instead of EOF still require 0xFE header, but OK packet content.
// /// Represents MySql's Ok packet.
// #[derive(Debug, Clone, Eq, PartialEq)]
// pub struct EofPacket<'a> {
//     affected_rows: RawInt<LenEnc>,
//     last_insert_id: RawInt<LenEnc>,
//     status_flags: Const<StatusFlags, LeU16>,
//     warnings: RawInt<LeU16>,
//     info: RawBytes<'a, LenEnc>,
//     session_state_info: RawBytes<'a, LenEnc>,

//     capabilities: CapabilityFlags,
// }

// // OK: header = 0 and length of packet >= 7
// // EOF: header = 0xfe and length of packet < 8
// #[allow(unused)]
// impl<'a> EofPacket<'a> {
//     const HEADER: u8 = 0xFE;
//     pub fn new(
//         affected_rows: u64,
//         last_insert_id: u64,
//         status_flags: StatusFlags,
//         warnings: u16,
//         info: impl Into<Cow<'a, [u8]>>,
//         session_state_info: impl Into<Cow<'a, [u8]>>,
//         capabilities: CapabilityFlags,
//     ) -> Self {
//         Self {
//             affected_rows: RawInt::new(affected_rows),
//             last_insert_id: RawInt::new(last_insert_id),
//             status_flags: Const::new(status_flags),
//             warnings: RawInt::new(warnings),
//             info: RawBytes::new(info.into()),
//             session_state_info: RawBytes::new(session_state_info.into()),

//             capabilities,
//         }
//     }
// }

// sql/protocol_classics.cc `net_send_ok`
// impl<'a> MySerialize for OkPacket2<'a> {
//     fn serialize(&self, buf: &mut Vec<u8>) {
//         buf.put_u8(OkPacket2::HEADER);
//         self.affected_rows.serialize(buf);
//         self.last_insert_id.serialize(buf);

//         if self
//             .capabilities
//             .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
//         {
//             self.status_flags.serialize(buf);
//             self.warnings.serialize(buf);
//         } else if self
//             .capabilities
//             .contains(CapabilityFlags::CLIENT_TRANSACTIONS)
//         {
//             self.status_flags.serialize(buf);
//         }

//         if self
//             .capabilities
//             .contains(CapabilityFlags::CLIENT_SESSION_TRACK)
//         {
//             if self
//                 .status_flags
//                 .contains(StatusFlags::SERVER_SESSION_STATE_CHANGED)
//             {
//                 self.info.serialize(buf);
//             } else {
//                 self.session_state_info.serialize(buf);
//             }
//         } else {
//             self.info.serialize(buf);
//         }
//     }
// }

#[allow(unused)]
/// MySql error packet.
///
/// May hold an error or a progress report.
#[derive(Debug, Clone, PartialEq)]
pub enum ErrPacket<'a> {
    Error(ServerError<'a>),
    Progress(ProgressReport<'a>),
}

#[allow(unused)]
impl ErrPacket<'_> {
    /// Returns false if this error packet contains progress report.
    pub fn is_error(&self) -> bool {
        matches!(self, ErrPacket::Error { .. })
    }

    /// Returns true if this error packet contains progress report.
    pub fn is_progress_report(&self) -> bool {
        !self.is_error()
    }

    /// Will panic if ErrPacket does not contains progress report
    pub fn progress_report(&self) -> &ProgressReport<'_> {
        match *self {
            ErrPacket::Progress(ref progress_report) => progress_report,
            _ => panic!("This ErrPacket does not contains progress report"),
        }
    }

    /// Will panic if ErrPacket does not contains a `ServerError`.
    pub fn server_error(&self) -> &ServerError<'_> {
        match self {
            ErrPacket::Error(error) => error,
            ErrPacket::Progress(_) => panic!("This ErrPacket does not contain a ServerError"),
        }
    }
}

impl MySerialize for ErrPacket<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        ErrPacketHeader::new().serialize(&mut *buf);
        match self {
            ErrPacket::Error(server_error) => {
                server_error.code.serialize(&mut *buf);
                server_error.serialize(buf);
            }
            ErrPacket::Progress(progress_report) => {
                RawInt::<LeU16>::new(0xFFFF).serialize(&mut *buf);
                progress_report.serialize(buf);
            }
        }
    }
}

/// MySql error packet.
///
/// May hold an error or a progress report.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerError<'a> {
    code: RawInt<LeU16>,
    state: Option<SqlState>,
    message: RawBytes<'a, EofBytes>,

    capabilities: CapabilityFlags,
}

#[allow(unused)]
impl<'a> ServerError<'a> {
    const HEADER: u8 = 0xFF;
    pub fn new(
        code: u16,
        state: Option<SqlState>,
        msg: impl Into<Cow<'a, [u8]>>,
        capabilities: CapabilityFlags,
    ) -> Self {
        Self {
            code: RawInt::new(code),
            state,
            message: RawBytes::new(msg),
            capabilities,
        }
    }

    /// Returns an error code.
    pub fn error_code(&self) -> u16 {
        *self.code
    }

    /// Returns an sql state.
    pub fn sql_state_ref(&self) -> Option<&SqlState> {
        self.state.as_ref()
    }

    /// Returns an error message.
    pub fn message_ref(&self) -> &[u8] {
        self.message.as_bytes()
    }

    /// Returns an error message as a string (lossy converted).
    pub fn message_str(&self) -> Cow<'_, str> {
        self.message.as_str()
    }
}

impl MySerialize for ServerError<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        if let Some(state) = &self.state {
            state.serialize(buf);
        }
        self.message.serialize(buf);
    }
}

// 100, 101, 102,  --catalog
// 0, --schema
// 0, --table
// 0, --org_table
// 9, --name
// 64, 64, 108, 111, 103, 95, 98, 105, 110, 0, --org_name:  @@bin_log
// 12,   --length of fixed length fields
// 63, 0, --character_set: binary
// 1,  0, 0, 0, --column_length
// 8,  --type
// 128, 0, --flags
// 0, --decimals
// 0, 0, --reserved
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct ColumnDefinition<'a> {
    catalog: RawBytes<'a, LenEnc>,
    schema: RawBytes<'a, LenEnc>,
    table: RawBytes<'a, LenEnc>,
    org_table: RawBytes<'a, LenEnc>,
    name: RawBytes<'a, LenEnc>,
    org_name: RawBytes<'a, LenEnc>,
    // 0x0c
    __fixed: RawInt<LenEnc>,
    character_set: RawInt<LeU16>,
    column_length: RawInt<LeU32>,

    data_type: RawInt<u8>,
    flags: RawInt<LeU16>,
    decimals: RawInt<u8>,
    // reserved for future use.
    __reserved: Skip<2>,
}

impl<'a> ColumnDefinition<'a> {
    pub fn new(
        schema: impl Into<Cow<'a, [u8]>>,
        table: impl Into<Cow<'a, [u8]>>,
        org_table: impl Into<Cow<'a, [u8]>>,
        name: impl Into<Cow<'a, [u8]>>,
        org_name: impl Into<Cow<'a, [u8]>>,
        character_set: u16,
        column_length: u32,
        data_type: u8,
        flags: u16,
        decimals: u8,
    ) -> Self {
        ColumnDefinition {
            catalog: RawBytes::new("def".as_bytes()),
            schema: RawBytes::new(schema),
            table: RawBytes::new(table),
            org_table: RawBytes::new(org_table),
            name: RawBytes::new(name),
            org_name: RawBytes::new(org_name),

            __fixed: RawInt::new(0x0c),
            character_set: RawInt::new(character_set),
            column_length: RawInt::new(column_length),
            data_type: RawInt::new(data_type),
            flags: RawInt::new(flags),
            decimals: RawInt::new(decimals),

            __reserved: Skip,
        }
    }
}

impl MySerialize for ColumnDefinition<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.catalog.serialize(&mut *buf);
        self.schema.serialize(&mut *buf);
        self.table.serialize(&mut *buf);
        self.org_table.serialize(&mut *buf);
        self.name.serialize(&mut *buf);
        self.org_name.serialize(&mut *buf);
        // 0x0C
        self.__fixed.serialize(&mut *buf);
        //
        self.character_set.serialize(&mut *buf);
        self.column_length.serialize(&mut *buf);
        self.data_type.serialize(&mut *buf);
        self.flags.serialize(&mut *buf);
        self.decimals.serialize(&mut *buf);
        // 0 0
        self.__reserved.serialize(&mut *buf);
    }
}

#[allow(unused)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TextResultsetRow<'a> {
    data: RawBytes<'a, LenEnc>,
}

#[allow(unused)]
impl<'a> TextResultsetRow<'a> {
    pub fn new(data: Option<impl Into<Cow<'a, [u8]>>>) -> Self {
        Self {
            data: match data {
                Some(value) => {
                    let value = RawBytes::new(value);
                    value
                }
                None => RawBytes::new("".as_bytes()),
            },
        }
    }
}

impl MySerialize for TextResultsetRow<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.data.serialize(&mut *buf);
    }
}


#[allow(unused)]
#[cfg(test)]
mod tests {
    use mysql_async::consts::CapabilityFlags;
    use mysql_common::{packets::ComBinlogDumpGtid, proto::MySerialize};

    use crate::{
        constants::ColumnDefinitionFlags,
         get_server_status,
        packets::{ColumnDefinition, OkPacket, TextResultsetRow},
    };

    fn get_capabilities() -> CapabilityFlags {
        CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_PLUGIN_AUTH
        | CapabilityFlags::CLIENT_SECURE_CONNECTION
        | CapabilityFlags::CLIENT_CONNECT_WITH_DB
        | CapabilityFlags::CLIENT_TRANSACTIONS
        | CapabilityFlags::CLIENT_CONNECT_ATTRS
        // | CapabilityFlags::CLIENT_QUERY_ATTRIBUTES
        | CapabilityFlags::CLIENT_DEPRECATE_EOF
    }

    #[test]
    fn test_build_column_definition() {
        // Packet1
        let data = ColumnDefinition::new(
            "".as_bytes(),
            "".as_bytes(),
            "".as_bytes(),
            "@@log_bin".as_bytes(),
            "".as_bytes(),
            63, // binary
            1,  //
            8,  // binary
            ColumnDefinitionFlags::BINARY_FLAG as u16,
            0x00,
        );

        let mut buffer = Vec::new();
        data.serialize(&mut buffer);
        println!("buffer: {:?}", buffer);

        // Packet2

        let data = TextResultsetRow::new(Some("1".as_bytes()));
        // println!("data: {:?}", data);
        let mut buffer = Vec::new();
        data.serialize(&mut buffer);
        println!("buffer: {:?}", buffer);

        // Packet3
        let mut buffer = Vec::new();
        OkPacket::new(
            0,
            0,
            get_server_status(),
            0,
            String::new().as_bytes(),
            String::new().as_bytes(),
            get_capabilities(),
            false,
        )
        .serialize(&mut buffer);

        println!("buffer: {:?}", buffer);
    }
}
