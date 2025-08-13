use std::{borrow::Cow, collections::HashMap, io};

use bytes::BufMut;
use mysql_async::consts::{CapabilityFlags, MariadbCapabilities, StatusFlags};
use mysql_common::{
    collations::CollationId,
    io::ParseBuf,
    misc::raw::{
        Const, Either, RawBytes, RawConst, RawInt, Skip,
        bytes::{EofBytes, NullBytes, U8Bytes},
        int::{LeU16, LeU32, LenEnc},
    },
    packets::{AuthPlugin, ErrPacketHeader, ProgressReport, SqlState},
    proto::{MyDeserialize, MySerialize},
};

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
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OkPacket<'a> {
    affected_rows: u64,
    last_insert_id: Option<u64>,
    status_flags: StatusFlags,
    warnings: u16,
    info: Option<RawBytes<'a, LenEnc>>,
    session_state_info: Option<RawBytes<'a, LenEnc>>,
}

impl OkPacket<'_> {
    pub fn new(
        affected_rows: u64,
        last_insert_id: Option<u64>,
        status_flags: StatusFlags,
        warnings: u16,
    ) -> Self {
        OkPacket {
            affected_rows,
            last_insert_id,
            status_flags,
            warnings,
            info: None,
            session_state_info: None,
        }
    }

    pub fn into_owned(self) -> OkPacket<'static> {
        OkPacket {
            affected_rows: self.affected_rows,
            last_insert_id: self.last_insert_id,
            status_flags: self.status_flags,
            warnings: self.warnings,
            info: self.info.map(|x| x.into_owned()),
            session_state_info: self.session_state_info.map(|x| x.into_owned()),
        }
    }

    /// Value of the affected_rows field of an Ok packet.
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    /// Value of the last_insert_id field of an Ok packet.
    pub fn last_insert_id(&self) -> Option<u64> {
        self.last_insert_id
    }

    /// Value of the status_flags field of an Ok packet.
    pub fn status_flags(&self) -> StatusFlags {
        self.status_flags
    }

    /// Value of the warnings field of an Ok packet.
    pub fn warnings(&self) -> u16 {
        self.warnings
    }
}

/// Represents MySql's Ok packet.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OkPacketBody<'a> {
    affected_rows: RawInt<LenEnc>,
    last_insert_id: RawInt<LenEnc>,
    status_flags: Const<StatusFlags, LeU16>,
    warnings: RawInt<LeU16>,
    info: RawBytes<'a, LenEnc>,
    session_state_info: RawBytes<'a, LenEnc>,

    capabilities: CapabilityFlags,
}

impl<'a> OkPacketBody<'a> {
    const HEADER: u8 = 0xFE;
    pub fn new(
        affected_rows: u64,
        last_insert_id: u64,
        status_flags: StatusFlags,
        warnings: u16,
        info: impl Into<Cow<'a, [u8]>>,
        session_state_info: impl Into<Cow<'a, [u8]>>,
        capabilities: CapabilityFlags,
    ) -> Self {
        OkPacketBody {
            affected_rows: RawInt::new(affected_rows),
            last_insert_id: RawInt::new(last_insert_id),
            status_flags: Const::new(status_flags),
            warnings: RawInt::new(warnings),
            info: RawBytes::new(info.into()),
            session_state_info: RawBytes::new(session_state_info.into()),

            capabilities,
        }
    }
}

// sql/protocol_classics.cc `net_send_ok`
impl<'a> MySerialize for OkPacketBody<'a> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_u8(OkPacketBody::HEADER);
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

/// MySql error packet.
///
/// May hold an error or a progress report.
#[derive(Debug, Clone, PartialEq)]
pub enum ErrPacket<'a> {
    Error(ServerError<'a>),
    Progress(ProgressReport<'a>),
}

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
        buf.put_u8(ServerError::HEADER);

        self.code.serialize(buf);

        if self
            .capabilities
            .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
        {
            if let Some(state) = &self.state {
                state.serialize(buf);
            }
        }

        self.message.serialize(buf);
    }
}
