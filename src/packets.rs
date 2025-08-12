use std::borrow::Cow;

use bytes::BufMut;
use mysql_async::consts::{CapabilityFlags, StatusFlags};
use mysql_common::{
    misc::raw::{
        Const, RawBytes, RawInt,
        bytes::{EofBytes, U8Bytes},
        int::{LeU16, LenEnc},
    },
    packets::{ErrPacketHeader, ProgressReport, SqlState},
    proto::MySerialize,
};

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
