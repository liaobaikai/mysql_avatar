use anyhow::{Result, anyhow};
use bytes::BytesMut;
use mysql_async::consts::CapabilityFlags;
use mysql_common::{
    io::ParseBuf,
    misc::raw::{RawBytes, RawInt, bytes::EofBytes},
};
use sqlparser::ast::{Function, Select, Set, ShowStatementFilter};

use crate::{consts::Command, variable::Variable};

pub mod binlog;
pub mod query;

pub trait Queryable {
    fn query(&mut self, sql: &str) -> Result<Vec<BytesMut>>;
    fn select_func(&mut self, f: Function) -> Result<KvPair>;
    fn select(&mut self, s: Select) -> Result<Vec<BytesMut>>;
    fn set(&mut self, s: Set) -> Result<Vec<BytesMut>>;
    fn show(
        &mut self,
        filter: Option<ShowStatementFilter>,
        global: bool,
        session: bool,
    ) -> Result<Vec<BytesMut>>;
    fn as_response_packet(
        &mut self,
        column_definitions: Vec<BytesMut>,
        rows: Vec<Vec<BytesMut>>,
    ) -> Vec<BytesMut>;
}

#[allow(unused)]
trait Dumpable {}

#[derive(Debug, Default)]
pub struct KvPair {
    key: String,
    val: String,
}

impl KvPair {
    fn new(key: String, val: String) -> Self {
        Self { key, val }
    }
}

pub struct SqlCommand {
    //
    client_capabilities: CapabilityFlags,
    // 会话级变量
    session_vars: Vec<Variable>,
}

impl SqlCommand {
    pub fn new(session_vars: Vec<Variable>) -> Self {
        Self {
            client_capabilities: CapabilityFlags::empty(),
            session_vars,
        }
    }

    #[allow(unused)]
    pub fn set_client_capabilities(&mut self, client_capabilities: CapabilityFlags) {
        self.client_capabilities.insert(client_capabilities);
    }

    pub fn read(&mut self, input: &mut BytesMut) -> Result<Vec<BytesMut>> {
        let mut buf = ParseBuf(&input);
        let com_val: RawInt<u8> = buf.parse(())?;
        let command: Command = Command::try_from(*com_val)?;

        match command {
            Command::COM_QUERY => {
                let query: RawBytes<EofBytes> = buf.parse(())?;
                let packets = self.query(&query.as_str())?;
                return Ok(packets);
            }
            Command::COM_QUIT => {
                return Err(anyhow!("Connection closed"));
            }
            Command::COM_REGISTER_SLAVE => {}
            Command::COM_BINLOG_DUMP | Command::COM_BINLOG_DUMP_GTID => {
                return Ok(vec![]);
            }
            _ => {}
        }

        Ok(vec![])
    }
}
