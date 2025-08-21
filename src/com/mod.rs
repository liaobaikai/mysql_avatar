use anyhow::{Result};
use bytes::BytesMut;
use mysql_async::consts::CapabilityFlags;
use mysql_common::{
    io::ParseBuf,
    misc::raw::{RawBytes, RawInt, bytes::EofBytes},
    proto::MySerialize,
};
use sqlparser::ast::{Function, Select, Set, ShowStatementFilter};

use crate::{com::binlog::{parse_com_register_slave, ReplicaInfo}, consts::Command, mysqld::server_status_flags, packets::OkPacket, variable::{get_session_var, Variable}};

pub mod binlog;
pub mod query;

// 解析命令
// 返回具体的命令和sql
pub fn parse_com_query<'a>(input: &BytesMut) -> Result<(Command, String)> {
    let mut buf = ParseBuf(&input);
    let com_val: RawInt<u8> = buf.parse(())?;
    let command: Command = Command::try_from(*com_val)?;
    match command {
        Command::COM_QUERY => {
            let query: RawBytes<EofBytes> = buf.parse(())?;
            return Ok((command, query.as_str().to_string()));
        }
        _ => {}
    }
    Ok((command, String::new()))
}

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
    pub fn new(key: String, val: String) -> Self {
        Self { key, val }
    }
}

pub struct SqlCommand<'a> {
    //
    client_capabilities: CapabilityFlags,
    // 会话级变量
    session_vars: &'a mut Vec<Variable>,
    // 
}

impl<'a> SqlCommand<'a> {
    pub fn new(session_vars: &'a mut Vec<Variable>) -> Self {
        Self {
            client_capabilities: CapabilityFlags::empty(),
            session_vars,
        }
    }

    pub fn get_session_var(&self, name: &str) -> Result<Variable> {
        get_session_var(&self.session_vars, name)
    }

    pub fn set_client_capabilities(&mut self, client_capabilities: CapabilityFlags) {
        self.client_capabilities.insert(client_capabilities);
    }

    // 读取并解析命令,如果sqlparse不支持,则抛出错误信息
    // 
    pub fn read(&mut self, input: &mut BytesMut, replica: &mut ReplicaInfo) -> Result<Vec<BytesMut>> {
        let (command, sql) = parse_com_query(input)?;
        match command {
            Command::COM_QUERY => {
                return Ok(self.query(&sql)?);
            }
            Command::COM_QUIT => {
                return Err(anyhow::Error::new::<std::io::Error>(std::io::ErrorKind::ConnectionAborted.into()));
            }
            Command::COM_REGISTER_SLAVE => {
                let com_register_slave = parse_com_register_slave(&input)?;
                replica.with_server_id(com_register_slave.server_id());
            }
            _ => {}
        }

        Ok(vec![ok_packet(self.client_capabilities, false)])
    }
}

pub fn ok_packet(client_capabilities: CapabilityFlags, is_eof: bool) -> BytesMut {
    let mut buf = BytesMut::new();
    let mut data = vec![];
    OkPacket::new(
        0,
        0,
        server_status_flags(),
        0,
        String::new().as_bytes(),
        String::new().as_bytes(),
        client_capabilities,
        is_eof,
    )
    .serialize(&mut data);
    buf.extend_from_slice(&data);
    buf
}
