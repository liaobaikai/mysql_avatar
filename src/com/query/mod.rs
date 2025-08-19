use anyhow::{Result, anyhow};
use bytes::BytesMut;
use mysql_async::consts::{CapabilityFlags, ColumnType};
use mysql_common::{
    misc::raw::{RawBytes, RawInt, int::LenEnc},
    proto::MySerialize,
};
use sqlparser::{
    ast::{Expr, Function, Select, SelectItem, Set, SetExpr, ShowStatementFilter, Statement},
    dialect::MySqlDialect,
    parser::Parser,
};

use crate::{
    com::{
        ok_packet, query::{
            query_select_func::{
                binlog_gtid_pos, current_timestamp, query_fn_name, unix_timestamp_string,
            },
            query_set::{get_var_setting_key, get_var_setting_value},
        }, KvPair, Queryable, SqlCommand
    },
    consts::{Charset, ColumnDefinitionFlags},
    mysqld::server_status_flags,
    packets::{ColumnDefinition, EofPacket, OkPacket},
    variable::{
        get_global_var, get_session_var, set_session_var, Variable, SESSION_CHARSET_KEY_NAME, SYSVARS
    },
};

pub mod query_select_func;
pub mod query_set;

pub fn text_resultset_col_count_packet(column_count: u64) -> BytesMut {
    let mut buffer = BytesMut::new();
    let column_count: RawInt<LenEnc> = RawInt::new(column_count);
    let mut data = vec![];
    column_count.serialize(&mut data);
    buffer.extend_from_slice(&data);
    buffer
}

// NULL is sent as 0xFB
// everything else is converted to a string and is sent as string<lenenc>
pub fn serialize_str<'a>(buffer: &mut BytesMut, text: Option<&'a str>) {
    match text {
        Some(s) => {
            let mut value = vec![];
            let data: RawBytes<'a, LenEnc> = RawBytes::new(s.as_bytes());
            data.serialize(&mut value);
            buffer.extend_from_slice(&value);
        }
        None => {
            buffer.extend_from_slice(&[0xFB]);
        }
    }
}

pub fn serialize_str2<'a>(text: Option<&'a str>) -> BytesMut {
    let mut buffer = BytesMut::new();
    serialize_str(&mut buffer, text);
    buffer
}

pub fn serialize_col(buffer: &mut BytesMut, col_name: &str) {
    let mut column_data = vec![];
    ColumnDefinition::new(
        &[],
        &[],
        &[],
        col_name.as_bytes(),
        &[],
        Charset::LATIN1_SWEDISH_CI as u16,
        0,
        ColumnType::MYSQL_TYPE_STRING as u8, // 如果需要右对齐，可以用 8: LONGLONG
        ColumnDefinitionFlags::BINARY_FLAG as u16,
        0,
    )
    .serialize(&mut column_data);
    buffer.extend_from_slice(&column_data)
}

pub fn serialize_col2(col_name: &str) -> BytesMut {
    let mut buffer = BytesMut::new();
    serialize_col(&mut buffer, col_name);
    buffer
}

pub fn get_var(
    session_vars: &mut Vec<Variable>,
    cid: Vec<sqlparser::ast::Ident>,
) -> Result<(Option<Variable>, String), anyhow::Error> {
    let mut var = None;
    let mut col_name = String::new();
    let mut is_global_variable = false;
    let mut is_session_variable = false;
    for id in cid {
        if !col_name.is_empty() {
            col_name.push_str(".");
        }
        col_name.push_str(&id.value);
        // 变量

        if is_global_variable {
            var = Some(get_global_var(&id.value)?);
            break;
        } else if is_session_variable {
            var = Some(get_session_var(session_vars, &id.value)?);
            break;
        } else {
            // Unsupport.
        }

        if id.value.to_lowercase().starts_with("@@global") {
            is_global_variable = true;
        } else if id.value.to_lowercase().starts_with("@@session") {
            is_session_variable = true;
        } else {
            // 普通列
        }
    }
    Ok((var, col_name))
}

impl<'a> Queryable for SqlCommand<'a> {
    fn query(&mut self, sql: &str) -> Result<Vec<BytesMut>> {
        for stmt in Parser::parse_sql(&MySqlDialect {}, &sql)? {
            match stmt {
                Statement::Query(q) => match *q.body {
                    SetExpr::Select(s) => {
                        println!("select: {:?}", s);
                        return self.select(*s);
                    }
                    _ => {}
                },

                Statement::Set(s) => {
                    println!("set: {:?}", s);
                    return self.set(s);
                }

                Statement::ShowVariables {
                    filter,
                    global,
                    session,
                } => {
                    println!("ShowVariables::filter: {:?}", filter);
                    return self.show(filter, global, session);
                }

                _ => {}
            }
        }

        Ok(vec![])
    }

    fn select_func(&mut self, f: Function) -> Result<KvPair> {
        let fn_name = query_fn_name(&f);
        let data = match fn_name.as_str() {
            "unix_timestamp" => unix_timestamp_string(&fn_name),
            "current_timestamp" | "now" => current_timestamp(&fn_name),
            "binlog_gtid_pos" => binlog_gtid_pos(&f),
            _ => KvPair::default(),
        };
        Ok(data)
    }

    fn select(&mut self, s: Select) -> Result<Vec<BytesMut>> {
        // set_session_var(&mut self.session_vars, "1111", "111")?;
        let mut column_definitions: Vec<BytesMut> = vec![];
        let mut column_values: Vec<BytesMut> = vec![];

        for item in s.projection {
            println!("item: {:?}", item);
            let kv_pair = match item {
                SelectItem::UnnamedExpr(exp) => match exp {
                    // 函数
                    Expr::Function(func) => self.select_func(func)?,
                    Expr::Identifier(id) => {
                        let name = id.value;
                        if name.starts_with("@") {
                            match get_session_var(&mut self.session_vars, &name) {
                                Ok(var) => KvPair::new(name, var.value().to_owned()),
                                Err(e) => {
                                    if name.starts_with("@@") {
                                        // 系统全局参数
                                        return Err(anyhow!("{e}"));
                                    } else if name.starts_with("@") {
                                        // 用户定义变量，直接返回NULL
                                        KvPair::new(name, String::new())
                                    } else {
                                        return Err(anyhow!("{e}"));
                                    }
                                }
                            }
                        } else {
                            KvPair::default()
                        }
                    }
                    Expr::CompoundIdentifier(cid) => {
                        // a.b
                        let (var, col_name) = get_var(&mut self.session_vars, cid)?;
                        match var {
                            Some(v) => KvPair::new(col_name, v.value().to_owned()),
                            None => KvPair::default(),
                        }
                    }
                    _ => KvPair::default(),
                },
                _ => KvPair::default(),
            };

            column_definitions.push(serialize_col2(&kv_pair.key));
            column_values.push(serialize_str2(Some(&kv_pair.val)));
        }

        Ok(self.as_response_packet(column_definitions, vec![column_values]))
    }

    fn set(&mut self, s: Set) -> Result<Vec<BytesMut>> {
        match s {
            sqlparser::ast::Set::SingleAssignment {
                scope: _,
                hivevar: _,
                variable,
                values,
            } => {
                let name = get_var_setting_key(variable);
                for exp in values {
                    let value = get_var_setting_value(&mut self.session_vars, exp)?;
                    set_session_var(&mut self.session_vars, &name, &value)?;
                }
            }
            sqlparser::ast::Set::MultipleAssignments { assignments } => {
                for variable in assignments {
                    let name = get_var_setting_key(variable.name);
                    let value = get_var_setting_value(&mut self.session_vars, variable.value)?;
                    println!("value: {:?}", value);
                    set_session_var(&mut self.session_vars, &name, &value)?;
                }
            }
            sqlparser::ast::Set::SetNames {
                charset_name,
                collation_name: _,
            } => {
                set_session_var(
                    &mut self.session_vars,
                    SESSION_CHARSET_KEY_NAME,
                    &charset_name.value,
                )?;
            }
            _ => {}
        }

        // let mut buf = BytesMut::new();
        // let mut data = vec![];
        // OkPacket::new(
        //     0,
        //     0,
        //     server_status_flags(),
        //     0,
        //     String::new().as_bytes(),
        //     String::new().as_bytes(),
        //     self.client_capabilities,
        //     false,
        // )
        // .serialize(&mut data);
        // buf.extend_from_slice(&data);

        Ok(vec![ok_packet(self.client_capabilities, false)])
    }

    fn show(
        &mut self,
        filter: Option<ShowStatementFilter>,
        global: bool,
        session: bool,
    ) -> Result<Vec<BytesMut>> {
        match filter {
            Some(f) => match f {
                sqlparser::ast::ShowStatementFilter::Like(v) => {
                    println!("v: {}", v);
                    let mut column_definitions: Vec<BytesMut> = vec![];
                    let mut column_values: Vec<BytesMut> = vec![];

                    // Variable_name, Value
                    column_definitions.push(serialize_col2("Variable_name"));
                    column_definitions.push(serialize_col2("Value"));

                    if session || (!global && !session) {
                        let var = get_session_var(&mut self.session_vars, &v)?;
                        column_values.push(serialize_str2(Some(&var.name())));
                        column_values.push(serialize_str2(Some(&var.value())));
                    } else if global {
                        let var = get_global_var(&v)?;
                        column_values.push(serialize_str2(Some(&var.name())));
                        column_values.push(serialize_str2(Some(&var.value())));
                    }

                    Ok(self.as_response_packet(column_definitions, vec![column_values]))
                }
                _ => Err(anyhow!("show: unsupport filter {:?}", f)),
            },
            _ => {
                // 返回所有参数
                let mut column_definitions: Vec<BytesMut> = vec![];
                // Variable_name, Value
                column_definitions.push(serialize_col2("Variable_name"));
                column_definitions.push(serialize_col2("Value"));

                let mut rows = Vec::new();

                if session || (!global && !session) {
                    for var in &mut self.session_vars.iter() {
                        if var.dynamic() {
                            continue;
                        }
                        let mut column_values: Vec<BytesMut> = vec![];
                        column_values.push(serialize_str2(Some(&var.name())));
                        column_values.push(serialize_str2(Some(&var.value())));
                        rows.push(column_values);
                    }
                } else if global {
                    for var in &mut SYSVARS.iter() {
                        if var.dynamic() {
                            continue;
                        }
                        let mut column_values: Vec<BytesMut> = vec![];
                        column_values.push(serialize_str2(Some(&var.name())));
                        column_values.push(serialize_str2(Some(&var.value())));
                        rows.push(column_values);
                    }
                }
                Ok(self.as_response_packet(column_definitions, rows))
            }
        }
    }

    fn as_response_packet(
        &mut self,
        column_definitions: Vec<BytesMut>,
        rows: Vec<Vec<BytesMut>>,
    ) -> Vec<BytesMut> {
        let mut packets: Vec<BytesMut> = vec![];
        let mut column_definitions_ = column_definitions.clone();
        let mut rows_ = rows.clone();

        // part1: 列数
        packets.push(text_resultset_col_count_packet(
            column_definitions_.len() as u64
        ));

        // part2: 每列拆分一个包，多列多个包
        loop {
            if column_definitions_.is_empty() {
                break;
            }
            packets.push(column_definitions_.remove(0));
        }

        if !self
            .client_capabilities
            .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
        {
            // Marker to set the end of metadata
            let mut buffer = BytesMut::new();
            let mut data = vec![];
            EofPacket::new(server_status_flags(), 0, self.client_capabilities).serialize(&mut data);
            buffer.extend_from_slice(&data);
            packets.push(buffer.clone());
        }

        // part3: 一行放一个包，多行多个包
        for row in rows_.iter_mut() {
            let mut buffer = BytesMut::new();
            loop {
                if row.is_empty() {
                    break;
                }
                let value = row.remove(0);
                buffer.extend_from_slice(&value);
            }
            packets.push(buffer.clone());
        }

        // 如果中间有错误，则返回ERR_Packet

        // part4: 结束
        let mut buffer = BytesMut::new();
        let mut data = vec![];
        if self
            .client_capabilities
            .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
        {
            OkPacket::new(
                0,
                0,
                server_status_flags(),
                0,
                String::new().as_bytes(),
                String::new().as_bytes(),
                self.client_capabilities,
                true,
            )
            .serialize(&mut data);
        } else {
            EofPacket::new(server_status_flags(), 0, self.client_capabilities).serialize(&mut data);
        }
        buffer.extend_from_slice(&data);
        packets.push(buffer.clone());
        packets
    }
}
