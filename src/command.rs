use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use bytes::{BufMut, BytesMut};
use chrono::{DateTime, Local, Utc};
use mysql_async::consts::CapabilityFlags;
use mysql_common::{
    misc::raw::{RawBytes, RawInt, int::LenEnc},
    proto::MySerialize,
};
use sqlparser::{
    ast::{Expr, Select, SelectItem, SetExpr, Statement},
    dialect::MySqlDialect,
    parser::Parser,
};

use crate::{
    consts::ColumnDefinitionFlags, get_server_status, packets::{ColumnDefinition, EofPacket, OkPacket}, variable::{get_global_var, get_session_var, set_session_var, Variable, SESSION_CHARSET_KEY_NAME}
};

// fn unsupport_session(sql: &str, _var: &str) -> Vec<u8> {
//     let mut data = Vec::new();
//     ErrPacket::Error(ServerError::new(
//         1064,
//         Some(SqlState::new(*b"42000")),
//         format!("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '{sql}' at line 1")
//         .as_bytes(),
//         get_capabilities(),
//     ))
//     .serialize(&mut data);
//     return data;
// }

// It is made up of 2 parts:
// the column definitions (a.k.a. the metadata)
// the actual rows
// fn handle_com_packet(key: &str, value: &str) -> Vec<BytesMut> {
//     let mut packets = Vec::new();
//     let mut data = Vec::new();
//     let mut buffer = BytesMut::new();

//     // Part1..
//     // the column definitions (a.k.a. the metadata)
//     // metadata_follows	Flag specifying if metadata are skipped or not. See enum_resultset_metadata
//     let metadata_follows = ResultSetMetadata::RESULTSET_METADATA_FULL;
//     if get_capabilities().contains(CapabilityFlags::CLIENT_OPTIONAL_RESULTSET_METADATA) {
//         buffer.put_u8(metadata_follows as u8);
//     }

//     // column_count
//     packets.push(query_select_text_resultset_col_count_packet(1));

//     //
//     if !get_capabilities().contains(CapabilityFlags::CLIENT_OPTIONAL_RESULTSET_METADATA)
//         || metadata_follows == ResultSetMetadata::RESULTSET_METADATA_FULL
//     {}

//     // Part2..
//     buffer.clear();
//     data.clear();
//     let cd = ColumnDefinition::new(
//         "".as_bytes(),
//         "".as_bytes(),
//         "".as_bytes(),
//         key.as_bytes(),
//         "".as_bytes(),
//         63, // binary
//         1,  //
//         8,  // binary
//         ColumnDefinitionFlags::BINARY_FLAG as u16,
//         0x00,
//     );
//     cd.serialize(&mut data);
//     buffer.extend_from_slice(&data);
//     packets.push(buffer.clone());

//     // if !get_capabilities().contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
//     //     data.clear();
//     //     EofPacket::new(get_server_status(), 0, get_capabilities()).serialize(&mut data);
//     //     buffer.extend_from_slice(&data);
//     //     // End of metadata
//     // }
//     // packets.push(buffer.clone());

//     // Part3..
//     // One or more Text Resultset Row
//     buffer.clear();
//     data.clear();
//     let packet2 = TextResultsetRow::new(Some([value.as_bytes()].to_vec()));
//     packet2.serialize(&mut data);
//     buffer.extend_from_slice(&data);
//     packets.push(buffer.clone());

//     // Part4..
//     buffer.clear();
//     data.clear();
//     if !get_capabilities().contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
//         EofPacket::new(get_server_status(), 0, get_capabilities()).serialize(&mut data);
//         buffer.extend_from_slice(&data);
//     } else if get_capabilities().contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
//         OkPacket2::new(
//             0,
//             0,
//             get_server_status(),
//             0,
//             String::new().as_bytes(),
//             String::new().as_bytes(),
//             get_capabilities(),
//         )
//         .serialize(&mut data);
//         buffer.extend_from_slice(&data);
//     }
//     packets.push(buffer.clone());

//     packets
// }

// NULL is sent as 0xFB
// everything else is converted to a string and is sent as string<lenenc>
fn serialize_str<'a>(buffer: &mut BytesMut, text: Option<&'a str>) {
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

fn serialize_str2<'a>(text: Option<&'a str>) -> BytesMut {
    let mut buffer = BytesMut::new();
    serialize_str(&mut buffer, text);
    buffer
}

fn serialize_col(buffer: &mut BytesMut, col_name: &str) {
    let mut column_data = vec![];
    ColumnDefinition::new(
        &[],
        &[],
        &[],
        col_name.as_bytes(),
        &[],
        63,
        1,
        8,
        ColumnDefinitionFlags::BINARY_FLAG as u16,
        0,
    )
    .serialize(&mut column_data);
    buffer.extend_from_slice(&column_data)
}

fn serialize_col2(col_name: &str) -> BytesMut {
    let mut buffer = BytesMut::new();
    serialize_col(&mut buffer, col_name);
    buffer
}

// #[allow(unused)]
fn query_select_text_resultset_col_count_packet(column_count: u64) -> BytesMut {
    let mut buffer = BytesMut::new();
    let column_count: RawInt<LenEnc> = RawInt::new(column_count);
    let mut data = vec![];
    column_count.serialize(&mut data);
    buffer.extend_from_slice(&data);
    buffer
}

// #[allow(unused)]
// fn query_select_text_resultset_col_definition_packet(key: &str) -> ColumnDefinition {
//     ColumnDefinition::new(
//         "".as_bytes(),
//         "".as_bytes(),
//         "".as_bytes(),
//         key.as_bytes(),
//         "".as_bytes(),
//         63,     // binary
//         0xFFFF, //
//         8,      // binary
//         ColumnDefinitionFlags::BINARY_FLAG as u16,
//         0x00,
//     )
// }

pub fn handle_query_select(
    s: Box<Select>,
    session_vars: &mut Vec<Variable>,
    client_capabilities: &CapabilityFlags,
) -> Result<Vec<BytesMut>> {
    //
    let mut column_definitions: Vec<BytesMut> = vec![];
    let mut column_values: Vec<BytesMut> = vec![];
    // let mut table_name = String::new();
    // for tab  in s.from {
    //     match tab.relation {
    //         TableFactor::Table { name, alias, args, with_hints, version, with_ordinality, partitions, json_path, sample, index_hints } => {

    //         }
    //         _ => {}
    //     }
    // }

    // 全部查询列
    for proj in s.projection {
        println!("proj: {:?}", proj);
        match proj {
            SelectItem::UnnamedExpr(exp) => {
                let (col, val) = match exp {
                    Expr::Function(func) => {
                        // 函数
                        let mut col = BytesMut::new();
                        let mut val = BytesMut::new();
                        for part in func.name.0.clone() {
                            match part {
                                sqlparser::ast::ObjectNamePart::Identifier(id) => {
                                    match id.value.to_lowercase().as_str() {
                                        "unix_timestamp" => {
                                            let now = Utc::now();
                                            let start: SystemTime = now.into();
                                            if let Ok(duration) = start.duration_since(UNIX_EPOCH) {
                                                let unix_timestamp = duration.as_secs();
                                                serialize_str(
                                                    &mut val,
                                                    Some(&format!("{unix_timestamp}")),
                                                );
                                                serialize_col(&mut col, &format!("{}()", id.value));
                                            }
                                        }
                                        "current_timestamp" => {
                                            //
                                            let now: DateTime<Local> = Local::now();
                                            let formatted_time =
                                                now.format("%Y-%m-%d %H:%M:%S").to_string();
                                            serialize_str(&mut val, Some(&formatted_time));
                                            serialize_col(&mut col, &format!("{}", id.value));
                                        }
                                        "now" => {
                                            //
                                            let now: DateTime<Local> = Local::now();
                                            let formatted_time =
                                                now.format("%Y-%m-%d %H:%M:%S").to_string();
                                            serialize_str(&mut val, Some(&formatted_time));
                                            serialize_col(&mut col, &format!("{}()", id.value));
                                        }
                                        "binlog_gtid_pos" => {
                                            // Mariadb专用函数，暂不支持，直接返回空
                                            // SELECT binlog_gtid_pos('mysql-bin.000001',328)
                                            let mut arg_values: Vec<String> =
                                                get_fun_args(&func.clone());
                                            let filename = arg_values.remove(0);
                                            let pos = arg_values.remove(0);
                                            serialize_col(
                                                &mut col,
                                                &format!("{}('{}',{})", id.value, filename, pos),
                                            );
                                            serialize_str(&mut val, Some(""));
                                        }
                                        _ => {}
                                    }
                                }
                                _ => {}
                            }
                        }
                        (col, val)
                    }
                    Expr::Identifier(id) => {
                        let name = id.value;
                        if name.starts_with("@") {
                            let val = match get_session_var(session_vars, &name) {
                                Ok(var) => serialize_str2(Some(var.value())),
                                Err(e) => {
                                    if name.starts_with("@@") {
                                        return Err(anyhow!("{e}"));
                                    } else if name.starts_with("@") {
                                        // 用户定义变量，直接返回NULL
                                        serialize_str2(None)
                                    } else {
                                        return Err(anyhow!("{e}"));
                                    }
                                }
                            };
                            (serialize_col2(&name), val)
                        } else {
                            (BytesMut::new(), BytesMut::new())
                        }
                    }
                    Expr::CompoundIdentifier(cid) => {
                        // a.b
                        let (var, name) = get_var(session_vars, cid)?;
                        let value = match var {
                            Some(v) => serialize_str2(Some(v.value())),
                            None => BytesMut::new(),
                        };
                        (serialize_col2(&name), value)
                    }
                    _ => (BytesMut::new(), BytesMut::new()),
                };

                column_definitions.push(col);
                column_values.push(val);
            }
            _ => {}
        }
    }

    Ok(build_packet(
        column_definitions,
        vec![column_values],
        client_capabilities,
    ))
}

// 获取函数的参数值
fn get_fun_args(func: &sqlparser::ast::Function) -> Vec<String> {
    let mut arg_values: Vec<String> = vec![];
    match func.args.clone() {
        sqlparser::ast::FunctionArguments::List(list) => {
            for arg in list.args {
                match arg {
                    sqlparser::ast::FunctionArg::Unnamed(fexp) => match fexp {
                        sqlparser::ast::FunctionArgExpr::Expr(exp) => match exp {
                            Expr::Value(v) => match v.value {
                                sqlparser::ast::Value::Number(val, _) => {
                                    arg_values.push(val);
                                }
                                sqlparser::ast::Value::SingleQuotedString(val) => {
                                    arg_values.push(val);
                                }
                                sqlparser::ast::Value::DoubleQuotedString(val) => {
                                    arg_values.push(val);
                                }
                                _ => {}
                            },
                            _ => {}
                        },
                        _ => {}
                    },
                    _ => {}
                }
            }
        }
        _ => {}
    }
    arg_values
}

fn build_packet(
    column_definitions: Vec<BytesMut>,
    rows: Vec<Vec<BytesMut>>,
    client_capabilities: &CapabilityFlags,
) -> Vec<BytesMut> {
    let mut packets: Vec<BytesMut> = vec![];
    let mut column_definitions_ = column_definitions.clone();
    let mut rows_ = rows.clone();

    // part1: 列数
    packets.push(query_select_text_resultset_col_count_packet(
        column_definitions_.len() as u64,
    ));

    // part2: 每列拆分一个包，多列多个包
    loop {
        if column_definitions_.is_empty() {
            break;
        }
        packets.push(column_definitions_.remove(0));
    }

    println!("client_capabilities: {client_capabilities:?}");
    if !client_capabilities.contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
        // Marker to set the end of metadata
        let mut buffer = BytesMut::new();
        let mut data = vec![];
        EofPacket::new(get_server_status(), 0, *client_capabilities).serialize(&mut data);
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
    if client_capabilities.contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
        OkPacket::new(
            0,
            0,
            get_server_status(),
            0,
            String::new().as_bytes(),
            String::new().as_bytes(),
            *client_capabilities,
            true,
        )
        .serialize(&mut data);
    } else {
        EofPacket::new(get_server_status(), 0, *client_capabilities).serialize(&mut data);
    }
    buffer.extend_from_slice(&data);

    packets.push(buffer.clone());

    packets
}

fn get_var(
    session_vars: &mut Vec<Variable>,
    cid: Vec<sqlparser::ast::Ident>,
) -> Result<(Option<Variable>, String), anyhow::Error> {
    let mut var = None;
    let mut name = String::new();
    let mut is_global_variable = false;
    let mut is_session_variable = false;
    for id in cid {
        if !name.is_empty() {
            name.push_str(".");
        }
        name.push_str(&id.value);
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
    Ok((var, name))
}

//
// fn handle_query_set(
//     s: Box<Set>,
//     session_vars: &mut HashMap<String, String>,
// ) -> Result<BytesMut> {
//     let mut buf = BytesMut::new();

//     Ok(buf)
// }

//
fn handle_command_set(
    set: sqlparser::ast::Set,
    session_vars: &mut Vec<Variable>,
    _client_capabilities: &CapabilityFlags,
) -> Result<()> {
    match set {
        sqlparser::ast::Set::SingleAssignment {
            scope: _,
            hivevar: _,
            variable,
            values,
        } => {
            let name = get_var_setting_key(variable);
            for exp in values {
                let value = get_var_setting_value(session_vars, exp)?;
                set_session_var(session_vars, &name, &value)?;
            }
        }
        sqlparser::ast::Set::MultipleAssignments { assignments } => {
            for variable in assignments {
                let name = get_var_setting_key(variable.name);
                let value = get_var_setting_value(session_vars, variable.value)?;
                println!("value: {:?}", value);
                set_session_var(session_vars, &name, &value)?;
            }
        }
        sqlparser::ast::Set::SetNames {
            charset_name,
            collation_name: _,
        } => {
            set_session_var(session_vars, SESSION_CHARSET_KEY_NAME, &charset_name.value)?;
        }
        _ => {}
    }

    Ok(())
}


// 返回实际数据
//
pub fn handle_command_query(
    sql: &str,
    session_vars: &mut Vec<Variable>,
    client_capabilities: &CapabilityFlags,
) -> Result<Vec<BytesMut>> {
    let mut buf = BytesMut::new();

    for stmt in Parser::parse_sql(&MySqlDialect {}, &sql)? {
        match stmt {
            Statement::Query(q) => match *q.body {
                SetExpr::Select(s) => {
                    println!("select: {:?}", s);
                    return Ok(handle_query_select(s, session_vars, client_capabilities)?);
                }
                _ => {}
            },
            Statement::Set(s) => {
                println!("set: {:?}", s);
                handle_command_set(s, session_vars, client_capabilities)?;

                let mut data = vec![];
                OkPacket::new(
                    0,
                    0,
                    get_server_status(),
                    0,
                    String::new().as_bytes(),
                    String::new().as_bytes(),
                    *client_capabilities,
                    false,
                )
                .serialize(&mut data);
                buf.extend_from_slice(&data);
            }
            Statement::ShowVariables {
                filter,
                global: _,
                session: _,
            } => match filter {
                Some(f) => match f {
                    sqlparser::ast::ShowStatementFilter::Like(v) => {
                        println!("v: {}", v);
                        let mut column_definitions: Vec<BytesMut> = vec![];
                        let mut column_values: Vec<BytesMut> = vec![];

                        // Variable_name, Value
                        column_definitions.push(serialize_col2("Variable_name"));
                        column_definitions.push(serialize_col2("Value"));

                        let var = get_session_var(session_vars, &v)?;
                        column_values.push(serialize_str2(Some(&var.name())));
                        column_values.push(serialize_str2(Some(&var.value())));

                        return Ok(build_packet(
                            column_definitions,
                            vec![column_values],
                            client_capabilities,
                        ));
                    }
                    _ => {}
                },
                _ => {
                    // 返回所有参数
                    let mut column_definitions: Vec<BytesMut> = vec![];
                    // Variable_name, Value
                    column_definitions.push(serialize_col2("Variable_name"));
                    column_definitions.push(serialize_col2("Value"));

                    let mut rows = Vec::new();
                    for var in session_vars.iter() {
                        if var.dynamic() {
                            continue;
                        }
                        let mut column_values: Vec<BytesMut> = vec![];
                        column_values.push(serialize_str2(Some(&var.name())));
                        column_values.push(serialize_str2(Some(&var.value())));
                        rows.push(column_values);
                    }

                    return Ok(build_packet(column_definitions, rows, client_capabilities));
                }
            },
            _ => {}
        }
    }
    Ok(vec![buf])
}
