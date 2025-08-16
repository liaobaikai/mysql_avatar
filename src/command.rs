use std::{
    borrow::Cow,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use chrono::Utc;
use mysql_async::consts::CapabilityFlags;
use mysql_common::{
    misc::raw::{RawBytes, RawInt, int::LenEnc},
    packets::SqlState,
    proto::MySerialize,
};
use sqlparser::{
    ast::{Expr, Select, SelectItem, SetExpr, Statement, ValueWithSpan},
    dialect::MySqlDialect,
    parser::Parser,
};

use crate::{
    constants::{ColumnDefinitionFlags, ResultSetMetadata},
    get_capabilities, get_server_status,
    packets::{
        ColumnDefinition, EofPacket, ErrPacket, OkPacket, OkPacket2, ServerError, TextResultsetRow,
    },
    vars::{
        SESSION_CHARSET_KEY_NAME, VARIABLE_SCOPE_SESSION, Variable, get_global_var,
        get_session_var, set_session_var,
    },
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

fn serialize_str<'a>(buffer: &mut BytesMut, text: &'a str) {
    let mut value = vec![];
    let data: RawBytes<'a, LenEnc> = RawBytes::new(text.as_bytes());
    data.serialize(&mut value);
    buffer.extend_from_slice(&value);
}

#[allow(unused)]
fn query_select_text_resultset_col_count_packet(column_count: u64) -> BytesMut {
    let mut buffer = BytesMut::new();
    let column_count: RawInt<LenEnc> = RawInt::new(column_count);
    let mut data = vec![];
    column_count.serialize(&mut data);
    buffer.extend_from_slice(&data);
    buffer
}

#[allow(unused)]
fn query_select_text_resultset_col_definition_packet(key: &str) -> ColumnDefinition {
    ColumnDefinition::new(
        "".as_bytes(),
        "".as_bytes(),
        "".as_bytes(),
        key.as_bytes(),
        "".as_bytes(),
        63,     // binary
        0xFFFF, //
        8,      // binary
        ColumnDefinitionFlags::BINARY_FLAG as u16,
        0x00,
    )
}

pub fn handle_query_select(
    s: Box<Select>,
    session_vars: &mut Vec<Variable>,
) -> Result<Vec<BytesMut>> {
    let mut packets: Vec<BytesMut> = vec![];
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
                        let mut col = BytesMut::new();
                        let mut val = BytesMut::new();

                        for part in func.name.0 {
                            match part {
                                sqlparser::ast::ObjectNamePart::Identifier(id) => {
                                    match id.value.to_lowercase().as_str() {
                                        "unix_timestamp" => {
                                            let now = Utc::now();
                                            let start: SystemTime = now.into();
                                            if let Ok(duration) = start.duration_since(UNIX_EPOCH) {
                                                // 将 Duration 转换为秒数（Unix 时间戳）
                                                let unix_timestamp = duration.as_secs();
                                                // col.extend_from_slice(&id.value.as_bytes());
                                                // val.put_u64(unix_timestamp);
                                                serialize_str(
                                                    &mut val,
                                                    &format!("{unix_timestamp}"),
                                                );

                                                let mut column_data = vec![];
                                                ColumnDefinition::new(
                                                    &[],
                                                    &[],
                                                    &[],
                                                    format!("{}()", id.value).as_bytes(),
                                                    &[],
                                                    63,
                                                    val.len() as u32,
                                                    8,
                                                    ColumnDefinitionFlags::BINARY_FLAG as u16,
                                                    0,
                                                )
                                                .serialize(&mut column_data);
                                                col.extend_from_slice(&column_data);
                                            }
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
                            let mut data = vec![];
                            data.extend_from_slice(name.as_bytes());
                            let var = get_session_var(session_vars, &name)?;
                            let mut value = BytesMut::new();
                            // value.extend_from_slice(var.value().as_bytes());

                            let mut column_data = vec![];
                            let mut column_data_mut = BytesMut::new();
                            ColumnDefinition::new(
                                &[],
                                &[],
                                &[],
                                data,
                                &[],
                                63,
                                var.value().len() as u32,
                                8,
                                ColumnDefinitionFlags::BINARY_FLAG as u16,
                                0,
                            )
                            .serialize(&mut column_data);
                            column_data_mut.extend_from_slice(&column_data);

                            serialize_str(&mut value, var.value());
                            (column_data_mut, value)
                        } else {
                            (BytesMut::new(), BytesMut::new())
                        }
                    }
                    Expr::CompoundIdentifier(cid) => {
                        // a.b
                        let (var, name) = get_var(session_vars, cid)?;
                        let (len, value) = match var {
                            Some(v) => {
                                let mut value = BytesMut::new();
                                serialize_str(&mut value, v.value());
                                (v.value().len(), value)
                            }
                            None => (0, BytesMut::new()),
                        };

                        let mut data = vec![];
                        data.extend_from_slice(name.as_bytes());
                        let mut column_data = vec![];
                        let mut column_data_mut = BytesMut::new();
                        ColumnDefinition::new(
                            &[],
                            &[],
                            &[],
                            data,
                            &[],
                            63,
                            len as u32,
                            8,
                            ColumnDefinitionFlags::BINARY_FLAG as u16,
                            0,
                        )
                        .serialize(&mut column_data);
                        column_data_mut.extend_from_slice(&column_data);
                        (column_data_mut, value)
                    }
                    // Expr::BinaryOp { left: _, op: _, right: _ } => {
                    // match *left {
                    //     Expr::Identifier(id) => {
                    //         if id.value.starts_with("@@") {
                    //             if op == BinaryOperator::Eq {
                    //                 match *right {
                    //                     Expr::Value(v) => {
                    //                         session_vars.insert(id.value, v.value.to_string());
                    //                         buffer.extend_from_slice(&session_var_setup());
                    //                     }
                    //                     _ => {}
                    //                 }
                    //             }
                    //         }
                    //     }
                    //     Expr::CompoundIdentifier(cid) => {
                    //         let mut key = String::new();
                    //         for val in cid {
                    //             if key.len() > 0 {
                    //                 key.push_str(".");
                    //             }
                    //             key.push_str(&val.value);
                    //         }

                    //         if op == BinaryOperator::Eq {
                    //             match *right {
                    //                 Expr::Value(v) => {
                    //                     if key.starts_with("@@global.") {
                    //                         buffer
                    //                             .extend_from_slice(&global_var_read_only(&key));
                    //                     } else if key.starts_with("@@session.") {
                    //                         session_vars.insert(key, v.value.to_string());
                    //                         buffer.extend_from_slice(&session_var_setup());
                    //                     }
                    //                 }
                    //                 _ => {}
                    //             }
                    //         }
                    //     }
                    //     _ => {}
                    // }
                    // (BytesMut::new(), BytesMut::new())
                    // }
                    _ => (BytesMut::new(), BytesMut::new()),
                };

                column_definitions.push(col);
                column_values.push(val);
            }
            _ => {}
        }
    }

    // part1: 列数
    packets.push(query_select_text_resultset_col_count_packet(
        column_definitions.len() as u64,
    ));

    // part2: 每列拆分一个包，多列多个包
    loop {
        if column_definitions.is_empty() {
            break;
        }
        packets.push(column_definitions.remove(0));
    }

    // part3: 一行放一个包，多行多个包
    let mut buffer = BytesMut::new();
    loop {
        if column_values.is_empty() {
            break;
        }
        let value = column_values.remove(0);
        buffer.extend_from_slice(&value);
    }
    packets.push(buffer.clone());

    // part4: 结束
    buffer.clear();
    let mut data = vec![];
    if !get_capabilities().contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
        EofPacket::new(get_server_status(), 0, get_capabilities()).serialize(&mut data);
        buffer.extend_from_slice(&data);
    } else if get_capabilities().contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
        OkPacket2::new(
            0,
            0,
            get_server_status(),
            0,
            String::new().as_bytes(),
            String::new().as_bytes(),
            get_capabilities(),
        )
        .serialize(&mut data);
        buffer.extend_from_slice(&data);
    }
    packets.push(buffer.clone());

    Ok(packets)
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
fn handle_command_set(set: sqlparser::ast::Set, session_vars: &mut Vec<Variable>) -> Result<()> {
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

fn get_var_setting_key(variable: sqlparser::ast::ObjectName) -> String {
    let mut name = String::new();
    for part in variable.0 {
        println!("part: {:?}", part);
        match part {
            sqlparser::ast::ObjectNamePart::Identifier(id) => {
                name.push_str(&id.value);
            }
            _ => {}
        }
    }
    name
}

fn get_var_setting_value(session_vars: &mut Vec<Variable>, exp: Expr) -> Result<String, anyhow::Error> {
    let value = match exp {
        Expr::Identifier(id) => {
            let var = get_session_var(session_vars, &id.value)?;
            var.value().to_string()
        }
        Expr::CompoundIdentifier(cid) => {
            let (var, _name) = get_var(session_vars, cid)?;
            match var {
                Some(v) => v.value().to_owned(),
                None => String::new(),
            }
        }
        Expr::Value(vw) => {
            match vw.value {
                sqlparser::ast::Value::SingleQuotedString(s) | sqlparser::ast::Value::DoubleQuotedString(s) => {
                    s
                },
                _ => String::new()
            }
        }
        _ => String::new(),
    };
    Ok(value)
}

// 返回实际数据
//
pub fn handle_command_query(sql: &str, session_vars: &mut Vec<Variable>) -> Result<Vec<BytesMut>> {
    let mut buf = BytesMut::new();

    for stmt in Parser::parse_sql(&MySqlDialect {}, &sql)? {
        match stmt {
            Statement::Query(q) => match *q.body {
                SetExpr::Select(s) => {
                    println!("select: {:?}", s);
                    return Ok(handle_query_select(s, session_vars)?);
                }
                _ => {}
            },
            Statement::Set(s) => {
                println!("set: {:?}", s);
                handle_command_set(s, session_vars)?;

                let mut data = vec![];
                OkPacket::new(
                    0,
                    0,
                    get_server_status(),
                    0,
                    String::new().as_bytes(),
                    String::new().as_bytes(),
                    get_capabilities(),
                )
                .serialize(&mut data);
                buf.extend_from_slice(&data);
            }
            _ => {}
        }
    }
    Ok(vec![buf])
}
