use anyhow::Result;
use bytes::{BufMut, BytesMut};
use mysql_async::consts::CapabilityFlags;
use mysql_common::{
    misc::raw::{RawInt, int::LenEnc},
    packets::SqlState,
    proto::MySerialize,
};
use sqlparser::{
    ast::{Expr, Select, SelectItem, SetExpr, Statement},
    dialect::MySqlDialect,
    parser::Parser,
};

use crate::{
    constants::{ColumnDefinitionFlags, ResultSetMetadata},
    get_capabilities, get_server_status,
    packets::{
        ColumnDefinition, EofPacket, ErrPacket, OkPacket, OkPacket2, ServerError, TextResultsetRow,
    },
    vars::{Variable, get_global_var, get_session_var},
};


fn unsupport_session(sql: &str, _var: &str) -> Vec<u8> {
    let mut data = Vec::new();
    ErrPacket::Error(ServerError::new(
        1064,
        Some(SqlState::new(*b"42000")),
        format!("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '{sql}' at line 1")
        .as_bytes(),
        get_capabilities(),
    ))
    .serialize(&mut data);
    return data;
}


// It is made up of 2 parts:
// the column definitions (a.k.a. the metadata)
// the actual rows
fn handle_com_packet(key: &str, value: &str) -> Vec<BytesMut> {
    let mut packets = Vec::new();
    let mut data = Vec::new();
    let mut buffer = BytesMut::new();

    // Part1..
    // the column definitions (a.k.a. the metadata)
    // metadata_follows	Flag specifying if metadata are skipped or not. See enum_resultset_metadata
    let metadata_follows = ResultSetMetadata::RESULTSET_METADATA_FULL;
    if get_capabilities().contains(CapabilityFlags::CLIENT_OPTIONAL_RESULTSET_METADATA) {
        buffer.put_u8(metadata_follows as u8);
    }

    // column_count
    packets.push(query_select_text_resultset_col_count_packet(1));

    //
    if !get_capabilities().contains(CapabilityFlags::CLIENT_OPTIONAL_RESULTSET_METADATA)
        || metadata_follows == ResultSetMetadata::RESULTSET_METADATA_FULL
    {}

    // Part2..
    buffer.clear();
    data.clear();
    let cd = ColumnDefinition::new(
        "".as_bytes(),
        "".as_bytes(),
        "".as_bytes(),
        key.as_bytes(),
        "".as_bytes(),
        63, // binary
        1,  //
        8,  // binary
        ColumnDefinitionFlags::BINARY_FLAG as u16,
        0x00,
    );
    cd.serialize(&mut data);
    buffer.extend_from_slice(&data);
    packets.push(buffer.clone());

    // if !get_capabilities().contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
    //     data.clear();
    //     EofPacket::new(get_server_status(), 0, get_capabilities()).serialize(&mut data);
    //     buffer.extend_from_slice(&data);
    //     // End of metadata
    // }
    // packets.push(buffer.clone());

    // Part3..
    // One or more Text Resultset Row
    buffer.clear();
    data.clear();
    let packet2 = TextResultsetRow::new(Some([value.as_bytes()].to_vec()));
    packet2.serialize(&mut data);
    buffer.extend_from_slice(&data);
    packets.push(buffer.clone());

    // Part4..
    buffer.clear();
    data.clear();
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

    packets
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
                    Expr::Identifier(id) => {
                        let name = id.value;
                        if name.starts_with("@") {
                            let mut data = vec![];
                            data.extend_from_slice(name.as_bytes());
                            let var = get_session_var(session_vars, &name)?;
                            let mut value = BytesMut::new();
                            value.extend_from_slice(var.value().as_bytes());

                            let mut column_data = vec![];
                            let mut column_data_mut = BytesMut::new();
                            ColumnDefinition::new(
                                &[],
                                &[],
                                &[],
                                data,
                                &[],
                                63,
                                value.len() as u32,
                                8,
                                ColumnDefinitionFlags::BINARY_FLAG as u16,
                                0,
                            )
                            .serialize(&mut column_data);
                            column_data_mut.extend_from_slice(&column_data);
                            (column_data_mut, value)
                        } else {
                            (BytesMut::new(), BytesMut::new())
                        }
                    }
                    Expr::CompoundIdentifier(cid) => {
                        // a.b
                        let mut var = None;
                        let mut name = String::new();
                        for id in cid {
                            if !name.is_empty() {
                                name.push_str(".");
                            }
                            // 变量
                            if id.value.to_lowercase().starts_with("@@global.") {
                                var = Some(get_global_var(&id.value)?)
                            } else if id.value.to_lowercase().starts_with("@@session.") {
                                var = Some(get_session_var(session_vars, &id.value)?)
                            } else {
                                // 普通列
                            }
                            name.push_str(&id.value);
                        }

                        let value = match var {
                            Some(v) => {
                                let mut value = BytesMut::new();
                                value.extend_from_slice(v.value().as_bytes());
                                value
                            }
                            None => BytesMut::new(),
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
                            value.len() as u32,
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

    // part2: 每个列拆分一个包，多列多个包
    loop {
        if column_definitions.is_empty() {
            break;
        }
        packets.push(column_definitions.remove(0));
    }

    // part3: 一个行放一个包，多行多个包
    let mut buffer = BytesMut::new();
    loop {
        if column_values.is_empty() {
            break;
        }
        let value = column_values.remove(0);
        buffer.put_u8(value.len() as u8);
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

//
// fn handle_query_set(
//     s: Box<Set>,
//     session_vars: &mut HashMap<String, String>,
// ) -> Result<BytesMut> {
//     let mut buf = BytesMut::new();

//     Ok(buf)
// }

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
            Statement::Set(_s) => {
                buf.extend_from_slice(&unsupport_session(sql, ""));
            }
            _ => {}
        }
    }
    Ok(vec![buf])
}
