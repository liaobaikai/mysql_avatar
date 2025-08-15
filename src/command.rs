use std::collections::HashMap;

use anyhow::{Ok, Result};
use bytes::BytesMut;
use lazy_static::lazy_static;
use mysql_common::{packets::SqlState, proto::MySerialize};
use sqlparser::{
    ast::{BinaryOperator, Expr, Select, SelectItem, SetExpr, Statement},
    dialect::MySqlDialect,
    parser::Parser,
};
use uuid::Uuid;

use crate::{
    get_capabilities, get_server_status,
    packets::{ErrPacket, OkPacket, ServerError},
};

lazy_static! {
    pub static ref SYSVARS: HashMap<String, String> = {
        let mut m = HashMap::new();
        m.insert(
            String::from("@@global.version_comment"),
            String::from("MySQL Community Server (GPL)"),
        );
        m.insert(String::from("@@global.server_id"), String::from("1"));
        m.insert(
            String::from("@@global.server_uuid"),
            Uuid::new_v4().to_string(),
        );
        m
    };
}

fn global_var_read_only(var: &str) -> Vec<u8> {
    let mut data = Vec::new();
    ErrPacket::Error(ServerError::new(
        1238,
        None,
        format!(
            "Global Variable '{}' is a read only variable",
            var.trim_start_matches("@@global.")
        )
        .as_bytes(),
        get_capabilities(),
    ))
    .serialize(&mut data);
    return data;
}

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

fn session_var_setup() -> Vec<u8> {
    let mut data: Vec<u8> = Vec::new();
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
    return data;
}

pub fn handle_query_select(
    s: Box<Select>,
    session_vars: &mut HashMap<String, String>,
) -> Result<BytesMut> {
    let mut buffer = BytesMut::new();
    for proj in s.projection {
        println!("proj: {:?}", proj);
        match proj {
            SelectItem::UnnamedExpr(exp) => {
                // println!("exp: {:?}", exp);
                match exp {
                    Expr::Identifier(id) => {
                        if id.value.starts_with("@@") {
                            println!("id: {}", id.value);
                            let value = session_vars.get(&id.value).unwrap();
                            println!("value: {}", value);
                        }
                    }
                    Expr::CompoundIdentifier(cid) => {
                        // a.b
                        let mut key = String::new();
                        for val in cid {
                            if key.len() > 0 {
                                key.push_str(".");
                            }
                            key.push_str(&val.value);
                        }
                        if key.starts_with("@@global.") {
                            println!("key: {}", key);
                            let value = SYSVARS.get(&key).unwrap();
                            println!("value: {}", value);
                        } else if key.starts_with("@@session.") {
                            println!("key: {}", key);
                            let value = session_vars.get(&key).unwrap();
                            println!("value: {}", value);
                        }
                    }
                    Expr::BinaryOp { left, op, right } => {
                        match *left {
                            // 修改参数
                            Expr::Identifier(id) => {
                                if id.value.starts_with("@@") {
                                    if op == BinaryOperator::Eq {
                                        match *right {
                                            Expr::Value(v) => {
                                                session_vars.insert(id.value, v.value.to_string());
                                                buffer.extend_from_slice(&session_var_setup());
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                            Expr::CompoundIdentifier(cid) => {
                                let mut key = String::new();
                                for val in cid {
                                    if key.len() > 0 {
                                        key.push_str(".");
                                    }
                                    key.push_str(&val.value);
                                }

                                if op == BinaryOperator::Eq {
                                    match *right {
                                        Expr::Value(v) => {
                                            if key.starts_with("@@global.") {
                                                buffer
                                                    .extend_from_slice(&global_var_read_only(&key));
                                            } else if key.starts_with("@@session.") {
                                                session_vars.insert(key, v.value.to_string());
                                                buffer.extend_from_slice(&session_var_setup());
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    Ok(buffer)
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
pub fn handle_command_query(
    sql: &str,
    session_vars: &mut HashMap<String, String>,
) -> Result<BytesMut> {
    let mut buf = BytesMut::new();

    for stmt in Parser::parse_sql(&MySqlDialect {}, &sql)? {
        match stmt {
            Statement::Query(q) => match *q.body {
                SetExpr::Select(s) => {
                    buf = handle_query_select(s, session_vars)?;
                }
                _ => {}
            },
            Statement::Set(_s) => {
                buf.extend_from_slice(&unsupport_session(sql, ""));
            }
            _ => {}
        }
    }
    Ok(buf)
}
