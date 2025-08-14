use std::collections::HashMap;

use anyhow::{anyhow, Result};
use bytes::BytesMut;
use lazy_static::lazy_static;
use sqlparser::{
    ast::{BinaryOperator, Expr, Select, SelectItem, SetExpr, Statement},
    dialect::MySqlDialect,
    parser::Parser,
};
use uuid::Uuid;

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

pub fn handle_query_select(s: Box<Select>, session_vars: &mut HashMap<String, String>) -> Result<()> {
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
                    Expr::BinaryOp { left, op, right } => match *left {
                        // 修改参数
                        Expr::Identifier(id) => {
                            if id.value.starts_with("@@") {
                                if op == BinaryOperator::Eq {
                                    match *right {
                                        Expr::Value(v) => {
                                            session_vars.insert(id.value, v.value.to_string());
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
                                            // 1238
                                            return Err(anyhow!("Global Variable '{}' is a read only variable", key));
                                        } else if key.starts_with("@@session.") {
                                            session_vars.insert(key, v.value.to_string());
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
            _ => {}
        }
    }
    Ok(())
}

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
                    handle_query_select(s, session_vars)?;

                },
                _ => {}
            },
            Statement::Set(s) => {
                println!("s: {:?}", s);
            }
            _ => {}
        }
    }
    Ok(buf)
}
