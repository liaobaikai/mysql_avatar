use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Local, Utc};
use sqlparser::ast::{Expr, Function, ObjectNamePart};

use crate::com::KvPair;


// 获取sql中的函数名
pub fn query_fn_name(func: &Function) -> String {
    let mut fn_name = String::new();
    for part in func.name.0.clone() {
        match part {
            ObjectNamePart::Identifier(id) => {
                fn_name.push_str(id.value.to_lowercase().as_str());
            }
            _ => {}
        }
    }
    fn_name
}

// 获取时间戳
pub fn unix_timestamp() -> u64 {
    let now = Utc::now();
    let start: SystemTime = now.into();
    start
        .duration_since(UNIX_EPOCH)
        .map(|a| a.as_secs())
        .unwrap_or_default()
}

pub fn unix_timestamp_string(col_name: &str) -> KvPair {
    KvPair::new(col_name.to_owned(), format!("{}", unix_timestamp()))
}

// current_timestamp
// alias: now
pub fn current_timestamp(col_name: &str) -> KvPair {
    let now: DateTime<Local> = Local::now();
    KvPair::new(
        col_name.to_owned(),
        now.format("%Y-%m-%d %H:%M:%S").to_string(),
    )
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

// 不支持，直接返回""
pub fn binlog_gtid_pos(func: &Function) -> KvPair {
    let fn_name = query_fn_name(&func);

    let mut arg_values = get_fun_args(&func);
    let filename = arg_values.remove(0);
    let pos = arg_values.remove(0);
    KvPair::new(
        format!("{}('{}',{})", fn_name, filename, pos),
        String::new(),
    )
}
