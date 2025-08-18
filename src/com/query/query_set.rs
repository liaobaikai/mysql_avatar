use sqlparser::ast::Expr;

use crate::{com::query::get_var, variable::{get_session_var, Variable}};

pub fn get_var_setting_key(variable: sqlparser::ast::ObjectName) -> String {
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

pub fn get_var_setting_value(
    session_vars: &mut Vec<Variable>,
    exp: Expr,
) -> Result<String, anyhow::Error> {
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
        Expr::Value(vw) => match vw.value {
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => s,
            sqlparser::ast::Value::Number(val, _) => val,
            _ => String::new(),
        },
        _ => String::new(),
    };
    Ok(value)
}
