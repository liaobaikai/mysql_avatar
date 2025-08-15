use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use uuid::Uuid;

lazy_static! {
    pub static ref SYSVARS: Vec<Variable> = {
        let mut vars = vec![];
        vars.push(Variable::new("version_comment".to_owned(), VARIABLE_SCOPE_GLOBAL, false, "MySQL Community Server (GPL)".to_owned()));
        vars.push(Variable::new("server_id".to_owned(), VARIABLE_SCOPE_GLOBAL, false, "1".to_owned()));
        vars.push(Variable::new("server_uuid".to_owned(), VARIABLE_SCOPE_GLOBAL, false, Uuid::new_v4().to_string()));
        vars
    };
}

pub const VARIABLE_SCOPE_GLOBAL: u8 = 1;
pub const VARIABLE_SCOPE_SESSION: u8 = 2;

#[derive(Debug, Clone)]
pub struct Variable {
    name: String,
    scope: u8,
    dynamic: bool,
    data_type: &'static str,
    default_value: String,
    minimum_value: Option<&'static str>,
    maximum_value: Option<&'static str>,
}

impl Variable {
    pub fn new(
        name: String,
        scope: u8,
        dynamic: bool,
        default_value: String,
    ) -> Self {
        Self {
            name,
            scope,
            dynamic,
            data_type: "",
            default_value,
            minimum_value: None,
            maximum_value: None,
        }
    }

    pub fn value(&self) -> &String {
        &self.default_value
    }
}

// 
// id: @@session, @@global
// 
pub fn get_global_var(name: &str) -> Result<Variable> {
    for var in SYSVARS.iter() {
        // name==name
        // @@name==@@name
        if name.eq_ignore_ascii_case(&var.name) || name.eq_ignore_ascii_case(&format!("@@{}", var.name)) {
            return Ok(var.clone());
        }
    }
    Err(anyhow!("Unknown {} variable '{}'", if name.starts_with("@@") { "system" } else { "user" }, name))
}

pub fn set_global_var(name: &str, _value: &str) -> Result<()>{
    // for var in SYSVARS.iter() {
    //     // name==name
    //     // @@name==@@name
    //     if name.eq_ignore_ascii_case(var.name) || name.eq_ignore_ascii_case(&format!("@@{}", var.name)) {
    //         return Some(var.clone());
    //     }
    // }
    // None
    Err(anyhow!("Variable '{}' is a read only variable", name))
}


pub fn set_session_var(session_vars: &mut Vec<Variable>, name: &str, value: &str) -> Result<()>{
    let mut exists = false;
    for var in session_vars.iter_mut() {
        // name==name
        // @@name==@@name
        if name.eq_ignore_ascii_case(&var.name) || name.eq_ignore_ascii_case(&format!("@@{}", var.name)) || name.eq_ignore_ascii_case(&format!("@{}", var.name)){
            var.default_value = value.to_string();
            exists = true;
        }
    }
    if !exists {
        session_vars.push(Variable::new(name.to_owned(), VARIABLE_SCOPE_SESSION, true, value.to_string()));
    }
    Ok(())
}

pub fn get_session_var(session_vars: &mut Vec<Variable>, name: &str) -> Result<Variable>{
    for var in session_vars.iter() {
        // name==name
        // @@name==@@name
        if name.eq_ignore_ascii_case(&var.name) || name.eq_ignore_ascii_case(&format!("@@{}", var.name)) || name.eq_ignore_ascii_case(&format!("@{}", var.name)) {
            return Ok(var.clone());
        }
    }
    Err(anyhow!("Unknown {} variable '{}'", if name.starts_with("@@") { "system" } else { "user" }, name))
}