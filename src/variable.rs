use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use uuid::Uuid;

pub const VARIABLE_PROTOCOL_VERSION: u8 = 10;
pub const VARIABLE_VERSION: &str = "8.0.43";
pub const VARIABLE_DEFAULT_AUTHENTICATION_PLUGIN: &str = "default_authentication_plugin";

// relay_log_basename = "./data/binlog/"

lazy_static! {
    pub static ref SYSVARS: Vec<Variable> = {
        let mut vars = vec![];
        vars.push(Variable::new("version".to_owned(), VARIABLE_SCOPE_GLOBAL, false, VARIABLE_VERSION.to_owned()));
        // vars.push(Variable::new("innodb_version".to_owned(), VARIABLE_SCOPE_GLOBAL, false, VARIABLE_VERSION.to_owned()));
        vars.push(Variable::new("protocol_version".to_owned(), VARIABLE_SCOPE_GLOBAL, false, format!("{VARIABLE_PROTOCOL_VERSION}")));
        vars.push(Variable::new("version_comment".to_owned(), VARIABLE_SCOPE_GLOBAL, false, "MySQL Community Server (GPL)".to_owned()));
        vars.push(Variable::new("server_id".to_owned(), VARIABLE_SCOPE_GLOBAL, false, "1".to_owned()));
        vars.push(Variable::new("server_uuid".to_owned(), VARIABLE_SCOPE_GLOBAL, false, Uuid::new_v4().to_string()));
        vars.push(Variable::new("binlog_checksum".to_owned(), VARIABLE_SCOPE_GLOBAL, false, "CRC32".to_owned()));
        vars.push(Variable::new("gtid_mode".to_owned(), VARIABLE_SCOPE_GLOBAL, false, "ON".to_owned()));
        vars.push(Variable::new("rpl_semi_sync_master_enabled".to_owned(), VARIABLE_SCOPE_GLOBAL, false, "0".to_owned()));
        
        vars.push(Variable::new("default_authentication_plugin".to_owned(), VARIABLE_SCOPE_GLOBAL, false, "mysql_native_password".to_owned()));
        vars.push(Variable::new("datadir".to_owned(), VARIABLE_SCOPE_GLOBAL, false, "".to_owned()));
        vars.push(Variable::new("relay_log_basename".to_owned(), VARIABLE_SCOPE_GLOBAL, false, "C:/Users/BK-liao/mysqltest/data/binlog/".to_owned()));
        vars.push(Variable::new("relay_log_index".to_owned(), VARIABLE_SCOPE_GLOBAL, false, "mysql-bin.index".to_owned()));

        vars.push(Variable::new(SESSION_CHARSET_KEY_NAME.to_owned(), VARIABLE_SCOPE_SESSION, true, "utf8".to_string()));
        vars
    };
}

pub const VARIABLE_SCOPE_GLOBAL: u8 = 1;
pub const VARIABLE_SCOPE_SESSION: u8 = 2;
pub const SESSION_CHARSET_KEY_NAME: &'static str = "__charset";

#[allow(unused)]
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

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn dynamic(&self) -> bool {
        self.dynamic
    }
}

// 
// id: @@session, @@global
// 
#[allow(unused)]
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

#[allow(unused)]
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

#[allow(unused)]
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

#[allow(unused)]
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