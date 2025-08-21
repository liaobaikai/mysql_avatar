use anyhow::Result;
use sqlparser::{dialect::MySqlDialect, parser::Parser, tokenizer::{Token, Word}};

// 1, SHOW MASTER STATUS | SHOW BINARY LOG STATUS
// 2,
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[repr(u8)]
pub enum ReplicaStatement {
    // https://dev.mysql.com/doc/refman/8.0/en/start-slave.html
    // alias: start_slave
    StartReplica(StartReplica),

    // https://dev.mysql.com/doc/refman/8.0/en/stop-slave.html
    StopReplica,

    //
    ResetReplica,

    // CHANGE MASTER TO
    // https://dev.mysql.com/doc/refman/5.7/en/change-master-to.html
    // change_master_to: bool,
    // CHANGE REPLICATION SOURCE TO
    // https://dev.mysql.com/doc/refman/8.0/en/change-replication-source-to.html
    // Alias: Change master to
    ChangeReplicationSourceTo(ChangeReplicationSourceTo),

    // https://dev.mysql.com/doc/refman/5.7/en/change-replication-filter.html
    ChangeReplicationFilter,

    Unknown,
}

// impl ReplicaStatement {
//     pub fn with_change_replication_source_to(&mut self, change_replication_source_to: bool) {
//         self.change_replication_source_to = change_replication_source_to;
//     }

//     pub fn with_crst(&mut self, crst: ChangeReplicationSourceTo) {
//         self.crst = crst;
//     }

//     pub fn with_start_replica(&mut self, start_replica: bool) {
//         self.start_replica = start_replica;
//     }

//     pub fn with_stop_replica(&mut self, stop_replica: bool) {
//         self.stop_replica = stop_replica;
//     }

//     pub fn with_reset_replica(&mut self, reset_replica: bool) {
//         self.reset_replica = reset_replica;
//     }
// }

// https://dev.mysql.com/doc/refman/8.4/en/start-replica.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct StartReplica {
    io_thread: bool,
    sql_thread: bool,

    // 未适配
    #[allow(unused)]
    until_option: Option<StartReplicaUntilOption>,
    // 未适配
    #[allow(unused)]
    conn_option: Option<StartReplicaConnOptions>,
    // 未适配
    #[allow(unused)]
    for_channel: Option<String>,
}

impl StartReplica {
    pub fn new() -> Self {
        Self {
            io_thread: true,
            sql_thread: true,
            until_option: None,
            conn_option: None,
            for_channel: None,
        }
    }

    pub fn with_io_thread(&mut self, io_thread: bool) {
        self.io_thread = io_thread;
    }

    pub fn io_thread(&self) -> bool {
        self.io_thread
    }

    pub fn with_sql_thread(&mut self, sql_thread: bool) {
        self.sql_thread = sql_thread;
    }

    // 不支持
    // pub fn sql_thread(&self) -> bool {
    //     self.sql_thread
    // }

    pub fn with_for_channel(&mut self, for_channel: Option<String>) {
        self.for_channel = for_channel;
    }

    pub fn for_channel(&self) -> &Option<String> {
        &self.for_channel
    }
}

// 未适配
#[allow(unused)]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct StartReplicaUntilOption {
    sql_before_gtids: Option<String>,
    sql_after_gtids: Option<String>,

    source_log_file: String,
    source_log_pos: u64,
    relay_log_file: Option<String>,
    relay_log_pos: Option<u64>,

    sql_after_mts_gaps: bool,
}

// 未适配
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct StartReplicaConnOptions {
    user: String,
    password: String,
    default_auth: String,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct ChangeReplicationSourceTo {
    // source_bind: String,
    source_host: String,
    source_user: String,
    source_password: String,
    source_port: u16,
    privilege_checks_user: String,
    require_row_format: usize,
    //   | require_table_primary_key_check = {stream | on | off | generate}
    //   | assign_gtids_to_anonymous_transactions = {off | local | uuid}
    source_log_file: String,
    source_log_pos: u64,
    source_auto_position: usize,
    //   | relay_log_file = 'relay_log_name'
    //   | relay_log_pos = relay_log_pos
    source_heartbeat_period: usize,
    source_connect_retry: usize,
    source_retry_count: usize,
    //   | source_connection_auto_failover = {0|1}
    source_delay: usize,
    //   | source_compression_algorithms = 'algorithm[,algorithm][,algorithm]'
    //   | source_zstd_compression_level = level
    //   | source_ssl = {0|1}
    //   | source_ssl_ca = 'ca_file_name'
    //   | source_ssl_capath = 'ca_directory_name'
    //   | source_ssl_cert = 'cert_file_name'
    //   | source_ssl_crl = 'crl_file_name'
    //   | source_ssl_crlpath = 'crl_directory_name'
    //   | source_ssl_key = 'key_file_name'
    //   | source_ssl_cipher = 'cipher_list'
    //   | source_ssl_verify_server_cert = {0|1}
    //   | source_tls_version = 'protocol_list'
    //   | source_tls_ciphersuites = 'ciphersuite_list'
    //   | source_public_key_path = 'key_file_name'
    //   | get_source_public_key = {0|1}
    //   | network_namespace = 'namespace'
    //   | ignore_server_ids = (server_id_list),
    //   | gtid_only = {0|1}
    for_channel: Option<String>,
}

impl ChangeReplicationSourceTo {
    pub fn new() -> Self {
        Self {
            source_host: "".to_owned(),
            source_user: "".to_owned(),
            source_password: "".to_owned(),
            source_port: 3306,
            privilege_checks_user: "".to_owned(),
            require_row_format: 0,
            source_log_file: "".to_owned(),
            source_log_pos: 0,
            source_auto_position: 0,
            source_heartbeat_period: 1000_000_000,
            source_connect_retry: 10,
            source_retry_count: 10,
            source_delay: 0,
            for_channel: None,
        }
    }

    pub fn with_source_host(&mut self, source_host: String) {
        self.source_host = source_host;
    }

    pub fn source_host(&self) -> &String {
        &self.source_host
    }

    pub fn with_source_user(&mut self, source_user: String) {
        self.source_user = source_user;
    }

    pub fn source_user(&self) -> &String {
        &self.source_user
    }

    pub fn with_source_password(&mut self, source_password: String) {
        self.source_password = source_password;
    }

    pub fn source_password(&self) -> &String {
        &self.source_password
    }

    pub fn with_source_port(&mut self, source_port: u16) {
        self.source_port = source_port;
    }

    pub fn source_port(&self) -> u16 {
        self.source_port
    }

    pub fn with_source_log_file(&mut self, source_log_file: String) {
        self.source_log_file = source_log_file;
    }

    pub fn source_log_file(&self) -> &String {
        &self.source_log_file
    }

    pub fn with_source_log_pos(&mut self, source_log_pos: u64) {
        self.source_log_pos = source_log_pos;
    }

    pub fn source_log_pos(&self) -> u64 {
        self.source_log_pos
    }

    pub fn with_source_auto_position(&mut self, source_auto_position: usize) {
        self.source_auto_position = source_auto_position;
    }

    pub fn source_auto_position(&self) -> usize {
        self.source_auto_position
    }

    pub fn with_source_heartbeat_period(&mut self, source_heartbeat_period: usize) {
        self.source_heartbeat_period = source_heartbeat_period;
    }

    pub fn source_heartbeat_period(&self) -> usize {
        self.source_heartbeat_period
    }

    pub fn with_source_connect_retry(&mut self, source_connect_retry: usize) {
        self.source_connect_retry = source_connect_retry;
    }

    pub fn source_connect_retry(&self) -> usize {
        self.source_connect_retry
    }

    pub fn with_source_retry_count(&mut self, source_retry_count: usize) {
        self.source_retry_count = source_retry_count;
    }

    pub fn source_retry_count(&self) -> usize {
        self.source_retry_count
    }

    pub fn with_source_delay(&mut self, source_delay: usize) {
        self.source_delay = source_delay;
    }

    pub fn source_delay(&self) -> usize {
        self.source_delay
    }

    pub fn with_for_channel(&mut self, for_channel: Option<String>) {
        self.for_channel = for_channel;
    }

    pub fn for_channel(&self) -> &Option<String> {
        &self.for_channel
    }
}

// pub enum RequireTablePrimaryKeyCheck {}

// 支持扩充sql语法
pub fn parse_sql_ext(sql: &str) -> Result<ReplicaStatement> {

    let mut sql = Parser::new(&MySqlDialect {}).try_with_sql(sql)?;
    let mut crst = ChangeReplicationSourceTo::new();
    let mut sr = StartReplica::new();
    let mut rs = ReplicaStatement::Unknown;
    let mut key = String::new();
    loop {
        let token = sql.next_token();
        match token.token {
            Token::Word(Word {
                value,
                quote_style: _,
                keyword: _,
            }) => {
                key.push_str(&value.to_lowercase());
                println!("key:: {key}");
                match key.as_str() {
                    "changereplicationsourceto" | "changemasterto" => {
                        rs = ReplicaStatement::ChangeReplicationSourceTo(
                            ChangeReplicationSourceTo::new(),
                        );
                        key.clear();
                        continue;
                    }
                    "startslave" | "startreplica" => {
                        rs = ReplicaStatement::StartReplica(StartReplica::new());
                    }
                    "stopslave" | "stopreplica" => {
                        rs = ReplicaStatement::StopReplica;
                    }
                    "resetslave" | "resetreplica" => {
                        rs = ReplicaStatement::ResetReplica;
                    }
                    "startslaveio_thread" | "startreplicaio_thread" => {
                        sr.with_io_thread(true);
                        sr.with_sql_thread(false);
                    }
                    _ => {
                        if key.starts_with("forchannel") {
                            crst.with_for_channel(Some(value.clone()));
                            sr.with_for_channel(Some(value));
                            key.clear();
                            continue;
                        }
                    }
                }
            }
            Token::Eq => {
                key.push_str("=");
            }
            Token::SingleQuotedString(value) | Token::DoubleQuotedString(value) => {
                match key.as_str() {
                    "master_host=" | "source_host=" => {
                        crst.with_source_host(value);
                    }
                    "master_user=" | "source_user=" => {
                        crst.with_source_user(value);
                    }
                    "master_password=" | "source_password=" => {
                        crst.with_source_password(value);
                    }
                    "master_port=" | "source_port=" => {
                        crst.with_source_port(value.parse::<u16>()?);
                    }
                    "master_log_file=" | "source_log_file=" => {
                        crst.with_source_log_file(value);
                    }
                    "master_log_pos=" | "source_log_pos=" => {
                        crst.with_source_log_pos(value.parse::<u64>()?);
                    }
                    "master_auto_position=" | "source_auto_position=" => {
                        crst.with_source_auto_position(value.parse::<usize>()?);
                    }
                    "master_heartbeat_period=" | "source_heartbeat_period=" => {
                        crst.with_source_heartbeat_period(value.parse::<usize>()?);
                    }
                    "master_connect_retry=" | "source_connect_retry=" => {
                        crst.with_source_connect_retry(value.parse::<usize>()?);
                    }
                    "master_retry_count=" | "source_retry_count=" => {
                        crst.with_source_retry_count(value.parse::<usize>()?);
                    }
                    "master_delay=" | "source_delay=" => {
                        crst.with_source_delay(value.parse::<usize>()?);
                    }
                    _ => {}
                }
            }
            Token::Comma => {
                key.clear();
            }
            Token::EOF => {
                match rs {
                    ReplicaStatement::ChangeReplicationSourceTo(_) => {
                        rs = ReplicaStatement::ChangeReplicationSourceTo(crst);
                    }
                    ReplicaStatement::StartReplica(_) => {
                        rs = ReplicaStatement::StartReplica(sr);
                    }
                    _ => {}
                }
                break;
            }
            _ => {}
        }
    }

    Ok(rs)
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use sqlparser::{
        dialect::MySqlDialect,
        parser::Parser,
        tokenizer::{Token, Word},
    };

    use crate::com::query::query_sql_parse::{
        ChangeReplicationSourceTo, ReplicaStatement, StartReplica,
    };

    // use crate::com::query::query_sql_parse::parse_sql;

    #[test]
    fn test() -> Result<()> {
        let data = "
change master to
  master_host='172.24.224.1',
  master_user='root',
  master_password='123456',
  master_port=8080,
  master_log_file='mysql-bin.000001', master_log_pos=4,
  master_connect_retry=3000000
";

        let data = "
CHANGE REPLICATION SOURCE TO 
SOURCE_USER='rpl_user', 
SOURCE_PASSWORD='123456', 
SOURCE_HOST='mysql-02',
SOURCE_PORT=3306,
SOURCE_RETRY_COUNT=2,
SOURCE_CONNECTION_AUTO_FAILOVER=1,
SOURCE_AUTO_POSITION=1 
For CHANNEL 'ch1'
";

        let data = "
reset slave";

        let mut sql = Parser::new(&MySqlDialect {}).try_with_sql(data)?;
        let mut crst = ChangeReplicationSourceTo::new();
        let mut sr = StartReplica::new();
        let mut rs = ReplicaStatement::Unknown;
        let mut key = String::new();
        loop {
            let token = sql.next_token();
            match token.token {
                Token::Word(Word {
                    value,
                    quote_style: _,
                    keyword: _,
                }) => {
                    key.push_str(&value.to_lowercase());
                    println!("key:: {key}");
                    match key.as_str() {
                        "changereplicationsourceto" | "changemasterto" => {
                            rs = ReplicaStatement::ChangeReplicationSourceTo(
                                ChangeReplicationSourceTo::new(),
                            );
                            key.clear();
                            continue;
                        }
                        "startslave" | "startreplica" => {
                            rs = ReplicaStatement::StartReplica(StartReplica::new());
                        }
                        "stopslave" | "stopreplica" => {
                            rs = ReplicaStatement::StopReplica;
                        }
                        "resetslave" | "resetreplica" => {
                            rs = ReplicaStatement::ResetReplica;
                        }
                        "startslaveio_thread" | "startreplicaio_thread" => {
                            sr.with_io_thread(true);
                            sr.with_sql_thread(false);
                        }
                        _ => {
                            if key.starts_with("forchannel") {
                                crst.with_for_channel(Some(value.clone()));
                                sr.with_for_channel(Some(value));
                                key.clear();
                                continue;
                            }
                        }
                    }
                }
                Token::Eq => {
                    key.push_str("=");
                }
                Token::SingleQuotedString(value) | Token::DoubleQuotedString(value) => {
                    match key.as_str() {
                        "master_host=" | "source_host=" => {
                            crst.with_source_host(value);
                        }
                        "master_user=" | "source_user=" => {
                            crst.with_source_user(value);
                        }
                        "master_password=" | "source_password=" => {
                            crst.with_source_password(value);
                        }
                        "master_port=" | "source_port=" => {
                            crst.with_source_port(value.parse::<u16>()?);
                        }
                        "master_log_file=" | "source_log_file=" => {
                            crst.with_source_log_file(value);
                        }
                        "master_log_pos=" | "source_log_pos=" => {
                            crst.with_source_log_pos(value.parse::<u64>()?);
                        }
                        "master_auto_position=" | "source_auto_position=" => {
                            crst.with_source_auto_position(value.parse::<usize>()?);
                        }
                        "master_heartbeat_period=" | "source_heartbeat_period=" => {
                            crst.with_source_heartbeat_period(value.parse::<usize>()?);
                        }
                        "master_connect_retry=" | "source_connect_retry=" => {
                            crst.with_source_connect_retry(value.parse::<usize>()?);
                        }
                        "master_retry_count=" | "source_retry_count=" => {
                            crst.with_source_retry_count(value.parse::<usize>()?);
                        }
                        "master_delay=" | "source_delay=" => {
                            crst.with_source_delay(value.parse::<usize>()?);
                        }
                        _ => {}
                    }
                }
                Token::Comma => {
                    key.clear();
                }
                Token::EOF => {
                    match rs {
                        ReplicaStatement::ChangeReplicationSourceTo(_) => {
                            rs = ReplicaStatement::ChangeReplicationSourceTo(crst);
                        }
                        ReplicaStatement::StartReplica(_) => {
                            rs = ReplicaStatement::StartReplica(sr);
                        }
                        _ => {}
                    }
                    break;
                }
                _ => {}
            }
        }
        // rs.with_crst(crst);

        println!("rs: {:?}", rs);

        // println!("token: {:?}", sql.next_token().token);
        // println!("token: {:?}", sql.next_token());
        // println!("token: {:?}", sql.next_token());
        // println!("token: {:?}", sql.next_token());
        // println!("token: {:?}", sql.next_token());
        // println!("token: {:?}", sql.next_token());
        // println!("token: {:?}", sql.next_token());

        // let _ = parse_sql(data)?;

        Ok(())
    }
}
