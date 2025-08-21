use anyhow::Result;

// 简单粗暴的解析
// 
#[derive(Debug, Clone)]
pub struct StatementExt {
    sql: String,
    list: Vec<String>,
}

impl StatementExt {

    pub fn new(sql: String) -> Self {
        Self { sql, list: Vec::new() }
    }

    

}

// 
pub fn parse_sql(sql: &str) -> Result<StatementExt> {

    for id in sql.split_whitespace() {
        println!("id: `{id}`");
    }

    Ok(StatementExt::new(sql.to_string()))
}


#[cfg(test)]
mod tests {
    use anyhow::Result;
    use sqlparser::{dialect::MySqlDialect, parser::Parser};

    use crate::com::query::query_sql_parse::parse_sql;

    
    #[test]
    fn test() -> Result<()>{

let data = "
CHANGE MASTER TO
  MASTER_HOST='172.24.224.1',
  MASTER_USER='root',
  MASTER_PASSWORD='123456',
  MASTER_PORT=8080,
  MASTER_LOG_FILE='mysql-bin.000001', MASTER_LOG_POS=4,
  MASTER_CONNECT_RETRY=3000000
";

        let mut sql = Parser::new(&MySqlDialect{}).try_with_sql(data)?;

        println!("token: {:?}", sql.next_token().token);
        println!("token: {:?}", sql.next_token());
        println!("token: {:?}", sql.next_token());
        println!("token: {:?}", sql.next_token());
        println!("token: {:?}", sql.next_token());
        println!("token: {:?}", sql.next_token());
        println!("token: {:?}", sql.next_token());

        // let _ = parse_sql(data)?;

        Ok(())
    }
}