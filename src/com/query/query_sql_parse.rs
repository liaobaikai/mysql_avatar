// 解析
#[derive(Debug, Clone)]
pub struct Stmt {
    sql: String,
    list: Vec<String>,
}

impl Stmt {

    pub fn new(sql: String) -> Self {
        Self { sql, list: Vec::new() }
    }

}