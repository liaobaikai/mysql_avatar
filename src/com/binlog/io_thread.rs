// 类似原生mysql的io_thread.
// 主要用于接收主库的binlog日志
// 并写到具体的relaylog目录中.

use std::time::Duration;

use anyhow::Result;
use tokio::task::JoinError;

use crate::com::query::query_sql_parse::ChangeReplicationSourceTo;

pub async fn start_io_thread(crst: &mut ChangeReplicationSourceTo) -> Result<tokio::task::JoinHandle<()>, JoinError> {
    let cloned_crst = crst.clone();
    tokio::spawn(async move {
        loop {

            println!("io_thread running....{:?}", cloned_crst);

            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }).await
}
