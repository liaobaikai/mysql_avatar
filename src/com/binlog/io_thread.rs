// 类似原生mysql的io_thread.
// 主要用于接收主库的binlog日志
// 并写到具体的relaylog目录中.
#[derive(Debug)]
pub struct IoWorker {
    running: bool,
}

impl IoWorker {
    pub fn new() -> Self {
        Self { running: false }
    }

    // pub async fn start(&mut self) {
    //     tokio::spawn(move || {
    //         log::debug!("IoThread:.....连接主库获取日志....");
    //     });
    // }
}
