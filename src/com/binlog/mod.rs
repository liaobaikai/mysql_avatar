use std::io::ErrorKind::UnexpectedEof;
use std::io::Seek;
use std::path::PathBuf;
use std::thread;
use std::{
    fs::File,
    io::{self, BufRead, BufReader},
};

use anyhow::{Result, anyhow};
use bytes::{BufMut, BytesMut};
use mysql_async::binlog::{BinlogVersion, events::Event};
use mysql_common::binlog::{BinlogFileHeader, EventStreamReader};
use mysql_common::io::ParseBuf;
use mysql_common::misc::raw::RawInt;
use crate::consts::Command;
use crate::variable::get_global_var;
pub mod index;
pub mod event;

#[allow(unused)]
#[derive(Debug, Clone, Default)]
pub struct BinlogState {
    pub filename: String,
    pub pos: u64,
    pub ack: bool,
}

#[allow(unused)]
impl<'a> BinlogState {
    pub fn new(filename: String, pos: u64, ack: bool) -> Self {
        BinlogState { filename, pos, ack }
    }

    pub fn set_filename_pos(filename: String, pos: u64) {

    }
}

// 判断是否是binlog命令
pub fn match_binlog_command(input: &mut BytesMut) -> Result<Option<Command>> {
    let mut buf = ParseBuf(&input);
    let com_val: RawInt<u8> = buf.parse(())?;
    let com: Command = Command::try_from(*com_val)?;
    log::debug!("match_binlog_command::com: {:?}", com);
    match com {
        Command::COM_BINLOG_DUMP | Command::COM_BINLOG_DUMP_GTID => Ok(Some(com)),
        _ => Ok(None),
    }
}

pub fn event_to_bytes(event: Event, rpl_semi_sync_master_enabled: bool, need_ack: bool) -> BytesMut {
    let mut output = Vec::new();
    event
        .write(mysql_async::binlog::BinlogVersion::Version4, &mut output)
        .unwrap();
    let mut src = BytesMut::new();
    src.put_u8(0x00);
    if rpl_semi_sync_master_enabled {
        src.put_u8(0xef);  // 魔术字符
        if need_ack {
            src.put_u8(0x01);  // 是否需要回復ack
        } else {
            src.put_u8(0x00);
        }
    }
    
    src.extend_from_slice(&output);
    src
}

// pub fn decode_com_binlog_dump<'a>(input: &mut BytesMut) -> Result<ComBinlogDump> {
//     let com_binlog_dump = ComBinlogDump::deserialize((), &mut ParseBuf(&input))?;
//     Ok(com_binlog_dump)
// }

/// Binlog file.
///
/// It's an iterator over events in a binlog file.
#[derive(Debug)]
pub struct BinlogFile<T> {
    reader: EventStreamReader,
    read: T,
}

#[allow(unused)]
impl<T: BufRead> BinlogFile<T> {
    /// Creates a new instance.
    ///
    /// It'll try to read binlog file header.
    pub fn new(mut read: T) -> io::Result<Self> {
        BinlogFileHeader::read(&mut read)?;
        Ok(BinlogFile::from(read))
    }

    pub fn from(read: T) -> Self {
        let reader = EventStreamReader::new(BinlogVersion::Version4);
        Self { reader, read }
    }

    /// Returns a reference to the binlog stream reader.
    pub fn reader(&self) -> &EventStreamReader {
        &self.reader
    }

    /// Returns a mutable reference to the binlog stream reader.
    pub fn reader_mut(&mut self) -> &mut EventStreamReader {
        &mut self.reader
    }
}

impl<T: BufRead> Iterator for BinlogFile<T> {
    type Item = io::Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read(&mut self.read) {
            Ok(event) => event.map(Ok),
            Err(err) if err.kind() == UnexpectedEof => None,
            Err(err) => Some(Err(err)),
        }
    }
}

// pub type BinlogCallback =
//     Box<dyn Fn(BytesMut) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send>;

#[derive(Debug, Default)]
pub struct MyBinlogFileReader {
    // 当前读到文件的哪个位置
    offset: u64,
    filename: PathBuf,
    // binlog位点
    pos: u64,
    server_id: u32,
}

fn get_server_id() -> u32 {
    let var = get_global_var("server_id").unwrap();
    let server_id: u32 = var.value().parse().unwrap_or(1);
    server_id
}

#[allow(unused)]
impl<'a> MyBinlogFileReader {
    pub fn new() -> Self {
        Self { offset: 0, filename: PathBuf::new(), pos: 0, server_id: get_server_id() }
    }

    // 可以设置当前文件，用于文件切换
    pub fn with_filename(&mut self, filename: PathBuf) -> Result<()> {
        if self.filename == filename {
            return Ok(());
        }

        if let Err(e) = File::open(&filename) {
            return Err(anyhow!("File {}: {e}", filename.display()));
        }
        self.filename = filename;
        self.offset = 0;
        Ok(())
    }

    pub fn filename(&self) -> PathBuf {
        self.filename.clone()
    }

    // 可以设置当前的位点，用于ack检查
    pub fn with_pos(&mut self, pos: u64) {
        self.pos = pos;
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }

    pub fn with_server_id(&mut self, server_id: u32) {
        self.server_id = server_id;
    }

    pub fn server_id(&self) -> u32 {
        self.server_id
    }
    

    // 如果文件不存在，则直接报错
    pub fn try_from(filename: PathBuf) -> Result<Self> {
        if let Err(e) = File::open(&filename) {
            return Err(anyhow!("File {}: {e}", filename.display()));
        }

        Ok(Self {
            offset: 0,
            pos: 0,
            filename,
            server_id: get_server_id()
        })
    }

    // 监控文件是否有变化，如果有变化，则返回最后一行
    pub fn changed(&mut self) -> Result<bool> {
        let file = File::open(&self.filename)?;
        let file_size = file.metadata()?.len();
        // log::debug!("file_size: {}, self.offset: {}", file_size, self.offset);
        let changed = file_size != self.offset;
        self.offset = file_size as u64;
        Ok(changed)
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    // 读取文件到末尾
    pub fn read_all(&mut self, skip_pos: u32) -> Result<()> {
        let file = File::open(&self.filename)?;
        self.offset = file.metadata()?.len();
        let mut binlog_file = BinlogFile::new(BufReader::new(file))?;
        while let Some(event) = binlog_file.next() {
            let event = event?;
            if event.header().log_pos() < skip_pos {
                continue;
            }
            log::trace!(
                "[{:?}] event: {:?}",
                thread::current().id(),
                event.header().event_type()
            );
        }
        Ok(())
    }

    // pub async fn read_all_to_stream(
    //     &mut self,
    //     skip_pos: u32,
    //     stream: &mut TcpStream,
    //     packet_codec: &mut PacketCodec,
    // ) -> Result<()> {
        
    //     while let Some(event) = binlog_file.next() {
    //         let event = event?;
    //         if event.header().log_pos() < skip_pos {
    //             continue;
    //         }
    //         // net_write(stream, packet_codec, &mut event_to_bytes(event)).await?;
    //     }
    //     self.offset = file_size;
    //     Ok(())
    // }

    pub fn get_binlog_file(&mut self) -> Result<BinlogFile<BufReader<File>>> {
        let mut file = File::open(&self.filename)?;
        let file_size = file.metadata()?.len();
        log::debug!("get_binlog_file:: filename: {} offset: {}", self.filename.display(), self.offset);
        let binlog_file = if self.offset > 0 {
            file.seek(io::SeekFrom::Start(self.offset))?;
            BinlogFile::from(BufReader::new(file))
        } else {
            BinlogFile::new(BufReader::new(file))?
        };
        self.offset = file_size;
        Ok(binlog_file)
    }

    // pub async fn read_all_fn(&mut self, skip_pos: u32, callback: BinlogCallback) -> Result<()> {
    //     let file = File::open(&self.filename)?;
    //     self.offset = file.metadata()?.len();
    //     let mut binlog_file = BinlogFile::new(BufReader::new(file))?;
    //     while let Some(event) = binlog_file.next() {
    //         let event = event?;
    //         if event.header().log_pos() < skip_pos {
    //             continue;
    //         }
    //         callback(event_to_bytes(event)).await?
    //     }
    //     Ok(())
    // }

    // 读增量内容
    pub fn read(&mut self) -> Result<bool> {
        let mut file = File::open(&self.filename)?;
        let file_size = file.metadata()?.len();
        file.seek(io::SeekFrom::Start(self.offset))?;
        let mut binlog_file = BinlogFile::from(BufReader::new(file));
        while let Some(event) = binlog_file.next() {
            log::trace!(
                "[{:?}] event: {:?}",
                thread::current().id(),
                event?.header().event_type()
            );
        }
        let updated = file_size > self.offset;
        self.offset = file_size;
        Ok(updated)
    }

    // pub async fn read_fn<F>(&mut self, callback: F) -> Result<()>
    // where
    //     F: Fn(Event) -> Result<()>,
    // {
    //     let mut file = File::open(&self.filename)?;
    //     let file_size = file.metadata()?.len();
    //     file.seek(io::SeekFrom::Start(self.offset))?;
    //     let mut binlog_file = BinlogFile::from(BufReader::new(file));
    //     while let Some(event) = binlog_file.next() {
    //         let event = event?;
    //         println!("event: {:?}", event);
    //         callback(event)?;
    //     }
    //     self.offset = file_size;
    //     Ok(())
    // }

    //
    // 循环读全部内容，如果空闲超过30秒，则退出
    //
    // pub fn run_dumper(&mut self, pos: u32) -> Result<()> {
    //     let _ = &self.read_all(pos)?;
    //     // 如果超过10没有更新，则退出
    //     let mut try_count = 0;
    //     loop {
    //         if !self.read()? {
    //             try_count += 1;
    //         } else {
    //             try_count = 0;
    //         }
    //         println!("[{:?}] read end, sleep 3s", thread::current().id());
    //         thread::sleep(Duration::from_secs(3));
    //         if try_count >= 10 {
    //             println!(
    //                 "[{:?}] Dumper idle try_count >= {try_count}, shutdown.",
    //                 thread::current().id()
    //             );
    //             break;
    //         }
    //     }
    //     Ok(())
    // }
}

// #[cfg(test)]
// mod tests {
//     use std::path::Path;

//     use anyhow::Result;

//     use crate::com::binlog::MyBinlogFileReader;

//     #[test]
//     fn test() -> Result<()> {
//         let mut reader = MyBinlogFileReader::try_from(Path::new(
//             "/Users/lbk/mysqltest/data/relay-bin/relay-bin.000000001",
//         ))?;
//         reader.run_dumper(0)?;

//         Ok(())
//     }
// }
