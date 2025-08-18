use std::io::ErrorKind::UnexpectedEof;
use std::io::Seek;
use std::path::Path;
use std::thread;
use std::time::Duration;
use std::{
    fs::File,
    io::{self, BufRead, BufReader},
};

use anyhow::{Result, anyhow};
use bytes::BytesMut;
use mysql_async::binlog::{BinlogVersion, events::Event};
use mysql_common::binlog::{BinlogFileHeader, EventStreamReader};
use mysql_common::io::ParseBuf;
use mysql_common::misc::raw::RawInt;

use crate::com::{Dumpable, SqlCommand};
use crate::consts::Command;

impl Dumpable for SqlCommand {}

// 判断是否是binlog命令
pub fn match_binlog_command(input: &mut BytesMut) -> Result<Option<Command>> {
    let mut buf = ParseBuf(&input);
    let com_val: RawInt<u8> = buf.parse(())?;
    let com: Command = Command::try_from(*com_val)?;
    println!("match_binlog_command::com: {:?}", com);
    match com {
        Command::COM_BINLOG_DUMP | Command::COM_BINLOG_DUMP_GTID => Ok(Some(com)),
        _ => Ok(None),
    }
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

pub struct MyBinlogFileReader<'a> {
    // 当前读到文件的哪个位置
    offset: u64,
    filename: &'a Path,
}

impl<'a> MyBinlogFileReader<'a> {
    // 如果文件不存在，则直接报错
    pub fn try_from(filename: &'a Path) -> Result<Self> {
        if let Err(e) = File::open(&filename) {
            return Err(anyhow!("File {}: {e}", filename.display()));
        }

        Ok(Self {
            offset: 0,
            filename,
        })
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
            println!(
                "[{:?}] event: {:?}",
                thread::current().id(),
                event.header().event_type()
            );
        }
        Ok(())
    }

    pub async fn read_all_fn<F>(&mut self, skip_pos: u32, callback: F) -> Result<()>
    where
        F: Fn(Event) -> Result<()>,
    {
        let file = File::open(&self.filename)?;
        self.offset = file.metadata()?.len();
        let mut binlog_file = BinlogFile::new(BufReader::new(file))?;
        while let Some(event) = binlog_file.next() {
            let event = event?;
            if event.header().log_pos() < skip_pos {
                continue;
            }
            callback(event)?;
        }
        Ok(())
    }

    // 读增量内容
    pub fn read(&mut self) -> Result<bool> {
        let mut file = File::open(&self.filename)?;
        let file_size = file.metadata()?.len();
        file.seek(io::SeekFrom::Start(self.offset))?;
        let mut binlog_file = BinlogFile::from(BufReader::new(file));
        while let Some(event) = binlog_file.next() {
            println!(
                "[{:?}] event: {:?}",
                thread::current().id(),
                event?.header().event_type()
            );
        }
        let updated = file_size > self.offset;
        self.offset = file_size;
        Ok(updated)
    }

    pub async fn read_fn<F>(&mut self, callback: F) -> Result<()>
    where
        F: Fn(Event) -> Result<()>,
    {
        let mut file = File::open(&self.filename)?;
        let file_size = file.metadata()?.len();
        file.seek(io::SeekFrom::Start(self.offset))?;
        let mut binlog_file = BinlogFile::from(BufReader::new(file));
        while let Some(event) = binlog_file.next() {
            let event = event?;
            println!("event: {:?}", event);
            callback(event)?;
        }
        self.offset = file_size;
        Ok(())
    }

    //
    // 循环读全部内容，如果空闲超过30秒，则退出
    //
    pub fn run_dumper(&mut self, pos: u32) -> Result<()> {
        let _ = &self.read_all(pos)?;
        // 如果超过10没有更新，则退出
        let mut try_count = 0;
        loop {
            if !self.read()? {
                try_count += 1;
            } else {
                try_count = 0;
            }
            println!("[{:?}] read end, sleep 3s", thread::current().id());
            thread::sleep(Duration::from_secs(3));
            if try_count >= 10 {
                println!(
                    "[{:?}] Dumper idle try_count >= {try_count}, shutdown.",
                    thread::current().id()
                );
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use anyhow::Result;

    use crate::com::binlog::MyBinlogFileReader;

    #[test]
    fn test() -> Result<()> {
        let mut reader = MyBinlogFileReader::try_from(Path::new(
            "/Users/lbk/mysqltest/data/relay-bin/relay-bin.000000001",
        ))?;
        reader.run_dumper(0)?;

        Ok(())
    }
}
