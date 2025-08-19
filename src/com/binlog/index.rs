use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use anyhow::{Result, anyhow};

// relaylog日志编号长度: 000001 ~ 999999
const RELAY_LOG_SUFFIX_LEN: usize = 6;

#[derive(Debug, Clone)]
// mysql-bin.index文件
pub struct BinlogIndex {
    offset: u64,
    filename: PathBuf,
    request_binlog_filename: String,
}

#[allow(unused)]
impl BinlogIndex {
    // 如果文件不存在，则直接报错
    pub fn try_from(filename: PathBuf) -> Result<Self> {
        let file = match File::open(&filename) {
            Ok(f) => f,
            Err(e) => return Err(anyhow!("File {}: {e}", filename.display())),
        };

        Ok(Self {
            offset: file.metadata()?.len(),
            filename,
            request_binlog_filename: String::new(),
        })
    }

    pub fn with_request_binlog_filename(&mut self, filename: String) {
        self.request_binlog_filename = filename;
    }

    // 监控文件是否有变化，如果有变化，则返回最后一行
    pub fn changed(&mut self) -> Result<Option<String>> {
        let file = File::open(&self.filename)?;

        let file_size = file.metadata()?.len();
        if file_size != self.offset {
            let mut buffer = BufReader::new(file);
            let mut line = String::new();
            loop {
                let mut buf = String::new();
                let n = buffer.read_line(&mut buf)?;
                if n <= 0 {
                    break;
                }
                line = buf;
            }
            line = line.trim_end_matches("\n").to_string();
            log::debug!("watch: line: {:?}", line);
            self.offset = file_size;
            Ok(Some(line))
        } else {
            Ok(None)
        }
    }

    // 获取文件中最后的一行
    pub fn last(&self) -> Result<Option<String>> {
        let file = File::open(&self.filename)?;

        let mut buffer = BufReader::new(file);

        let mut line = String::new();
        loop {
            let mut buf = String::new();
            let n = buffer.read_line(&mut buf)?;
            if n <= 0 {
                break;
            }
            line = buf;
        }
        line = line.trim_end_matches("\n").to_string();
        if line.len() > 0 {
            Ok(Some(line))
        } else {
            Ok(None)
        }
    }

    // 判断是否有下一个文件
    pub fn has_next(&self) -> Result<bool> {
        Ok(true)
    }

    // find
    pub fn find(&self) -> Result<Option<String>> {
        let file = File::open(&self.filename)?;
        let mut buffer = BufReader::new(file);
        loop {
            let mut buf = String::new();
            let n = buffer.read_line(&mut buf)?;
            if n <= 0 {
                break;
            }
            buf = buf.trim_end_matches("\n").to_string();
            if buf.ends_with(&self.request_binlog_filename) {
                return Ok(Some(buf));
            }
        }
        Ok(None)
    }

    pub fn find_next(&mut self) -> Result<Option<String>> {
        let seq: usize = match Path::new(&self.request_binlog_filename).extension() {
            Some(suffix) => suffix.to_string_lossy().parse()?,
            None => return Err(anyhow!("BinlogIndex::find_next: Filename format error")),
        };

        self.request_binlog_filename = Path::new(&self.request_binlog_filename)
            .with_extension(format!("{:>0RELAY_LOG_SUFFIX_LEN$}", seq + 1))
            .as_os_str()
            .to_string_lossy()
            .to_string();

        self.find()
    }

    // 获取文件中第一行
    pub fn first(&self) -> Result<Option<String>> {
        let file = File::open(&self.filename)?;

        let mut buffer = BufReader::new(file);
        let mut buf = String::new();
        buffer.read_line(&mut buf)?;
        buf = buf.trim_end_matches("\n").to_string();
        if buf.len() > 0 {
            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use anyhow::Result;

    use crate::{com::binlog::index::BinlogIndex, variable::get_global_var};

    #[test]
    fn test() -> Result<()> {
        let relay_log_basename = get_global_var("relay_log_basename")?;
        let relay_log_index = get_global_var("relay_log_index")?;

        let path = Path::new(relay_log_basename.value()).join(relay_log_index.value());
        let index = BinlogIndex::try_from(path.to_path_buf())?;

        println!("index: {:?}", index.last());

        Ok(())
    }

    #[test]
    fn test2() -> Result<()> {
        let a = true;
        println!("res: {}", a & true);

        Ok(())
    }
}
