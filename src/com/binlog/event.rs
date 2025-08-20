use anyhow::Result;
use bytes::BytesMut;
use mysql_async::binlog::{
    events::{BinlogEventHeader, Event, FormatDescriptionEvent, RotateEvent}, EventFlags
};
use mysql_common::proto::MySerialize;

pub trait EventExt {
    fn try_from_rotate_event(server_id: u32, rotate_event: RotateEvent) -> Result<Event>;
}

impl EventExt for Event {
    fn try_from_rotate_event(server_id: u32, rotate_event: RotateEvent) -> Result<Event> {
        let mut data_buf = vec![];
        rotate_event.serialize(&mut data_buf);
        println!("data: {:?}", data_buf);

        let event_size = (BinlogEventHeader::LEN + data_buf.len()) as u32;
        let header = BinlogEventHeader::new(
            0,
            mysql_async::binlog::EventType::ROTATE_EVENT,
            server_id,
            event_size,
            rotate_event.position() as u32,
            EventFlags::LOG_EVENT_ARTIFICIAL_F,
        );

        let mut buffer = BytesMut::new();
        let mut header_buf = vec![];
        header.serialize(&mut header_buf);
        buffer.extend_from_slice(&header_buf);
        buffer.extend_from_slice(&data_buf);

        let fde = FormatDescriptionEvent::new(mysql_async::binlog::BinlogVersion::Version4);

        Ok(Event::read(&fde, &buffer.to_vec()[..])?)
    }
}



#[cfg(test)]
mod tests{
    use std::path::Path;

    use anyhow::Result;
    use mysql_async::binlog::events::{Event, EventData, RotateEvent};

    use crate::com::binlog::event::EventExt;


    #[test]
    pub fn test() -> Result<()>{

        let name = Path::new("C:/Users/BK-liao/mysqltest/data/binlog/mysql-bin.000001").file_name().unwrap();
        let rotate_event = RotateEvent::new(4, name.as_encoded_bytes());
        let ev = Event::try_from_rotate_event(1000, rotate_event)?;

        let mut output = Vec::new();
        ev.write(mysql_async::binlog::BinlogVersion::Version4, &mut output)?;

        match ev.read_data()?.unwrap() {
            EventData::RotateEvent(ev) => {
                println!("{ev:?}")
            },
            _ => {}
        }

        println!("output: {:?}", output);

        Ok(())
    }

}