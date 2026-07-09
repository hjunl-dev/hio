use std::{collections::VecDeque, io, net::TcpStream};

//
// Connection for representing a TCP connection.
// It is owned by the poller and handles non-blocking reads and partial writes.
//

pub enum ReadOutcome {
    Data(usize),
    Idle,
    Eof,
}

pub struct Connection {
    stream: TcpStream,
    write_buf: VecDeque<u8>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Self {
            stream,
            write_buf: VecDeque::new(),
        })
    }

    pub fn write_payload(&mut self, data: Vec<u8>) {
        self.write_buf.extend(data);
    }

    pub fn try_read(&mut self, buf: &mut [u8]) -> ReadOutcome {
        todo!()
    }

    pub fn flush(&mut self) -> io::Result<bool> {
        todo!()
    }
}
