use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::TcpStream,
};

//
// Connection for representing a TCP connection.
// It is owned by the poller and handles non-blocking reads and partial writes.
//

pub type ConnId = u64;

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
        match self.stream.read(buf) {
            Ok(0) => ReadOutcome::Eof,
            Ok(n) => ReadOutcome::Data(n),
            Err(ref e) => {
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::Interrupted {
                    ReadOutcome::Idle
                } else {
                    ReadOutcome::Eof
                }
            }
        }
    }

    pub fn flush(&mut self) -> io::Result<bool> {
        while !self.write_buf.is_empty() {
            let n = {
                let (head, _) = self.write_buf.as_slices();
                match self.stream.write(head) {
                    Ok(0) => return Ok(true),
                    Ok(n) => n,
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(true),
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            };
            self.write_buf.drain(..n);
        }
        Ok(false)
    }
}
