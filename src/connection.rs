use error::*;
use protocol::Protocol;

use tokio::net::TcpStream;

use futures::{Async::Ready, AsyncSink, Poll, Sink, StartSend, Stream};

use bytes::{Buf, BytesMut, IntoBuf};

use std::{
    io::{Read, Write},
    mem,
};

use bincode;

use byteorder::{BigEndian, ByteOrder};

use rand::{self, Rng};

pub struct Connection {
    stream: TcpStream,
    buf: Vec<u8>,
    next_packet_buf: BytesMut,
    next_packet_len: Option<u16>,
}

impl From<TcpStream> for Connection {
    fn from(stream: TcpStream) -> Connection {
        Connection {
            stream,
            buf: vec![0; 1536],
            next_packet_len: None,
            next_packet_buf: BytesMut::new(),
        }
    }
}

impl Connection {
    fn read_next_packet(&mut self) -> Poll<Option<BytesMut>, Error> {
        // TODO: optimize
        loop {
            if let Some(next_packet_len) = self.next_packet_len {
                if self.next_packet_buf.len() >= next_packet_len as usize {
                    self.next_packet_len = None;
                    let res = self.next_packet_buf.split_to(next_packet_len as usize);
                    return Ok(Ready(Some(res)));
                }

                let len = try_nb!(self.stream.read(&mut self.buf));

                // other side closed the connection
                if len == 0 {
                    return Ok(Ready(None));
                }

                self.next_packet_buf.extend_from_slice(&self.buf[..len]);
            } else {
                if self.next_packet_buf.len() >= mem::size_of::<u16>() {
                    let len = self.next_packet_buf.split_to(mem::size_of::<u16>());
                    self.next_packet_len = Some(len.into_buf().get_u16_be());
                } else {
                    let len = try_nb!(self.stream.read(&mut self.buf));

                    // other side closed the connection
                    if len == 0 {
                        return Ok(Ready(None));
                    }

                    self.next_packet_buf.extend_from_slice(&self.buf[..len]);
                }
            }
        }
    }
}

impl Stream for Connection {
    type Item = Protocol;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let packet = match try_ready!(self.read_next_packet()) {
            Some(packet) => packet,
            None => return Ok(Ready(None)),
        };

        let msg = bincode::deserialize(&packet)?;
        Ok(Ready(Some(msg)))
    }
}

impl Sink for Connection {
    type SinkItem = Protocol;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let buf = bincode::serialize(&item)?;

        let mut len: [u8; 2] = [0; 2];
        BigEndian::write_u16(&mut len, buf.len() as u16);
        self.stream.write(&len)?;
        self.stream.write(&buf)?;

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.stream.flush()?;
        Ok(Ready(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Future;
    use std::{sync::mpsc::channel, thread, time::Duration};
    use tokio::{
        self,
        executor::current_thread,
        net::{TcpListener, TcpStream},
    };

    #[test]
    fn send_msg_and_recv_msg() {
        let (sender, recv) = channel();
        thread::spawn(move || {
            let listener = TcpListener::bind(&([0, 0, 0, 0], 0).into()).unwrap();
            sender.send(listener.local_addr().unwrap()).unwrap();

            // for each incoming connection, recv a message and send this message back.
            tokio::run(
                listener
                    .incoming()
                    .for_each(|c| {
                        tokio::spawn(
                            Connection::from(c)
                                .into_future()
                                .map_err(|e| e.0)
                                .and_then(|(msg, c)| c.send(msg.unwrap()))
                                .map_err(|e| panic!(e))
                                .map(|_| ()),
                        );
                        Ok(())
                    })
                    .map_err(|e| panic!(e)),
            );
        });

        let addr = recv.recv_timeout(Duration::from_secs(1)).unwrap();
        let msg = Protocol::Request {
            id: 0,
            data: vec![0, 1, 2, 3, 5, 6],
        };
        let (msgr, c, msg) = current_thread::block_on_all(
            TcpStream::connect(&addr)
                .map_err(|e| Error::Io(e))
                .and_then(move |s| Connection::from(s).send(msg.clone()).map(move |c| (c, msg)))
                .and_then(|(c, msg)| {
                    c.into_future()
                        .map(move |(msgr, c)| (msgr, c, msg))
                        .map_err(|e| e.0)
                })
                .map_err(|e| panic!(e)),
        ).unwrap();

        assert_eq!(msgr.unwrap(), msg);

        let (res, con) = current_thread::block_on_all(c.into_future().map_err(|e| e.0)).unwrap();
        assert_eq!(None, res);

        let res = current_thread::block_on_all(con.send(Protocol::Request {
            id: 0,
            data: vec![1, 2],
        }));
        assert!(res.is_err());
    }
}

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize, Eq, Hash)]
pub struct ConnectionIdentifier {
    id: u64,
}

impl ConnectionIdentifier {
    pub fn new() -> ConnectionIdentifier {
        //TODO: Make sure that the id is unique!
        ConnectionIdentifier {
            id: rand::thread_rng().gen(),
        }
    }
}
