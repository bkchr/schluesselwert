use error::*;
use protocol::Protocol;

use tokio::net::TcpStream;

use futures::{Async::Ready, Poll, Sink, StartSend, Stream};

use std::net::Shutdown;

use bincode;

use rand::{self, Rng};

use length_delimited::Framed;

pub struct Connection {
    stream: Framed<TcpStream>,
}

impl From<TcpStream> for Connection {
    fn from(stream: TcpStream) -> Connection {
        Connection {
            stream: Framed::new(stream),
        }
    }
}

impl Stream for Connection {
    type Item = Protocol;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match try_ready!(self.stream.poll()) {
            Some(packet) => Ok(Ready(Some(bincode::deserialize(&packet)?))),
            None => Ok(Ready(None)),
        }
    }
}

impl Sink for Connection {
    type SinkItem = Protocol;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.stream
            .start_send(bincode::serialize(&item)?.into())
            .map(move |r| r.map(|_| item))
            .map_err(|e| e.into())
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.stream.poll_complete().map_err(|e| e.into())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.stream.get_mut().shutdown(Shutdown::Both);
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
                    }).map_err(|e| panic!(e)),
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
                }).map_err(|e| panic!(e)),
        ).unwrap();

        assert_eq!(msgr.unwrap(), msg);

        let (res, con) = current_thread::block_on_all(c.into_future().map_err(|e| e.0)).unwrap();
        assert_eq!(None, res);

        let con = current_thread::block_on_all(con.send(Protocol::Request {
            id: 0,
            data: vec![1, 2],
        })).unwrap();

        // Second write is an error when the connection was closed
        let res = current_thread::block_on_all(con.send(Protocol::Request {
            id: 0,
            data: vec![1, 2],
        }));
        assert!(res.is_err());
    }
}
