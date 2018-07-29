use connection::{Connection, ConnectionIdentifier};
use error::*;
use node::{NodeMessage, RequestIdentifier};
use protocol::Protocol;

use tokio::{
    self,
    net::{Incoming, TcpListener},
};

use raft::eraftpb::Message;

use futures::{
    sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    Async::{NotReady, Ready},
    Future, Poll, Sink, Stream,
};

use protobuf::Message as PMessage;

/// Listens for incoming connections and handles these connections.
pub struct IncomingConnections {
    incoming: Incoming,
    msg_sender: UnboundedSender<NodeMessage>,
}

impl IncomingConnections {
    pub fn create_and_spawn(
        listen_port: u16,
        msg_sender: UnboundedSender<NodeMessage>,
    ) -> Result<()> {
        let listener = TcpListener::bind(&([0, 0, 0, 0], listen_port).into())?;
        let incoming = listener.incoming();

        // TODO: Make sure that the program exits when `IncomingConnections` are closed!
        tokio::spawn(
            IncomingConnections {
                incoming,
                msg_sender,
            }.map(|v| panic!(v))
                .map_err(|e| panic!(e)),
        );
        Ok(())
    }
}

impl Future for IncomingConnections {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let new_con = match try_ready!(self.incoming.poll()) {
                Some(con) => con,
                None => return Ok(Ready(())),
            };

            let con = Connection::from(new_con);
            tokio::spawn(IncomingConnection::new(self.msg_sender.clone(), con));
        }
    }
}

/// Send incoming requests to the `Node` and send responses back over the `Connection`.
struct IncomingConnection {
    con: Connection,
    msg_sender: UnboundedSender<NodeMessage>,
    id: ConnectionIdentifier,
    request_result: (UnboundedSender<Protocol>, UnboundedReceiver<Protocol>),
}

impl IncomingConnection {
    fn new(msg_sender: UnboundedSender<NodeMessage>, con: Connection) -> IncomingConnection {
        IncomingConnection {
            msg_sender,
            con,
            request_result: unbounded(),
            id: ConnectionIdentifier::new(),
        }
    }

    /// Poll the connection.
    ///
    /// Result
    ///
    /// true - connection is still alive.
    /// false - connection closed.
    fn poll_con(&mut self) -> bool {
        loop {
            match self.con.poll() {
                Ok(NotReady) => return true,
                Ok(Ready(Some(msg))) => self.handle_incoming_msg(msg),
                Err(_) | Ok(Ready(None)) => return false,
            }
        }
    }

    fn handle_incoming_msg(&mut self, msg: Protocol) {
        match msg {
            Protocol::Request { id, data } => {
                let _ = self.msg_sender.unbounded_send(NodeMessage::Propose {
                    req: data,
                    response: self.request_result.0.clone(),
                    id: RequestIdentifier::new(id, self.id.clone()),
                });
            }
            Protocol::Raft { msg } => {
                let mut new_msg = Message::default();
                new_msg.merge_from_bytes(&msg).unwrap();

                let _ = self.msg_sender.unbounded_send(NodeMessage::Raft { msg: new_msg });
            }
            _ => {}
        }
    }

    fn poll_request_result(&mut self) -> bool {
        loop {
            match self.request_result.1.poll() {
                Ok(Ready(Some(res))) => {
                    let _ = self.con.start_send(res);
                }
                _ => break,
            }
        }
        self.con.poll_complete().is_ok()
    }
}

impl Future for IncomingConnection {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if !self.poll_con() {
            return Ok(Ready(()));
        }

        if !self.poll_request_result() {
            return Ok(Ready(()));
        }

        Ok(NotReady)
    }
}
