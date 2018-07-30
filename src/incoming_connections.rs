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
    sync::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    Async::{NotReady, Ready},
    Future, Poll, Sink, Stream,
};

use protobuf::Message as PMessage;

use std::cell::RefCell;

// TODO: Distinguishing between peer_connections and incoming connections is not the best idea!
//       Both should be handled by one handler.

/// Listens for incoming connections and handles these connections.
pub struct IncomingConnections {
    incoming: Incoming,
    msg_sender: UnboundedSender<NodeMessage>,
    /// These handles inform the `IncomingConnection` instances if the instance of
    /// `IncomingConnections` is dropped.
    incoming_handles: Vec<RefCell<IncomingConnectionHandle>>,
}

impl IncomingConnections {
    pub fn new(
        listen_port: u16,
        msg_sender: UnboundedSender<NodeMessage>,
    ) -> Result<IncomingConnections> {
        let listener = TcpListener::bind(&([0, 0, 0, 0], listen_port).into())?;
        let incoming = listener.incoming();

        Ok(IncomingConnections {
            incoming,
            msg_sender,
            incoming_handles: Vec::new(),
        })
    }
}

impl Future for IncomingConnections {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.incoming_handles
            .retain(|h| h.borrow_mut().poll().is_ok());

        loop {
            let new_con = match try_ready!(self.incoming.poll()) {
                Some(con) => con,
                None => return Ok(Ready(())),
            };

            let con = Connection::from(new_con);

            let (incoming, mut handle) = IncomingConnection::new(self.msg_sender.clone(), con);
            tokio::spawn(incoming);

            let _ = handle.poll();
            self.incoming_handles.push(RefCell::new(handle));
        }
    }
}

/// Some sort of hack to inform `IncomingConnections` or `IncomingConnection` about dropping of one
/// side. If one side is dropped, the other side will be notified by this handle.
struct IncomingConnectionHandle {
    _sender: oneshot::Sender<()>,
    recv: oneshot::Receiver<()>,
}

impl IncomingConnectionHandle {
    fn new() -> (Self, Self) {
        let (sender0, receiver0) = oneshot::channel();
        let (sender1, receiver1) = oneshot::channel();

        (
            IncomingConnectionHandle {
                _sender: sender0,
                recv: receiver1,
            },
            IncomingConnectionHandle {
                _sender: sender1,
                recv: receiver0,
            },
        )
    }
}

impl Future for IncomingConnectionHandle {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.recv.poll().map_err(|_| ()).map(|r| r.map(|_| ()))
    }
}

/// Send incoming requests to the `Node` and send responses back over the `Connection`.
struct IncomingConnection {
    con: Connection,
    msg_sender: UnboundedSender<NodeMessage>,
    id: ConnectionIdentifier,
    request_result: (UnboundedSender<Protocol>, UnboundedReceiver<Protocol>),
    handle: IncomingConnectionHandle,
}

impl IncomingConnection {
    fn new(
        msg_sender: UnboundedSender<NodeMessage>,
        con: Connection,
    ) -> (IncomingConnection, IncomingConnectionHandle) {
        let (handle0, handle1) = IncomingConnectionHandle::new();

        (
            IncomingConnection {
                msg_sender,
                con,
                request_result: unbounded(),
                id: ConnectionIdentifier::new(),
                handle: handle0,
            },
            handle1,
        )
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
                Ok(Ready(Some(msg))) => if !self.handle_incoming_msg(msg) {
                    return false;
                },
                Err(_) | Ok(Ready(None)) => return false,
            }
        }
    }

    /// Handle an incoming message.
    /// Returns false, when the `msg_sender` does not receive anymore messages.
    fn handle_incoming_msg(&mut self, msg: Protocol) -> bool {
        match msg {
            Protocol::Request { id, data } => self
                .msg_sender
                .unbounded_send(NodeMessage::Propose {
                    req: data,
                    response: self.request_result.0.clone(),
                    id: RequestIdentifier::new(id, self.id.clone()),
                }).is_ok(),
            Protocol::Raft { msg } => {
                let mut new_msg = Message::default();
                new_msg.merge_from_bytes(&msg).unwrap();

                self.msg_sender
                    .unbounded_send(NodeMessage::Raft { msg: new_msg })
                    .is_ok()
            }
            _ => true,
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
        if self.handle.poll().is_err() {
            return Ok(Ready(()));
        }

        if !self.poll_con() {
            return Ok(Ready(()));
        }

        if !self.poll_request_result() {
            return Ok(Ready(()));
        }

        Ok(NotReady)
    }
}
