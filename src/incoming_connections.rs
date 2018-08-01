use connection::{Connection, ConnectionIdentifier};
use error::*;
use node::{NodeMessage, RequestIdentifier};
use protocol::{Protocol, RequestChangeConf};

use tokio::{
    self,
    net::{Incoming, TcpListener},
};

use raft::eraftpb::{ConfChange, ConfChangeType, Message};

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
    incoming: Option<Incoming>,
    msg_sender: UnboundedSender<NodeMessage>,
    /// These handles inform the `IncomingConnection` instances if the instance of
    /// `IncomingConnections` is dropped.
    incoming_handles: Vec<RefCell<IncomingConnectionHandle>>,
    shutting_down: bool,
    send_incoming_peer_con_closed: UnboundedSender<u64>,
}

impl IncomingConnections {
    pub fn new(
        listen_port: u16,
        msg_sender: UnboundedSender<NodeMessage>,
        send_incoming_peer_con_closed: UnboundedSender<u64>,
    ) -> Result<IncomingConnections> {
        let listener = TcpListener::bind(&([0, 0, 0, 0], listen_port).into())?;
        let incoming = listener.incoming();

        Ok(IncomingConnections {
            incoming: Some(incoming),
            msg_sender,
            incoming_handles: Vec::new(),
            shutting_down: false,
            send_incoming_peer_con_closed,
        })
    }

    /// Shutdown the instance. It will return `Ok(Ready(()))` at `poll()`, if everything was
    /// shutdown correctly.
    pub fn shutdown(&mut self) {
        self.shutting_down = true;
        self.incoming_handles
            .iter()
            .for_each(|h| h.borrow_mut().shutdown());
        self.incoming.take();
    }
}

impl Future for IncomingConnections {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.incoming_handles.retain(|h| {
            h.borrow_mut()
                .poll()
                .map(|r| r.is_not_ready())
                .unwrap_or(false)
        });

        if self.shutting_down && self.incoming_handles.is_empty() {
            return Ok(Ready(()));
        }

        while !self.shutting_down {
            let new_con = match try_ready!(self.incoming.as_mut().unwrap().poll()) {
                Some(con) => con,
                None => return Ok(Ready(())),
            };

            let con = Connection::from(new_con);

            let (incoming, mut handle) = IncomingConnection::new(
                self.msg_sender.clone(),
                con,
                self.send_incoming_peer_con_closed.clone(),
            );
            tokio::spawn(incoming);

            let _ = handle.poll();
            self.incoming_handles.push(RefCell::new(handle));
        }

        Ok(NotReady)
    }
}

/// Some sort of hack to inform `IncomingConnections` or `IncomingConnection` about dropping of one
/// side. If one side is dropped, the other side will be notified by this handle.
struct IncomingConnectionHandle {
    sender: Option<oneshot::Sender<()>>,
    recv: oneshot::Receiver<()>,
}

impl IncomingConnectionHandle {
    fn new() -> (Self, Self) {
        let (sender0, receiver0) = oneshot::channel();
        let (sender1, receiver1) = oneshot::channel();

        (
            IncomingConnectionHandle {
                sender: Some(sender0),
                recv: receiver1,
            },
            IncomingConnectionHandle {
                sender: Some(sender1),
                recv: receiver0,
            },
        )
    }

    /// Shutdown the connected `IncomingConnection`.
    fn shutdown(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(());
        }
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
    send_incoming_peer_con_closed: UnboundedSender<u64>,
    /// If this connection comes from another peer, this is the peer id of the other peer.
    peer_id: Option<u64>,
}

impl IncomingConnection {
    fn new(
        msg_sender: UnboundedSender<NodeMessage>,
        con: Connection,
        send_incoming_peer_con_closed: UnboundedSender<u64>,
    ) -> (IncomingConnection, IncomingConnectionHandle) {
        let (handle0, handle1) = IncomingConnectionHandle::new();

        (
            IncomingConnection {
                msg_sender,
                con,
                request_result: unbounded(),
                id: ConnectionIdentifier::new(),
                handle: handle0,
                send_incoming_peer_con_closed,
                peer_id: None,
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
                })
                .is_ok(),
            Protocol::Raft { msg } => {
                let mut new_msg = Message::default();
                new_msg.merge_from_bytes(&msg).unwrap();

                self.msg_sender
                    .unbounded_send(NodeMessage::Raft { msg: new_msg })
                    .is_ok()
            }
            Protocol::RequestChangeConf { id, req } => {
                let mut conf_change = ConfChange::new();

                let node_addr = match req {
                    RequestChangeConf::AddNode { node_id, node_addr } => {
                        conf_change.set_node_id(node_id);
                        conf_change.set_change_type(ConfChangeType::AddNode);
                        Some(node_addr)
                    }
                    RequestChangeConf::RemoveNode { node_id } => {
                        conf_change.set_node_id(node_id);
                        conf_change.set_change_type(ConfChangeType::RemoveNode);
                        None
                    }
                };

                self.msg_sender
                    .unbounded_send(NodeMessage::ProposeConfChange {
                        req: conf_change,
                        node_addr,
                        response: self.request_result.0.clone(),
                        id: RequestIdentifier::new(id, self.id.clone()),
                    })
                    .is_ok()
            }
            Protocol::PeerHello { id } => {
                self.peer_id = Some(id);
                true
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

    fn inform_peer_con_closed(&mut self) {
        if let Some(id) = self.peer_id {
            let _ = self.send_incoming_peer_con_closed.unbounded_send(id);
        }
    }
}

impl Future for IncomingConnection {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.handle.poll().map(|r| r.is_ready()).unwrap_or(true) {
            self.inform_peer_con_closed();
            return Ok(Ready(()));
        }

        if !self.poll_con() {
            self.inform_peer_con_closed();
            return Ok(Ready(()));
        }

        if !self.poll_request_result() {
            self.inform_peer_con_closed();
            return Ok(Ready(()));
        }

        Ok(NotReady)
    }
}
