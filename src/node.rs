use connection::ConnectionIdentifier;
use error::*;
use incoming_connections::IncomingConnections;
use peer_connections::PeerConnections;
use protocol::{Protocol, Request, RequestResult};
use storage::Storage;

use raft::{
    self,
    eraftpb::{Entry, EntryType, Message},
    raw_node::RawNode,
    Config, Ready,
};

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::Path,
    time::{Duration, Instant},
};

use tokio::timer::Interval;

use futures::{
    sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    Async, Future, Poll, Stream,
};

use bincode;

/// An identifier to uniquely identify a request of a connection.
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize, Eq, Hash)]
pub struct RequestIdentifier {
    con_id: ConnectionIdentifier,
    id: u64,
}

impl RequestIdentifier {
    pub fn new(id: u64, con_id: ConnectionIdentifier) -> RequestIdentifier {
        RequestIdentifier { id, con_id }
    }
}

/// A message for the node.
pub enum NodeMessage {
    Propose {
        req: Vec<u8>,
        response: UnboundedSender<Protocol>,
        id: RequestIdentifier,
    },
    Raft {
        msg: Message,
    },
}

pub struct Node {
    node: RawNode<Storage>,
    timer: Interval,
    recv_msgs: UnboundedReceiver<NodeMessage>,
    request_response: HashMap<RequestIdentifier, UnboundedSender<Protocol>>,
    peer_connections: PeerConnections,
    incoming_connections: IncomingConnections,
}

impl Node {
    pub fn new<T: AsRef<Path>>(
        id: u64,
        peers: Vec<Peer>,
        listen_port: u16,
        storage_path: T,
    ) -> Result<Node> {
        let storage = Storage::new(storage_path)?;
        let config = Config {
            // The unique ID for the Raft node.
            id,
            peers: vec![],
            election_tick: 10,
            heartbeat_tick: 3,
            max_size_per_msg: 1024 * 1024 * 1024,
            max_inflight_msgs: 256,
            applied: storage.get_last_applied_index(),
            tag: format!("[node {}]", id),
            ..Default::default()
        };

        let node = RawNode::new(
            &config,
            storage,
            peers.iter().map(|p| p.clone().into()).collect(),
        )?;
        let timer = Interval::new(Instant::now(), Duration::from_millis(100));
        let peer_connections = PeerConnections::new(
            peers
                .into_iter()
                .filter(|p| p.0.id != id)
                .map(|p| p.into())
                .collect(),
        );
        let (sender, recv_msgs) = unbounded();

        let incoming_connections = IncomingConnections::new(listen_port, sender)?;

        Ok(Node {
            node,
            timer,
            recv_msgs,
            request_response: HashMap::new(),
            peer_connections,
            incoming_connections,
        })
    }

    fn tick(&mut self) {
        self.node.tick();

        if self.node.has_ready() {
            self.process_ready();
        }
    }

    fn send_msgs(&mut self, ready: &mut Ready) {
        let msgs = ready.messages.drain(..);
        for msg in msgs {
            let peer = msg.to;
            if self.peer_connections.send_msg(msg).is_err() {
                self.node.report_unreachable(peer);
            }
        }
    }

    fn process_ready(&mut self) {
        let mut ready = self.node.ready();

        if self.is_leader() {
            self.send_msgs(&mut ready);
        }

        if !raft::is_empty_snap(&ready.snapshot) {
            self.node
                .mut_store()
                .apply_snapshot(&ready.snapshot)
                .unwrap();
        }

        if !ready.entries.is_empty() {
            self.node
                .mut_store()
                .append_entries(&ready.entries)
                .unwrap();
        }

        if let Some(ref hs) = ready.hs {
            self.node.mut_store().set_hard_state(hs.clone()).unwrap();
        }

        if !self.is_leader() {
            self.send_msgs(&mut ready);
        }

        if let Some(committed_entries) = ready.committed_entries.take() {
            committed_entries
                .into_iter()
                .for_each(|e| self.handle_commited_entry(e));
        }

        self.node.advance(ready);
    }

    /// Handle a committed entry.
    fn handle_commited_entry(&mut self, entry: Entry) {
        if entry.get_data().is_empty() {
            // Emtpy entry, when the peer becomes Leader it will send an empty entry.
            return;
        }

        match entry.get_entry_type() {
            EntryType::EntryNormal => {
                let request: Request = bincode::deserialize(&entry.get_data())
                    .expect("Entry data needs to be a valid `Request`!");
                let id: RequestIdentifier = bincode::deserialize(&entry.get_context())
                    .expect("Entry context needs to be a valid `RequestIdentifier`!");
                self.handle_commited_request(request, id, entry.index);
            }
            EntryType::EntryConfChange => {
                //TODO!
            }
        }
    }

    /// Handle a request that was committed.
    fn handle_commited_request(
        &mut self,
        request: Request,
        id: RequestIdentifier,
        entry_index: u64,
    ) {
        let response_sender = self.request_response.remove(&id);

        let response = match request {
            Request::Set { key, value } => RequestResult::Set {
                successful: self.node.mut_store().set(key, &value, entry_index).is_ok(),
            },
            Request::Get { key } => RequestResult::Get {
                value: self.node.mut_store().get(key, entry_index).unwrap(),
            },
            Request::Delete { key } => RequestResult::Delete {
                successful: self.node.mut_store().delete(key, entry_index).is_ok(),
            },
            Request::Scan => RequestResult::Scan {
                keys: self.node.mut_store().scan(entry_index).ok(),
            },
        };

        if let Some(sender) = response_sender {
            let _ = sender.unbounded_send(Protocol::RequestResult {
                id: id.id,
                res: response,
            });
        }
    }

    fn is_leader(&self) -> bool {
        self.node.raft.id == self.node.raft.leader_id
    }

    fn poll_recv_msgs(&mut self) -> Poll<(), ()> {
        loop {
            let msg = try_ready!(self.recv_msgs.poll())
                .expect("Not all NodeMessage senders can be dropped.");

            match msg {
                NodeMessage::Raft { msg } => {
                    self.node.step(msg).expect("step");
                }
                NodeMessage::Propose { id, req, response } => {
                    if self.is_leader() {
                        // TODO: Make sure that we do not overwrite anything!
                        self.request_response.insert(id, response);
                        self.node
                            .propose(bincode::serialize(&id).unwrap(), req)
                            .expect("propose");
                    } else {
                        let _ = response.unbounded_send(Protocol::NotLeader {
                            leader_addr: self
                                .peer_connections
                                .get_addr_of_peer(self.node.raft.leader_id),
                        });
                    }
                }
            }
        }
    }

    /// Returns the id of the leader.
    pub fn get_leader_id(&self) -> u64 {
        self.node.raft.leader_id
    }

    /// Returns the id of this `Node`.
    pub fn get_id(&self) -> u64 {
        self.node.raft.id
    }
}

impl Future for Node {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.incoming_connections.poll() {
                Ok(Async::Ready(())) => panic!("IncomingConnections ended!"),
                Err(e) => panic!("IncomingConnections ended with: {:?}", e),
                _ => {}
            };

            let _ = self.peer_connections.poll();
            let _ = self.poll_recv_msgs();

            try_ready!(self.timer.poll());

            self.tick();
        }
    }
}

/// A new type wrapper around `raft::Peer` that ensures that the `context` parameter holds the
/// `SocketAddr` of the peer.
pub struct Peer(raft::Peer);

impl Peer {
    pub fn new(id: u64, addr: SocketAddr) -> Peer {
        let addr = bincode::serialize(&addr).unwrap();
        Peer(raft::Peer {
            id,
            context: Some(addr),
        })
    }
}

impl Into<raft::Peer> for Peer {
    fn into(self) -> raft::Peer {
        self.0
    }
}

impl Clone for Peer {
    fn clone(&self) -> Self {
        let id = self.0.id;
        let context = self.0.context.clone();
        Peer(raft::Peer { id, context })
    }
}
