use connection::Connection;
use error::*;
use protocol::Protocol;

use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use raft::{eraftpb, Peer};

use futures::{
    stream::{futures_unordered, FuturesUnordered},
    Async::{NotReady, Ready},
    Future, Poll, Sink, Stream,
};

use bincode;

use tokio::{
    net::{ConnectFuture, TcpStream},
    timer::Delay,
};

use protobuf::Message;

/// Builds a connection to a peer. If the peer is not reachable, the connection will be retried
/// after 500ms.
struct BuildingConnection {
    peer_id: u64,
    peer_addr: SocketAddr,
    connect: ConnectFuture,
    timer: Option<Delay>,
}

impl<'a> From<&'a Peer> for BuildingConnection {
    fn from(peer: &'a Peer) -> BuildingConnection {
        let peer_id = peer.id;
        let peer_addr: SocketAddr = bincode::deserialize(&peer.context.as_ref().unwrap())
            .expect("Peer context needs to be its SocketAddr");
        let connect = TcpStream::connect(&peer_addr);
        BuildingConnection {
            peer_id,
            peer_addr,
            connect,
            timer: None,
        }
    }
}

impl Future for BuildingConnection {
    type Item = (u64, Connection);
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            if self.timer.is_some() {
                // TODO: Handle Err(_) case!
                match self.timer.as_mut().unwrap().poll() {
                    Ok(NotReady) => return Ok(NotReady),
                    _ => {}
                }

                self.connect = TcpStream::connect(&self.peer_addr);
                self.timer = None;
            }

            match self.connect.poll() {
                Ok(NotReady) => return Ok(NotReady),
                Ok(Ready(stream)) => return Ok(Ready((self.peer_id, Connection::from(stream)))),
                Err(_) => {
                    // wait 500ms before reconnecting
                    self.timer = Some(Delay::new(Instant::now() + Duration::from_millis(500)));
                }
            }
        }
    }
}

/// Stores all outgoing connections to other peers.
/// If a connection to a peer is lost, the connection will be reestablished.
pub struct PeerConnections {
    connections: HashMap<u64, Connection>,
    building_connections: FuturesUnordered<BuildingConnection>,
    peers: HashMap<u64, Peer>,
}

impl PeerConnections {
    pub fn new(peers: Vec<Peer>) -> PeerConnections {
        PeerConnections {
            connections: HashMap::default(),
            building_connections: futures_unordered(
                peers.iter().map(|p| BuildingConnection::from(p)),
            ),
            peers: peers.into_iter().map(|p| (p.id, p)).collect(),
        }
    }

    pub fn send_msg(&mut self, msg: eraftpb::Message) -> Result<()> {
        let peer = msg.to;
        let res = {
            let mut con = self.connections.get_mut(&peer);

            if let Some(con) = con {
                con.start_send(Protocol::Raft {
                    msg: msg.write_to_bytes()?,
                }).and_then(|_| con.poll_complete())
            } else {
                bail!("Connection to peer {}, does not exist!", peer);
            }
        };

        if res.is_err() {
            // rebuild the connection
            self.building_connections
                .push(BuildingConnection::from(self.peers.get(&peer).unwrap()));
            self.connections.remove(&peer);
        }

        res.map(|_| ())
    }

    fn poll_building_connections(&mut self) -> Poll<(), ()> {
        loop {
            let (peer, connection) = match try_ready!(self.building_connections.poll()) {
                Some(res) => res,
                None => return Ok(NotReady),
            };

            self.connections.insert(peer, connection);
        }
    }

    /// Returns the known address of a peer.
    pub fn get_addr_of_peer(&self, peer: u64) -> Option<SocketAddr> {
        self.peers
            .get(&peer)
            .map(|p| bincode::deserialize(&p.context.as_ref().unwrap()).unwrap())
    }
}

impl Future for PeerConnections {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let _ = self.poll_building_connections();
        // TODO: When `self.building_connections` are empty, the future will no be called anymore,
        // but as we are polling it directly, that should not be any problem.
        Ok(NotReady)
    }
}
