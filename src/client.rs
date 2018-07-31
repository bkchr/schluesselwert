use connection::Connection;
use error::*;
use protocol::{Protocol, Request, RequestChangeConf, RequestResult};

use std::{collections::HashMap, net::SocketAddr, thread};

use tokio::{
    self,
    net::{ConnectFuture, TcpStream},
};

use futures::{
    sync::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    Async::{NotReady, Ready},
    Future, Poll, Sink, Stream,
};

use bincode;

use rand::{self, Rng};

//TODO: Make requests timeout
pub struct Client {
    send_req: UnboundedSender<ClusterRequest>,
}

impl Client {
    pub fn new(nodes: Vec<SocketAddr>) -> Client {
        let send_req = ClusterConnection::create_and_run(nodes);

        Client { send_req }
    }

    /// Set a key and value.
    pub fn set<K: Into<Vec<u8>>, V: Into<Vec<u8>>>(
        &mut self,
        key: K,
        value: V,
    ) -> impl Future<Item = (), Error = Error> {
        let (sender, receiver) = oneshot::channel();

        let req = Request::Set {
            key: key.into(),
            value: value.into(),
        };

        let _ = self
            .send_req
            .unbounded_send(ClusterRequest::Request { req, sender });

        receiver
            .map_err(|e| Error::from(e))
            .flatten()
            .and_then(|rr| match rr {
                RequestResult::Set { successful } => if successful {
                    Ok(())
                } else {
                    Err(Error::RequestNotSuccessful)
                },
                r @ _ => Err(Error::IncorrectRequestResult(r)),
            })
    }

    /// Get the value for a key.
    pub fn get<K: Into<Vec<u8>>, V: From<Vec<u8>>>(
        &mut self,
        key: K,
    ) -> impl Future<Item = Option<V>, Error = Error> {
        let (sender, receiver) = oneshot::channel();
        let req = Request::Get { key: key.into() };

        let _ = self
            .send_req
            .unbounded_send(ClusterRequest::Request { req, sender });

        receiver
            .map_err(|e| Error::from(e))
            .flatten()
            .and_then(|rr| match rr {
                RequestResult::Get { value } => Ok(value.map(|v| v.into())),
                r @ _ => Err(Error::IncorrectRequestResult(r)),
            })
    }

    /// Delete key and value.
    pub fn delete<K: Into<Vec<u8>>>(&mut self, key: K) -> impl Future<Item = (), Error = Error> {
        let (sender, receiver) = oneshot::channel();
        let req = Request::Delete { key: key.into() };

        let _ = self
            .send_req
            .unbounded_send(ClusterRequest::Request { req, sender });

        receiver
            .map_err(|e| Error::from(e))
            .flatten()
            .and_then(|rr| match rr {
                RequestResult::Delete { successful } => if successful {
                    Ok(())
                } else {
                    Err(Error::RequestNotSuccessful)
                },
                r @ _ => Err(Error::IncorrectRequestResult(r)),
            })
    }

    /// Scan for all keys.
    pub fn scan<K: From<Vec<u8>>>(&mut self) -> impl Future<Item = Vec<K>, Error = Error> {
        let (sender, receiver) = oneshot::channel();
        let req = Request::Scan;
        let _ = self
            .send_req
            .unbounded_send(ClusterRequest::Request { req, sender });

        receiver
            .map_err(|e| Error::from(e))
            .flatten()
            .and_then(|rr| match rr {
                RequestResult::Scan { keys } => if let Some(keys) = keys {
                    Ok(keys.into_iter().map(|k| k.into()).collect())
                } else {
                    Err(Error::RequestNotSuccessful)
                },
                r @ _ => Err(Error::IncorrectRequestResult(r)),
            })
    }

    /// Add a node to the cluster.
    pub fn add_node_to_cluster(
        &mut self,
        node_id: u64,
        node_addr: SocketAddr,
    ) -> impl Future<Item = (), Error = Error> {
        let (sender, receiver) = oneshot::channel();
        let req = RequestChangeConf::AddNode { node_id, node_addr };

        let _ = self
            .send_req
            .unbounded_send(ClusterRequest::ChangeConf { req, sender });

        receiver.map_err(|e| Error::from(e)).flatten()
    }

    /// Remove a node from the cluster.
    pub fn remove_node_from_cluster(
        &mut self,
        node_id: u64,
    ) -> impl Future<Item = (), Error = Error> {
        let (sender, receiver) = oneshot::channel();
        let req = RequestChangeConf::RemoveNode { node_id };

        let _ = self
            .send_req
            .unbounded_send(ClusterRequest::ChangeConf { req, sender });

        receiver.map_err(|e| Error::from(e)).flatten()
    }
}

enum ClusterRequest {
    Request {
        req: Request,
        sender: oneshot::Sender<Result<RequestResult>>,
    },
    ChangeConf {
        req: RequestChangeConf,
        sender: oneshot::Sender<Result<()>>,
    },
}

enum ClusterRequestReponseSender {
    Request(oneshot::Sender<Result<RequestResult>>),
    ChangeConf(oneshot::Sender<Result<()>>),
}

struct ClusterConnection {
    /// All known cluster nodes.
    nodes: Vec<SocketAddr>,
    /// Stores a connection that is build to the cluster.
    connect: Option<ConnectFuture>,
    /// Stores the active connection to the cluster.
    connection: Option<Connection>,
    recv_send_req: UnboundedReceiver<ClusterRequest>,
    /// The id of the next request.
    next_request_id: u64,
    /// All requests that are currently active (not answered by the cluster).
    active_requests: HashMap<u64, (Protocol, ClusterRequestReponseSender)>,
}

impl ClusterConnection {
    /// Creates a `ClusterConnection` instances and runs it in a thread.
    fn create_and_run(nodes: Vec<SocketAddr>) -> UnboundedSender<ClusterRequest> {
        let (sender, recv_send_req) = unbounded();

        thread::spawn(move || {
            let inst = ClusterConnection {
                nodes,
                connect: None,
                connection: None,
                recv_send_req,
                next_request_id: 0,
                active_requests: HashMap::new(),
            };
            tokio::run(inst.map_err(|e| panic!(e)));
        });

        sender
    }

    /// Create a connection to the cluster.
    /// leader - The address of leader, if no address is given, a random one is chosen from the
    ///          nodes list.
    fn connect_to_cluster(&mut self, leader: Option<SocketAddr>) {
        let addr = if let Some(ref leader) = leader {
            // Add leader to nodes list, if it was not known before.
            if !self.nodes.contains(leader) {
                self.nodes.push(leader.clone());
            }

            leader
        } else {
            rand::thread_rng().choose(&self.nodes).unwrap()
        };

        self.connect = Some(TcpStream::connect(addr));
        self.connection = None;
    }

    fn poll_connect(&mut self) -> Poll<(), Error> {
        loop {
            match self
                .connect
                .as_mut()
                .expect("poll_connect() should only be called while connecting")
                .poll()
            {
                Ok(Ready(con)) => {
                    self.connection = Some(Connection::from(con));
                    self.connect = None;
                    self.resend_active_requests();
                    return Ok(Ready(()));
                }
                Ok(NotReady) => {
                    return Ok(NotReady);
                }
                Err(_) => self.connect_to_cluster(None),
            }
        }
    }

    /// Resends all active requests. That is important if the connection was not to the cluster
    /// leader and thus we need to resend all the data to the leader.
    fn resend_active_requests(&mut self) {
        // make borrowck happy!
        fn resend(
            active_requests: &HashMap<u64, (Protocol, ClusterRequestReponseSender)>,
            connection: &mut Connection,
        ) -> Result<()> {
            active_requests
                .iter()
                .try_for_each(|(_, (v, _))| connection.start_send(v.clone()).map(|_| ()))
        }

        let res = resend(&self.active_requests, self.connection.as_mut().unwrap());

        // TODO: handle more gracefully
        if res.is_err() {
            panic!(res);
        }

        self.connection.as_mut().unwrap().poll_complete().unwrap();
    }

    fn poll_connection(&mut self) {
        loop {
            match self
                .connection
                .as_mut()
                .expect("poll_connection() should only be called with an active connection")
                .poll()
            {
                Ok(Ready(Some(msg))) => {
                    if !self.handle_msg(msg) {
                        break;
                    }
                }
                Err(_) | Ok(Ready(None)) => {
                    self.connect_to_cluster(None);
                    break;
                }
                Ok(NotReady) => break,
            }
        }
    }

    /// Handle an incoming message.
    /// Returns false when the connection will be recreated.
    fn handle_msg(&mut self, msg: Protocol) -> bool {
        match msg {
            Protocol::NotLeader { leader_addr } => {
                self.connect_to_cluster(leader_addr);
                false
            }
            Protocol::RequestResult { id, res } => {
                if let Some((_, sender)) = self.active_requests.remove(&id) {
                    match sender {
                        ClusterRequestReponseSender::Request(sender) => {
                            let _ = sender.send(Ok(res));
                        }
                        _ => {}
                    };
                }

                true
            }
            Protocol::RequestChangeConfResult { id } => {
                if let Some((_, sender)) = self.active_requests.remove(&id) {
                    match sender {
                        ClusterRequestReponseSender::ChangeConf(sender) => {
                            let _ = sender.send(Ok(()));
                        }
                        _ => {}
                    };
                }

                true
            }
            Protocol::ClusterMajorityDown { id } => {
                if let Some((_, sender)) = self.active_requests.remove(&id) {
                    match sender {
                        ClusterRequestReponseSender::ChangeConf(sender) => {
                            let _ = sender.send(Err(Error::ClusterMajorityDown));
                        }
                        ClusterRequestReponseSender::Request(sender) => {
                            let _ = sender.send(Err(Error::ClusterMajorityDown));
                        }
                    };
                }

                true
            }
            _ => true,
        }
    }

    /// Poll the send requests and forward them to the cluster!
    fn poll_send_reqs(&mut self) -> Poll<Option<()>, Error> {
        loop {
            if !self.connection.as_mut().unwrap().poll_writeable() {
                return Ok(NotReady);
            }

            match self.recv_send_req.poll() {
                Ok(Ready(Some(req))) => {
                    let id = self.next_request_id;
                    self.next_request_id += 1;

                    let (req, sender) = match req {
                        ClusterRequest::Request { req, sender } => {
                            let data = bincode::serialize(&req).expect("Serializes request");
                            (
                                Protocol::Request { id, data },
                                ClusterRequestReponseSender::Request(sender),
                            )
                        }
                        ClusterRequest::ChangeConf { req, sender } => (
                            Protocol::RequestChangeConf { id, req },
                            ClusterRequestReponseSender::ChangeConf(sender),
                        ),
                    };

                    self.active_requests.insert(id, (req.clone(), sender));

                    if let Err(e) = self.connection.as_mut().unwrap().start_send(req) {
                        // TODO: maybe do not reconnect directly
                        eprintln!("poll_send_reqs error: {:?}", e);
                        // reconnect to the cluster
                        self.connect_to_cluster(None);
                        return Ok(Ready(Some(())));
                    }
                }
                Err(_) | Ok(Ready(None)) => return Ok(Ready(None)),
                Ok(NotReady) => return Ok(NotReady),
            }
        }
    }
}

impl Future for ClusterConnection {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            if self.connect.is_some() {
                try_ready!(self.poll_connect());
            } else if self.connection.is_some() {
                self.poll_connection();

                if self.connection.is_some() {
                    match try_ready!(self.poll_send_reqs()) {
                        Some(()) => {}
                        None => return Ok(Ready(())),
                    }
                }
            } else {
                self.connect_to_cluster(None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use incoming_connections::IncomingConnections;
    use node::NodeMessage;
    use tokio::executor::current_thread;

    struct FakeNode<F: Fn(NodeMessage) + Send> {
        recv_msg: UnboundedReceiver<NodeMessage>,
        incoming_connections: IncomingConnections,
        callback: Box<F>,
    }

    impl<F: Fn(NodeMessage) + Send + 'static> FakeNode<F> {
        fn create_and_run(listen_port: u16, callback: F) {
            thread::spawn(move || {
                let (sender, recv_msg) = unbounded();
                let incoming_connections = IncomingConnections::new(listen_port, sender).unwrap();
                let node = FakeNode {
                    recv_msg,
                    incoming_connections,
                    callback: Box::new(callback),
                };
                tokio::run(node);
            });
        }
    }

    impl<F: Fn(NodeMessage) + Send> Future for FakeNode<F> {
        type Item = ();
        type Error = ();

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            let _ = self.incoming_connections.poll();

            loop {
                match self.recv_msg.poll().unwrap() {
                    Ready(Some(msg)) => (self.callback)(msg),
                    Ready(None) => panic!("FakeNode::poll() - None!"),
                    NotReady => return Ok(NotReady),
                }
            }
        }
    }

    fn create_node_addresses(ports: Vec<u16>) -> Vec<SocketAddr> {
        ports
            .into_iter()
            .map(|port| ([127, 0, 0, 1], port).into())
            .collect()
    }

    #[test]
    fn client_finds_valid_node() {
        let fake_node_port = 20344;
        FakeNode::create_and_run(fake_node_port, |msg| match msg {
            NodeMessage::Propose { id, response, .. } => response
                .unbounded_send(Protocol::RequestResult {
                    id: id.get_client_request_id(),
                    res: RequestResult::Get {
                        value: Some(vec![1]),
                    },
                }).unwrap(),
            _ => panic!("unexpected message!"),
        });

        let nodes =
            create_node_addresses(vec![fake_node_port, fake_node_port + 1, fake_node_port + 2]);

        for _ in 0..10 {
            let mut client = Client::new(nodes.clone());
            let res = client.get(vec![1]);
            let res: Vec<u8> = current_thread::block_on_all(res).unwrap().unwrap();
            assert_eq!(&[1], &res[..]);
        }
    }

    #[test]
    fn client_reconnects_to_leader_hint() {
        let fake_node_port = 20354;
        let fake_node_leader_port = 20355;
        let fake_node_leader_addr: SocketAddr = ([127, 0, 0, 1], fake_node_leader_port).into();

        // start node that redirects to leader
        FakeNode::create_and_run(fake_node_port, move |msg| match msg {
            NodeMessage::Propose { response, .. } => response
                .unbounded_send(Protocol::NotLeader {
                    leader_addr: Some(fake_node_leader_addr),
                }).unwrap(),
            _ => panic!("unexpected message!"),
        });

        // start leader node
        FakeNode::create_and_run(fake_node_leader_port, |msg| match msg {
            NodeMessage::Propose { id, response, .. } => response
                .unbounded_send(Protocol::RequestResult {
                    id: id.get_client_request_id(),
                    res: RequestResult::Get {
                        value: Some(vec![1]),
                    },
                }).unwrap(),
            _ => panic!("unexpected message!"),
        });

        // do not give leader address
        let nodes = create_node_addresses(vec![fake_node_port]);

        for _ in 0..10 {
            let mut client = Client::new(nodes.clone());
            let res = client.get(vec![1]);
            let res: Vec<u8> = current_thread::block_on_all(res).unwrap().unwrap();
            assert_eq!(&[1], &res[..]);
        }
    }

    #[test]
    fn client_get_returns_none() {
        let fake_node_port = 20350;
        FakeNode::create_and_run(fake_node_port, |msg| match msg {
            NodeMessage::Propose { id, response, .. } => response
                .unbounded_send(Protocol::RequestResult {
                    id: id.get_client_request_id(),
                    res: RequestResult::Get { value: None },
                }).unwrap(),
            _ => panic!("unexpected message!"),
        });

        let nodes = create_node_addresses(vec![fake_node_port]);

        let mut client = Client::new(nodes.clone());
        let res = client.get::<_, Vec<_>>(vec![1]);
        assert_eq!(None, current_thread::block_on_all(res).unwrap(),);
    }

    #[test]
    fn client_sets_value() {
        let fake_node_port = 20364;
        FakeNode::create_and_run(fake_node_port, |msg| match msg {
            NodeMessage::Propose { id, response, .. } => response
                .unbounded_send(Protocol::RequestResult {
                    id: id.get_client_request_id(),
                    res: RequestResult::Set { successful: true },
                }).unwrap(),
            _ => panic!("unexpected message!"),
        });

        let nodes = create_node_addresses(vec![fake_node_port]);

        let mut client = Client::new(nodes.clone());
        let res = client.set(vec![1], vec![2]);
        assert!(current_thread::block_on_all(res).is_ok());
    }

    #[test]
    fn client_sets_value_fails() {
        let fake_node_port = 20365;
        FakeNode::create_and_run(fake_node_port, |msg| match msg {
            NodeMessage::Propose { id, response, .. } => response
                .unbounded_send(Protocol::RequestResult {
                    id: id.get_client_request_id(),
                    res: RequestResult::Set { successful: false },
                }).unwrap(),
            _ => panic!("unexpected message!"),
        });

        let nodes = create_node_addresses(vec![fake_node_port]);

        let mut client = Client::new(nodes.clone());
        let res = client.set(vec![1], vec![2]);
        assert_eq!(
            Error::RequestNotSuccessful,
            current_thread::block_on_all(res).err().unwrap()
        );
    }

    #[test]
    fn client_deletes_value() {
        let fake_node_port = 20366;
        FakeNode::create_and_run(fake_node_port, |msg| match msg {
            NodeMessage::Propose { id, response, .. } => response
                .unbounded_send(Protocol::RequestResult {
                    id: id.get_client_request_id(),
                    res: RequestResult::Delete { successful: true },
                }).unwrap(),
            _ => panic!("unexpected message!"),
        });

        let nodes = create_node_addresses(vec![fake_node_port]);

        let mut client = Client::new(nodes.clone());
        let res = client.delete(vec![1]);
        assert!(current_thread::block_on_all(res).is_ok());
    }

    #[test]
    fn client_deletes_value_fails() {
        let fake_node_port = 20367;
        FakeNode::create_and_run(fake_node_port, |msg| match msg {
            NodeMessage::Propose { id, response, .. } => response
                .unbounded_send(Protocol::RequestResult {
                    id: id.get_client_request_id(),
                    res: RequestResult::Delete { successful: false },
                }).unwrap(),
            _ => panic!("unexpected message!"),
        });

        let nodes = create_node_addresses(vec![fake_node_port]);

        let mut client = Client::new(nodes.clone());
        let res = client.delete(vec![1]);
        assert_eq!(
            Error::RequestNotSuccessful,
            current_thread::block_on_all(res).err().unwrap()
        );
    }

    #[test]
    fn client_scan() {
        let fake_node_port = 20368;
        FakeNode::create_and_run(fake_node_port, |msg| match msg {
            NodeMessage::Propose { id, response, .. } => response
                .unbounded_send(Protocol::RequestResult {
                    id: id.get_client_request_id(),
                    res: RequestResult::Scan {
                        keys: Some(vec![vec![1]]),
                    },
                }).unwrap(),
            _ => panic!("unexpected message!"),
        });

        let nodes = create_node_addresses(vec![fake_node_port]);

        let mut client = Client::new(nodes.clone());
        let res = client.scan();
        let res: Vec<Vec<u8>> = current_thread::block_on_all(res).unwrap();
        assert_eq!(&[1], &res[0][..]);
    }

    #[test]
    fn client_scan_fails() {
        let fake_node_port = 20369;
        FakeNode::create_and_run(fake_node_port, |msg| match msg {
            NodeMessage::Propose { id, response, .. } => response
                .unbounded_send(Protocol::RequestResult {
                    id: id.get_client_request_id(),
                    res: RequestResult::Scan { keys: None },
                }).unwrap(),
            _ => panic!("unexpected message!"),
        });

        let nodes = create_node_addresses(vec![fake_node_port]);

        let mut client = Client::new(nodes.clone());
        let res = client.scan::<Vec<_>>();
        assert_eq!(
            Error::RequestNotSuccessful,
            current_thread::block_on_all(res).err().unwrap()
        );
    }
}
