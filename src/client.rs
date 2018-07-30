use connection::Connection;
use error::*;
use protocol::{Protocol, Request, RequestResult};

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

pub struct Client {
    send_req: UnboundedSender<(Request, oneshot::Sender<RequestResult>)>,
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
        let _ = self.send_req.unbounded_send((
            Request::Set {
                key: key.into(),
                value: value.into(),
            },
            sender,
        ));

        receiver.map_err(|e| e.into()).and_then(|rr| match rr {
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
        let _ = self
            .send_req
            .unbounded_send((Request::Get { key: key.into() }, sender));

        receiver.map_err(|e| e.into()).and_then(|rr| match rr {
            RequestResult::Get { value } => Ok(value.map(|v| v.into())),
            r @ _ => Err(Error::IncorrectRequestResult(r)),
        })
    }

    /// Delete key and value.
    pub fn delete<K: Into<Vec<u8>>>(&mut self, key: K) -> impl Future<Item = (), Error = Error> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .send_req
            .unbounded_send((Request::Delete { key: key.into() }, sender));

        receiver.map_err(|e| e.into()).and_then(|rr| match rr {
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
        let _ = self.send_req.unbounded_send((Request::Scan, sender));

        receiver.map_err(|e| e.into()).and_then(|rr| match rr {
            RequestResult::Scan { keys } => if let Some(keys) = keys {
                Ok(keys.into_iter().map(|k| k.into()).collect())
            } else {
                Err(Error::RequestNotSuccessful)
            },
            r @ _ => Err(Error::IncorrectRequestResult(r)),
        })
    }
}

struct ClusterConnection {
    /// All known cluster nodes.
    nodes: Vec<SocketAddr>,
    /// Stores a connection that is build to the cluster.
    connect: Option<ConnectFuture>,
    /// Stores the active connection to the cluster.
    connection: Option<Connection>,
    recv_send_req: UnboundedReceiver<(Request, oneshot::Sender<RequestResult>)>,
    /// The id of the next request.
    next_request_id: u64,
    /// All requests that are currently active (not answered by the cluster).
    active_requests: HashMap<u64, (Vec<u8>, oneshot::Sender<RequestResult>)>,
}

impl ClusterConnection {
    /// Creates a `ClusterConnection` instances and runs it in a thread.
    fn create_and_run(
        nodes: Vec<SocketAddr>,
    ) -> UnboundedSender<(Request, oneshot::Sender<RequestResult>)> {
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
            leader
        } else {
            rand::thread_rng().choose(&self.nodes).unwrap()
        };

        self.connect = Some(TcpStream::connect(addr));
        self.connection = None;
    }

    fn poll_connect(&mut self) {
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
            }
            Ok(NotReady) => {}
            Err(_) => self.connect_to_cluster(None),
        }
    }

    /// Resends all active requests. That is important if the connection was not to the cluster
    /// leader and thus we need to resend all the data to the leader.
    fn resend_active_requests(&mut self) {
        // make borrowck happy!
        fn resend<V2>(
            active_requests: &HashMap<u64, (Vec<u8>, V2)>,
            connection: &mut Connection,
        ) -> Result<()> {
            active_requests.iter().try_for_each(|(k, (v, _))| {
                connection
                    .start_send(Protocol::Request {
                        id: *k,
                        data: v.clone(),
                    })
                    .map(|_| ())
            })
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
                    let _ = sender.send(res);
                }

                true
            }
            _ => true,
        }
    }

    /// Poll the send requests and forward them to the cluster!
    fn poll_send_reqs(&mut self) -> bool {
        loop {
            if !self.connection.as_mut().unwrap().poll_writeable() {
                return true;
            }

            match self.recv_send_req.poll() {
                Ok(Ready(Some((req, sender)))) => {
                    let data = bincode::serialize(&req).expect("Serializes request");
                    let id = self.next_request_id;
                    self.next_request_id += 1;

                    self.active_requests.insert(id, (data.clone(), sender));

                    if let Err(e) = self
                        .connection
                        .as_mut()
                        .unwrap()
                        .start_send(Protocol::Request { id, data })
                    {
                        // TODO: maybe do not reconnect directly
                        eprintln!("poll_send_reqs error: {:?}", e);
                        // reconnect to the cluster
                        self.connect_to_cluster(None);
                    }
                }
                Err(_) | Ok(Ready(None)) => return false,
                Ok(NotReady) => return true,
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
                self.poll_connect();
            } else if self.connection.is_some() {
                self.poll_connection();

                if self.connection.is_some() {
                    self.poll_send_reqs();
                }
            } else {
                self.connect_to_cluster(None);
            }
        }
    }
}
