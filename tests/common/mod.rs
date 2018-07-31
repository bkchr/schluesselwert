#![allow(dead_code)]

use schluesselwert::{Node, Peer};

use std::{
    collections::HashMap,
    mem,
    ops::{Deref, DerefMut},
    sync::mpsc::{channel, Receiver},
    thread,
    time::Duration,
};

use futures::{
    future,
    sync::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    Async::Ready,
    Future, Poll, Stream,
};

use tokio::{executor::current_thread, runtime::Runtime};

use tempdir::TempDir;

use rand::{Rng, SeedableRng, distributions::Standard, prng::XorShiftRng};

const TEST_SEED: [u8; 16] = [
    39, 122, 200, 21, 199, 23, 104, 89, 86, 255, 116, 75, 18, 231, 38, 191,
];

pub enum TestMessages {
    RequestLeaderId {
        result: UnboundedSender<TestMessages>,
    },
    LeaderId {
        id: u64,
    },
}

/// Drives a Node in the test context.
pub struct NodeExecutor {
    node: Node,
    msg_recv: UnboundedReceiver<TestMessages>,
    node_handle_recv: oneshot::Receiver<()>,
}

impl NodeExecutor {
    fn new(node: Node) -> (NodeExecutor, UnboundedSender<TestMessages>, NodeHandle) {
        let (sender, msg_recv) = unbounded();
        let (node_handle, node_handle_recv) = NodeHandle::new();
        (
            NodeExecutor {
                node,
                msg_recv,
                node_handle_recv,
            },
            sender,
            node_handle,
        )
    }
}

impl Future for NodeExecutor {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.node_handle_recv.poll().is_err() {
            return Ok(Ready(()));
        }

        self.node.poll().unwrap();
        loop {
            match try_ready!(self.msg_recv.poll()) {
                Some(TestMessages::RequestLeaderId { result }) => {
                    let _ = result.unbounded_send(TestMessages::LeaderId {
                        id: self.node.get_leader_id(),
                    });
                }
                _ => {}
            };
        }
    }
}

/// When dropped, a the associated Node will stop
pub struct NodeHandle {
    _sender: oneshot::Sender<()>,
}

impl NodeHandle {
    fn new() -> (NodeHandle, oneshot::Receiver<()>) {
        let (sender, recv) = oneshot::channel();
        (NodeHandle { _sender: sender }, recv)
    }
}

fn start_node(
    node: Peer,
    listen_port: u16,
    peers: Vec<Peer>,
) -> Receiver<(NodeHandle, UnboundedSender<TestMessages>)> {
    let (sender, receiver) = channel();
    thread::spawn(move || {
        let dir = TempDir::new("with_one_node").unwrap();
        let mut runtime = Runtime::new().expect("Creates runtime");

        let node = Node::new(node.get_id(), peers, listen_port, &dir).unwrap();

        let (executor, node_sender, node_handle) = NodeExecutor::new(node);
        let _ = sender.send((node_handle, node_sender));

        let _ = runtime.block_on(executor);
        runtime.shutdown_now();
    });

    receiver
}

type NodesMapInner = HashMap<u64, (NodeHandle, UnboundedSender<TestMessages>)>;
pub struct NodesMap(NodesMapInner);

impl NodesMap {
    pub fn merge(&mut self, mut other: Self) {
        let inner = mem::replace(&mut other.0, HashMap::default());
        self.0.extend(inner);
    }
}

impl From<NodesMapInner> for NodesMap {
    fn from(map: NodesMapInner) -> NodesMap {
        NodesMap(map)
    }
}

impl Deref for NodesMap {
    type Target = NodesMapInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for NodesMap {
    fn deref_mut(&mut self) -> &mut NodesMapInner {
        &mut self.0
    }
}

impl Drop for NodesMap {
    fn drop(&mut self) {
        if !self.0.is_empty() {
            self.0.clear();
            // To prevent crashes at closing the node threads, we give them some time to finish
            thread::sleep(Duration::from_millis(500));
        }
    }
}

/// Setup the given nodes and tell them about the given cluster nodes
pub fn setup_nodes_with_cluster_nodes(
    nodes: Vec<Peer>,
    listen_ports: Vec<u16>,
    cluster_nodes: Vec<Peer>,
) -> NodesMap {
    let node_receivers = (0..nodes.len())
        .map(|i| {
            (
                nodes[i].get_id(),
                start_node(nodes[i].clone(), listen_ports[i], cluster_nodes.clone()),
            )
        }).collect::<Vec<_>>();

    node_receivers
        .into_iter()
        .map(|(id, r)| (id, r.recv_timeout(Duration::from_millis(500)).unwrap()))
        .collect::<HashMap<_, _>>()
        .into()
}

/// Setup the nodes
pub fn setup_nodes(nodes: Vec<Peer>, listen_ports: Vec<u16>) -> NodesMap {
    let cluster_nodes = nodes.clone();
    setup_nodes_with_cluster_nodes(nodes, listen_ports, cluster_nodes)
}

/// Collect the leader id from each nodes, checks that all selected the same leader and returns
/// the leader id.
pub fn collect_leader_ids(nodes_map: &NodesMap) -> u64 {
    let result_receivers = nodes_map
        .iter()
        .map(|(_, (_, s))| {
            let (sender, receiver) = unbounded();
            s.unbounded_send(TestMessages::RequestLeaderId { result: sender })
                .unwrap();
            receiver.into_future().map_err(|e| e.0).map(|v| match v.0 {
                Some(TestMessages::LeaderId { id }) => Some(id),
                _ => None,
            })
        }).collect::<Vec<_>>();

    let leader_ids = current_thread::block_on_all(future::join_all(result_receivers)).unwrap();

    assert_eq!(nodes_map.len(), leader_ids.len());
    if nodes_map.len() > 1 {
        leader_ids
            .into_iter()
            .fold(None, |leader_id, id| {
                if leader_id.is_none() {
                    assert_ne!(Some(0), id);
                    id
                } else {
                    assert_eq!(leader_id, id);
                    leader_id
                }
            }).expect("Leader id needs to be not None!")
    } else {
        assert!(nodes_map.contains_key(&leader_ids[0].unwrap()));
        leader_ids[0].unwrap()
    }
}

pub fn create_node(id: u64, first_listen_port: u16) -> (Peer, u16) {
    let listen_port = first_listen_port + id as u16;
    let node = Peer::new(id as u64, ([127, 0, 0, 1], listen_port).into());

    (node, listen_port)
}

pub fn create_nodes(count: usize, first_listen_port: u16) -> (Vec<Peer>, Vec<u16>) {
    let mut nodes = Vec::new();
    let mut listen_ports = Vec::new();

    for i in 1..=count {
        let (node, listen_port) = create_node(i as u64, first_listen_port);

        nodes.push(node);
        listen_ports.push(listen_port);
    }

    (nodes, listen_ports)
}

pub fn generate_random_data(count: usize) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut rng = XorShiftRng::from_seed(TEST_SEED);
    let mut data = HashMap::new();

    while data.len() < count {
        let key_len = rng.gen_range(5, 15);
        let key = rng.sample_iter(&Standard).take(key_len).collect();

        let value_len = rng.gen_range(25, 50);
        let value = rng.sample_iter(&Standard).take(value_len).collect();

        data.insert(key, value);
    }

    data
}
