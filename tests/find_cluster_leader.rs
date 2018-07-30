#[macro_use]
extern crate futures;
extern crate schluesselwert;
extern crate tempdir;
extern crate tokio;

use schluesselwert::{Node, Peer};

use std::{
    collections::HashMap,
    ops::Deref,
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

use tokio::executor::current_thread;

use tempdir::TempDir;

enum TestMessages {
    RequestLeaderId {
        result: UnboundedSender<TestMessages>,
    },
    LeaderId {
        id: u64,
    },
}

struct NodeExecutor {
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
struct NodeHandle {
    _sender: oneshot::Sender<()>,
}

impl NodeHandle {
    fn new() -> (NodeHandle, oneshot::Receiver<()>) {
        let (sender, recv) = oneshot::channel();
        (NodeHandle { _sender: sender }, recv)
    }
}

fn start_node(
    node: usize,
    listen_port: u16,
    peers: Vec<Peer>,
) -> Receiver<(NodeHandle, UnboundedSender<TestMessages>)> {
    let (sender, receiver) = channel();
    thread::spawn(move || {
        let dir = TempDir::new("with_one_node").unwrap();
        let node = Node::new(peers[node].get_id(), peers, listen_port, &dir).unwrap();

        let (executor, node_sender, node_handle) = NodeExecutor::new(node);
        let _ = sender.send((node_handle, node_sender));
        tokio::run(executor);
    });

    receiver
}

struct NodesMap(HashMap<u64, (NodeHandle, UnboundedSender<TestMessages>)>);

impl From<HashMap<u64, (NodeHandle, UnboundedSender<TestMessages>)>> for NodesMap {
    fn from(map: HashMap<u64, (NodeHandle, UnboundedSender<TestMessages>)>) -> NodesMap {
        NodesMap(map)
    }
}

impl Deref for NodesMap {
    type Target = HashMap<u64, (NodeHandle, UnboundedSender<TestMessages>)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for NodesMap {
    fn drop(&mut self) {
        self.0.clear();
        // To prevent crashes at closing the node threads, we give them some time to finish
        thread::sleep(Duration::from_millis(500));
    }
}

/// Setup the nodes
fn setup_nodes(nodes: Vec<Peer>, listen_ports: Vec<u16>) -> NodesMap {
    let node_receivers = (0..nodes.len())
        .map(|i| {
            (
                nodes[i].get_id(),
                start_node(i, listen_ports[i], nodes.clone()),
            )
        }).collect::<Vec<_>>();
    node_receivers
        .into_iter()
        .map(|(id, r)| (id, r.recv_timeout(Duration::from_millis(500)).unwrap()))
        .collect::<HashMap<_, _>>()
        .into()
}

/// Collect the leader id from each nodes, checks that all selected the same leader and returns
/// the leader id.
fn collect_leader_ids(nodes_map: &NodesMap) -> u64 {
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

fn create_nodes(count: usize, first_listen_port: u16) -> (Vec<Peer>, Vec<u16>) {
    let mut nodes = Vec::new();
    let mut listen_ports = Vec::new();

    for i in 1..=count {
        let listen_port = first_listen_port + i as u16;
        let node = Peer::new(i as u64, ([127, 0, 0, 1], listen_port).into());

        nodes.push(node);
        listen_ports.push(listen_port);
    }

    (nodes, listen_ports)
}

#[test]
fn with_one_node() {
    let (nodes, listen_ports) = create_nodes(1, 20000);
    let nodes_map = setup_nodes(nodes, listen_ports);

    // give some time for the election
    thread::sleep(Duration::from_secs(2));
    collect_leader_ids(&nodes_map);
}

#[test]
fn with_two_node() {
    let (nodes, listen_ports) = create_nodes(2, 20010);
    let nodes_map = setup_nodes(nodes, listen_ports);

    // give some time for the election
    thread::sleep(Duration::from_secs(2));
    collect_leader_ids(&nodes_map);
}

#[test]
fn with_three_node() {
    let (nodes, listen_ports) = create_nodes(3, 20020);
    let nodes_map = setup_nodes(nodes, listen_ports);

    // give some time for the election
    thread::sleep(Duration::from_secs(2));
    collect_leader_ids(&nodes_map);
}

#[test]
fn with_five_node() {
    let (nodes, listen_ports) = create_nodes(5, 20030);
    let nodes_map = setup_nodes(nodes, listen_ports);

    // give some time for the election
    thread::sleep(Duration::from_secs(2));
    collect_leader_ids(&nodes_map);
}
