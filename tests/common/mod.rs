#![allow(dead_code)]

use schluesselwert::{Node, Peer, Snapshot};

use std::{
    cell::RefCell,
    collections::HashMap,
    mem,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    sync::mpsc::{channel, Receiver},
    thread,
    time::{Duration, Instant},
};

use futures::{
    sync::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    Async::{NotReady, Ready},
    Future, Poll, Stream,
};

use tokio::{executor::current_thread, runtime::Runtime, timer::Interval};

use tempdir::TempDir;

use rand::{distributions::Standard, prng::XorShiftRng, Rng, SeedableRng};

const TEST_SEED: [u8; 16] = [
    39, 122, 200, 21, 199, 23, 104, 89, 86, 255, 116, 75, 18, 231, 38, 191,
];

pub enum TestMessages {
    WaitForLeaderId {
        result: UnboundedSender<u64>,
        created: Instant,
    },
    GetLastAppliedIndex {
        result: UnboundedSender<u64>,
    },
    GetSnapshot {
        result: UnboundedSender<Snapshot>,
        last_applied_index: u64,
    },
    WaitForSnapshotApply {
        result: UnboundedSender<()>,
    },
    WaitForMajorityDown {
        result: UnboundedSender<()>,
    },
    GetLeaderId {
        result: UnboundedSender<u64>,
    },
}

/// Drives a Node in the test context.
pub struct NodeExecutor {
    node: RefCell<Node>,
    msg_recv: UnboundedReceiver<TestMessages>,
    node_handle_recv: oneshot::Receiver<oneshot::Sender<()>>,
    pending_requests: Vec<RefCell<TestMessages>>,
    timer: Interval,
    shutdown_finished: Option<oneshot::Sender<()>>,
}

impl NodeExecutor {
    fn new(node: Node) -> (NodeExecutor, UnboundedSender<TestMessages>, NodeHandle) {
        let (sender, msg_recv) = unbounded();
        let (node_handle, node_handle_recv) = NodeHandle::new();
        (
            NodeExecutor {
                node: RefCell::new(node),
                msg_recv,
                node_handle_recv,
                pending_requests: Vec::new(),
                timer: Interval::new(Instant::now(), Duration::from_millis(200)),
                shutdown_finished: None,
            },
            sender,
            node_handle,
        )
    }

    fn process_pending_requests(&mut self) {
        if self.timer.poll().map(|r| r.is_not_ready()).unwrap_or(true) {
            return;
        }
        let node = self.node.borrow_mut();
        self.pending_requests.retain(|r| {
            let mut r = r.borrow_mut();
            match *r {
                TestMessages::WaitForLeaderId {
                    ref mut result,
                    created,
                } => {
                    let leader = node.get_leader_id();
                    if created.elapsed().as_secs() > 2 && leader != 0 {
                        let _ = result.unbounded_send(leader);
                        false
                    } else {
                        true
                    }
                }
                TestMessages::GetLeaderId { ref mut result } => {
                    let leader = node.get_leader_id();
                    let _ = result.unbounded_send(leader);
                    false
                }
                TestMessages::GetSnapshot {
                    ref mut result,
                    last_applied_index,
                } => {
                    if node.get_last_applied_index() == last_applied_index {
                        let _ = result.unbounded_send(node.create_snapshot().unwrap());
                        false
                    } else {
                        true
                    }
                }
                TestMessages::WaitForSnapshotApply { ref mut result } => {
                    if node.applied_snapshot() {
                        let _ = result.unbounded_send(());
                        false
                    } else {
                        true
                    }
                }
                TestMessages::GetLastAppliedIndex { ref mut result } => {
                    let _ = result.unbounded_send(node.get_last_applied_index());
                    false
                }
                TestMessages::WaitForMajorityDown { ref mut result } => {
                    if !node.is_cluster_majority_running() {
                        let _ = result.unbounded_send(());
                        false
                    } else {
                        true
                    }
                }
            }
        })
    }
}

impl Future for NodeExecutor {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.node_handle_recv.poll() {
            Err(_) => return Ok(Ready(())),
            Ok(Ready(sender)) => {
                self.node.borrow_mut().shutdown();
                self.shutdown_finished = Some(sender);
            }
            _ => {}
        };

        match self.node.borrow_mut().poll().unwrap() {
            Ready(()) => {
                if let Some(sender) = self.shutdown_finished.take() {
                    let _ = sender.send(());
                }

                return Ok(Ready(()));
            }
            _ => {}
        };

        loop {
            match self.msg_recv.poll().unwrap() {
                Ready(Some(req)) => {
                    self.pending_requests.push(RefCell::new(req));
                }
                _ => break,
            };
        }

        self.process_pending_requests();
        Ok(NotReady)
    }
}

/// When dropped, a the associated Node will stop
pub struct NodeHandle {
    sender: oneshot::Sender<oneshot::Sender<()>>,
}

impl NodeHandle {
    fn new() -> (NodeHandle, oneshot::Receiver<oneshot::Sender<()>>) {
        let (sender, recv) = oneshot::channel();
        (NodeHandle { sender }, recv)
    }

    fn shutdown(self) -> oneshot::Receiver<()> {
        let (sender, recv) = oneshot::channel();
        let _ = self.sender.send(sender);
        recv
    }
}

fn start_node(
    node: Peer,
    listen_port: u16,
    peers: Vec<Peer>,
    path: Option<TempDir>,
) -> Receiver<(NodeHandle, UnboundedSender<TestMessages>, TempDir)> {
    let (sender, receiver) = channel();
    thread::spawn(move || {
        let dir = path.unwrap_or_else(|| TempDir::new("node").unwrap());
        let mut runtime = Runtime::new().expect("Creates runtime");

        let node = Node::new(node.get_id(), peers, listen_port, &dir).unwrap();

        let (executor, node_sender, node_handle) = NodeExecutor::new(node);
        let _ = sender.send((node_handle, node_sender, dir));

        let _ = runtime.block_on(executor);
        runtime.shutdown_now().wait().unwrap();
    });

    receiver
}

type NodesMapInner = HashMap<
    u64,
    (
        Peer,
        NodeHandle,
        UnboundedSender<TestMessages>,
        Option<TempDir>,
    ),
>;
pub struct NodesMap(NodesMapInner);

impl NodesMap {
    pub fn merge(&mut self, mut other: Self) {
        let inner = mem::replace(&mut other.0, HashMap::default());
        self.0.extend(inner);
    }

    pub fn take_dir(&mut self, id: u64) -> Option<TempDir> {
        self.0.get_mut(&id).unwrap().3.take()
    }

    pub fn take_dir_and_shutdown(&mut self, id: u64) -> Option<TempDir> {
        let res = self.0.remove(&id).unwrap();
        let _ = current_thread::block_on_all(res.1.shutdown());
        res.3
    }

    pub fn shutdown_node(&mut self, id: u64) {
        let _ = current_thread::block_on_all(self.0.remove(&id).unwrap().1.shutdown());
    }

    pub fn restart_node(&mut self, id: u64, db_path: Option<TempDir>, base_listen_port: u16) {
        assert!(!self.0.contains_key(&id));
        let (node, listen_port) = create_node(id, base_listen_port);

        let mut nodes = self.0.values().map(|v| v.0.clone()).collect::<Vec<_>>();
        nodes.push(node.clone());

        self.merge(setup_nodes_with_cluster_nodes(
            vec![node],
            vec![listen_port],
            Some(vec![db_path]),
            nodes,
        ));
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
    db_paths: Option<Vec<Option<TempDir>>>,
    cluster_nodes: Vec<Peer>,
) -> NodesMap {
    let db_paths = db_paths.unwrap_or_else(|| listen_ports.iter().map(|_| None).collect());

    let node_receivers = nodes
        .into_iter()
        .zip(listen_ports.into_iter())
        .zip(db_paths.into_iter())
        .map(|((n, lp), db)| {
            (
                n.get_id(),
                n.clone(),
                start_node(n, lp, cluster_nodes.clone(), db),
            )
        }).collect::<Vec<_>>();

    node_receivers
        .into_iter()
        .map(|(id, n, r)| (id, n, r.recv_timeout(Duration::from_millis(500)).unwrap()))
        .map(|(id, n, v)| (id, (n, v.0, v.1, Some(v.2))))
        .collect::<HashMap<_, _>>()
        .into()
}

/// Setup the nodes
pub fn setup_nodes(nodes: Vec<Peer>, listen_ports: Vec<u16>) -> NodesMap {
    let cluster_nodes = nodes.clone();
    setup_nodes_with_cluster_nodes(nodes, listen_ports, None, cluster_nodes)
}

/// Collect the leader id from each nodes, checks that all selected the same leader and returns
/// the leader id.
pub fn collect_leader_ids(nodes_map: &NodesMap) -> u64 {
    let receiver = {
        let (sender, receiver) = unbounded();
        nodes_map.iter().for_each(|(_, (_, _, s, _))| {
            s.unbounded_send(TestMessages::WaitForLeaderId {
                result: sender.clone(),
                created: Instant::now(),
            }).unwrap();
        });
        receiver
    };

    let leader_ids = current_thread::block_on_all(receiver.collect()).unwrap();

    assert_eq!(nodes_map.len(), leader_ids.len());
    if nodes_map.len() > 1 {
        leader_ids.into_iter().fold(0, |leader_id, id| {
            if leader_id == 0 {
                assert_ne!(0, id);
                id
            } else {
                assert_eq!(leader_id, id);
                leader_id
            }
        })
    } else {
        assert!(nodes_map.contains_key(&leader_ids[0]));
        leader_ids[0]
    }
}

pub fn wait_for_same_leader(nodes_map: &NodesMap) -> u64 {
    loop {
        let receiver = {
            let (sender, receiver) = unbounded();
            nodes_map.iter().for_each(|(_, (_, _, s, _))| {
                s.unbounded_send(TestMessages::GetLeaderId {
                    result: sender.clone(),
                }).unwrap();
            });
            receiver
        };

        let leader_ids = current_thread::block_on_all(receiver.collect()).unwrap();

        assert_eq!(nodes_map.len(), leader_ids.len());
        if nodes_map.len() > 1 {
            // If all are the same, return it
            if leader_ids.iter().eq(leader_ids.iter()) && leader_ids[0] != 0 {
                return leader_ids[0];
            }
        } else {
            return leader_ids[0];
        }
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

pub fn generate_random_data_with_size(max_size: usize) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut rng = XorShiftRng::from_seed(TEST_SEED);
    let mut data = HashMap::new();
    let mut size = 0;

    loop {
        let key_len = rng.gen_range(5, 15);
        let key = rng.sample_iter(&Standard).take(key_len).collect();

        let value_len = rng.gen_range(25, 50);
        let value = rng.sample_iter(&Standard).take(value_len).collect();

        data.insert(key, value);

        size += key_len + value_len;

        if size >= max_size {
            return data;
        }
    }
}

/// Create a snapshot on each node and compare all of them.
pub fn compare_node_snapshots(nodes_map: &NodesMap) {
    let leader_id = collect_leader_ids(nodes_map);

    let (sender, receiver) = unbounded();
    nodes_map
        .get(&leader_id)
        .unwrap()
        .2
        .unbounded_send(TestMessages::GetLastAppliedIndex { result: sender })
        .unwrap();
    let last_applied_index = current_thread::block_on_all(receiver.collect()).unwrap()[0];

    let receiver = {
        let (sender, receiver) = unbounded();
        nodes_map.iter().for_each(|(_, (_, _, s, _))| {
            s.unbounded_send(TestMessages::GetSnapshot {
                result: sender.clone(),
                last_applied_index,
            }).unwrap();
        });
        receiver
    };

    let snapshots = current_thread::block_on_all(receiver.collect()).unwrap();

    assert_eq!(nodes_map.len(), snapshots.len());
    if nodes_map.len() > 1 {
        snapshots.into_iter().fold(None, |snapshot, current| {
            if snapshot.is_none() {
                Some(current)
            } else {
                assert_eq!(snapshot, Some(current));
                snapshot
            }
        });
    } else {
        panic!("Only one snapshot");
    }
}

pub fn wait_for_snapshot_applied(nodes_map: &NodesMap, node_id: u64) {
    let (sender, receiver) = unbounded();
    nodes_map
        .get(&node_id)
        .unwrap()
        .2
        .unbounded_send(TestMessages::WaitForSnapshotApply { result: sender })
        .unwrap();

    current_thread::block_on_all(receiver.collect()).unwrap();
}

pub fn wait_for_cluster_majority_down(nodes_map: &NodesMap) {
    let receiver = {
        let (sender, receiver) = unbounded();
        nodes_map.values().for_each(|v| {
            v.2.unbounded_send(TestMessages::WaitForMajorityDown {
                result: sender.clone(),
            }).unwrap()
        });
        receiver
    };

    current_thread::block_on_all(receiver.collect()).unwrap();
}

pub fn listen_ports_to_socket_addrs(listen_ports: Vec<u16>) -> Vec<SocketAddr> {
    listen_ports
        .into_iter()
        .map(|p| ([127, 0, 0, 1], p).into())
        .collect()
}
