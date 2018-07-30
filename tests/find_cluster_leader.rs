#[macro_use]
extern crate futures;
extern crate schluesselwert;
extern crate tempdir;
extern crate tokio;

use schluesselwert::{Node, Peer};

use std::{
    sync::mpsc::{channel, Receiver},
    thread,
    time::Duration,
};

use futures::{
    future,
    sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
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
}

impl NodeExecutor {
    fn new(node: Node) -> (NodeExecutor, UnboundedSender<TestMessages>) {
        let (sender, msg_recv) = unbounded();
        (NodeExecutor { node, msg_recv }, sender)
    }
}

impl Future for NodeExecutor {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
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

fn start_node(
    node: usize,
    listen_port: u16,
    peers: Vec<Peer>,
) -> Receiver<UnboundedSender<TestMessages>> {
    let (sender, receiver) = channel();
    thread::spawn(move || {
        let dir = TempDir::new("with_one_node").unwrap();
        let node = Node::new(peers[node].get_id(), peers, listen_port, &dir).unwrap();

        let (executor, node_sender) = NodeExecutor::new(node);
        let _ = sender.send(node_sender);
        tokio::run(executor);
    });

    receiver
}

fn test_with_nodes(nodes: Vec<Peer>, listen_ports: Vec<u16>) {
    let node_receivers = (0..nodes.len())
        .map(|i| start_node(i, listen_ports[i], nodes.clone()))
        .collect::<Vec<_>>();
    let node_senders = node_receivers
        .into_iter()
        .map(|r| r.recv_timeout(Duration::from_millis(500)).unwrap())
        .collect::<Vec<_>>();
    thread::sleep(Duration::from_secs(2));

    let result_receivers = node_senders
        .iter()
        .map(|s| {
            let (sender, receiver) = unbounded();
            s.unbounded_send(TestMessages::RequestLeaderId { result: sender })
                .unwrap();
            receiver.into_future().map_err(|e| e.0).map(|v| match v.0 {
                Some(TestMessages::LeaderId { id }) => Some(id),
                _ => None,
            })
        })
        .collect::<Vec<_>>();

    let leader_ids = current_thread::block_on_all(future::join_all(result_receivers)).unwrap();

    assert_eq!(nodes.len(), leader_ids.len());
    if nodes.len() > 1 {
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
            })
            .expect("Leader id needs to be not None!");
    } else {
        assert_eq!(Some(nodes[0].get_id()), leader_ids[0]);
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
    test_with_nodes(nodes, listen_ports);
}

#[test]
fn with_two_node() {
    let (nodes, listen_ports) = create_nodes(2, 20010);
    test_with_nodes(nodes, listen_ports);
}

#[test]
fn with_three_node() {
    let (nodes, listen_ports) = create_nodes(3, 20020);
    test_with_nodes(nodes, listen_ports);
}

#[test]
fn with_five_node() {
    let (nodes, listen_ports) = create_nodes(5, 20030);
    test_with_nodes(nodes, listen_ports);
}
