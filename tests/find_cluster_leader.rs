#[macro_use]
extern crate futures;
extern crate schluesselwert;
extern crate tempdir;
extern crate tokio;

use schluesselwert::{Node, Peer};

use std::{
    sync::mpsc::{channel, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};

use futures::{
    sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    Future, Poll, Stream,
};

use tokio::{executor::current_thread, timer::Delay};

use tempdir::TempDir;

enum TestMessages {
    RequestLeaderId {
        result: UnboundedSender<TestMessages>,
    },
    LeaderId {
        id: u64,
        node: u64,
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
                        node: self.node.get_id(),
                    });
                }
                _ => {}
            };
        }
    }
}

#[test]
fn with_one_node() {
    let (sender, receiver) = channel();
    thread::spawn(move || {
        let dir = TempDir::new("with_one_node").unwrap();
        let node = Node::new(
            1,
            vec![Peer::new(1, ([127, 0, 0, 1], 20000).into())],
            20000,
            &dir,
        ).unwrap();

        let (executor, node_sender) = NodeExecutor::new(node);
        let _ = sender.send(node_sender);
        tokio::run(executor);
    });

    let node_sender = receiver.recv_timeout(Duration::from_millis(500)).unwrap();
    thread::sleep(Duration::from_secs(2));

    let (sender, receiver) = unbounded();
    node_sender
        .unbounded_send(TestMessages::RequestLeaderId { result: sender })
        .unwrap();

    let leader_id = current_thread::block_on_all(
        receiver.into_future().map_err(|e| e.0).map(|v| v.0),
    ).unwrap()
        .unwrap();

    assert_eq!(
        1,
        match leader_id {
            TestMessages::LeaderId { id, .. } => id,
            _ => panic!("unexpected message!"),
        }
    );
}
