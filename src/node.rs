use error::*;
use storage::Storage;

use raft::{self, raw_node::RawNode, Config, Peer};

use std::{
    path::Path,
    time::{Duration, Instant},
};

use tokio_timer::Interval;

use futures::{Future, Poll};

struct Node {
    node: RawNode<Storage>,
    timer: Interval,
}

impl Node {
    fn new<T: AsRef<Path>>(id: u64, peers: Vec<Peer>, storage_path: T) -> Result<Node> {
        let storage = Storage::new(storage_path)?;
        // TODO: Check config values!
        let config = Config::new(id);
        let node = RawNode::new(&config, storage, peers)?;
        let timer = Interval::new(Instant::now(), Duration::from_millis(100));

        Ok(Node { node, timer })
    }

    fn tick(&mut self) {
        self.node.tick();

        if self.node.has_ready() {
            self.process_ready();
        }
    }

    fn process_ready(&mut self) {
        let mut ready = self.node.ready();

        if !raft::is_empty_snap(&ready.snapshot) {
            self.node.mut_store().apply_snapshot(&ready.snapshot).unwrap();
        }

        if !ready.entries.is_empty() {
            self.node.mut_store().append_entries(&ready.entries).unwrap();
        }

        if let Some(ref hs) = ready.hs {
            self.node.mut_store().set_hardstate(hs.clone());
        }

        if !self.is_leader() {
            // If not leader, the follower needs to reply the messages to
            // the leader after appending Raft entries.
            let msgs = ready.messages.drain(..);
            for _msg in msgs {
                // Send messages to other peers.
            }
}
    }

    fn is_leader(&self) -> bool {
        self.node.raft.id == self.node.raft.leader_id
    }
}

impl Future for Node {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            try_ready!(self.timer.poll());

            self.tick();
        }
    }
}
