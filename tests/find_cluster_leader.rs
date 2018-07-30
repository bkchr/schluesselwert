extern crate futures;
extern crate schluesselwert;
extern crate tempdir;
extern crate tokio;

use schluesselwert::{Node, Peer};

use std::{
    thread,
    time::{Duration, Instant},
};

use futures::Future;

use tokio::{executor::current_thread, timer::Delay};

use tempdir::TempDir;

#[test]
fn with_one_node() {
    let dir = TempDir::new("with_one_node").unwrap();
    let mut node = Node::new(
        1,
        vec![Peer::new(1, ([127, 0, 0, 1], 20000).into())],
        20000,
        &dir,
    ).unwrap();

    tokio::run(node.map_err(|e| panic!(e)));

    // assert_eq!(1, node.get_leader_id());
}
