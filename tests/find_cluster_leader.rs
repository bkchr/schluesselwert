#[macro_use]
extern crate futures;
extern crate schluesselwert;
extern crate tempdir;
extern crate tokio;

mod common;

use common::{
    collect_leader_ids, create_node, create_nodes, setup_nodes, setup_nodes_with_cluster_nodes,
    NodesMap,
};

use std::{thread, time::Duration};

// TODO: Implement all the stuff without sleeps!

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

fn wait_for_leader_kill_leader_and_wait_for_next_leader_impl(
    base_listen_port: u16,
) -> (NodesMap, u64, u64) {
    let (nodes, listen_ports) = create_nodes(5, base_listen_port);
    let mut nodes_map = setup_nodes(nodes, listen_ports);

    // give some time for the election
    thread::sleep(Duration::from_secs(2));
    let leader_id = collect_leader_ids(&nodes_map);

    nodes_map
        .remove(&leader_id)
        .expect("Leader needs to exist in the nodes map!");

    // give some time for the election
    thread::sleep(Duration::from_secs(2));
    let new_leader_id = collect_leader_ids(&nodes_map);

    assert_ne!(leader_id, new_leader_id);
    (nodes_map, leader_id, new_leader_id)
}

#[test]
fn wait_for_leader_kill_leader_and_wait_for_next_leader() {
    wait_for_leader_kill_leader_and_wait_for_next_leader_impl(20040);
}

#[test]
fn killed_node_rejoins() {
    let (mut nodes_map, removed_node, last_leader) =
        wait_for_leader_kill_leader_and_wait_for_next_leader_impl(20050);

    let (nodes, _) = create_nodes(5, 20050);
    let (removed_node, listen_port) = create_node(removed_node, 20050);
    // just recreate the killed node
    let new_nodes_map =
        setup_nodes_with_cluster_nodes(vec![removed_node], vec![listen_port], nodes.clone());
    nodes_map.merge(new_nodes_map);

    // give some time for joining the new node
    thread::sleep(Duration::from_secs(2));
    let new_leader_id = collect_leader_ids(&nodes_map);

    assert_eq!(last_leader, new_leader_id);
}
