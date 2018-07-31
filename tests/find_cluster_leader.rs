extern crate futures;
extern crate rand;
extern crate schluesselwert;
extern crate tempdir;
extern crate tokio;

mod common;

use common::{
    collect_leader_ids, create_node, create_nodes, setup_nodes, setup_nodes_with_cluster_nodes,
    NodesMap,
};

#[test]
fn with_one_node() {
    let (nodes, listen_ports) = create_nodes(1, 20000);
    let nodes_map = setup_nodes(nodes, listen_ports);

    collect_leader_ids(&nodes_map, None);
}

#[test]
fn with_two_node() {
    let (nodes, listen_ports) = create_nodes(2, 20010);
    let nodes_map = setup_nodes(nodes, listen_ports);

    collect_leader_ids(&nodes_map, None);
}

#[test]
fn with_three_node() {
    let (nodes, listen_ports) = create_nodes(3, 20020);
    let nodes_map = setup_nodes(nodes, listen_ports);

    collect_leader_ids(&nodes_map, None);
}

#[test]
fn with_five_node() {
    let (nodes, listen_ports) = create_nodes(5, 20030);
    let nodes_map = setup_nodes(nodes, listen_ports);

    collect_leader_ids(&nodes_map, None);
}

fn wait_for_leader_kill_leader_and_wait_for_next_leader_impl(
    base_listen_port: u16,
) -> (NodesMap, u64, u64) {
    let (nodes, listen_ports) = create_nodes(5, base_listen_port);
    let mut nodes_map = setup_nodes(nodes, listen_ports);

    let leader_id = collect_leader_ids(&nodes_map, None);

    nodes_map
        .remove(&leader_id)
        .expect("Leader needs to exist in the nodes map!");

    let new_leader_id = collect_leader_ids(&nodes_map, Some(leader_id));

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
        setup_nodes_with_cluster_nodes(vec![removed_node], vec![listen_port], None, nodes.clone());
    nodes_map.merge(new_nodes_map);

    let new_leader_id = collect_leader_ids(&nodes_map, None);

    assert_eq!(last_leader, new_leader_id);
}
