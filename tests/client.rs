extern crate futures;
extern crate rand;
extern crate schluesselwert;
extern crate tempdir;
extern crate tokio;

mod common;

use common::{
    collect_leader_ids, compare_node_snapshots, create_node, create_nodes, generate_random_data,
    setup_nodes, setup_nodes_with_cluster_nodes, wait_for_cluster_majority_down,
    wait_for_snapshot_applied, NodesMap,
};

use schluesselwert::{Client, Error};

use std::{collections::HashMap, net::SocketAddr};

use tokio::executor::current_thread;

use futures::{future, Future};

use tempdir::TempDir;

fn listen_ports_to_socket_addrs(listen_ports: Vec<u16>) -> Vec<SocketAddr> {
    listen_ports
        .into_iter()
        .map(|p| ([127, 0, 0, 1], p).into())
        .collect()
}

fn check_data_with_get(
    test_data: HashMap<Vec<u8>, Vec<u8>>,
    client: &mut Client,
    skip: Option<usize>,
    take: Option<usize>,
) {
    let test_data_len = test_data.len();
    let mut test_data = test_data
        .into_iter()
        .skip(skip.unwrap_or(0))
        .take(take.unwrap_or_else(|| test_data_len))
        .collect::<HashMap<_, _>>();

    let cluster_data =
        current_thread::block_on_all(future::join_all(test_data.iter().map(|(k, _)| {
            client
                .get(k.clone())
                .map(move |v: Option<Vec<u8>>| (k.clone(), v))
        }))).unwrap();

    cluster_data
        .into_iter()
        .for_each(|(k, v)| assert_eq!(&test_data.remove(&k).unwrap()[..], &v.unwrap()[..]));
    assert!(test_data.is_empty());
}

#[test]
fn set_and_get_values() {
    let (nodes, listen_ports) = create_nodes(5, 20030);
    let nodes_map = setup_nodes(nodes, listen_ports.clone());

    let nodes = listen_ports_to_socket_addrs(listen_ports);
    let mut client = Client::new(nodes);

    let test_data = generate_random_data(1000);
    // wait for cluster to start
    collect_leader_ids(&nodes_map);

    current_thread::block_on_all(future::join_all(
        test_data
            .iter()
            .map(|(k, v)| client.set(k.clone(), v.clone())),
    )).unwrap();

    check_data_with_get(test_data, &mut client, None, None);
}

#[test]
fn set_delete_and_get_and_scan() {
    let (nodes, listen_ports) = create_nodes(5, 20040);
    let nodes_map = setup_nodes(nodes, listen_ports.clone());

    let nodes = listen_ports_to_socket_addrs(listen_ports);
    let mut client = Client::new(nodes);

    let test_data = generate_random_data(1000);
    // wait for cluster to start
    collect_leader_ids(&nodes_map);
    current_thread::block_on_all(future::join_all(
        test_data
            .iter()
            .map(|(k, v)| client.set(k.clone(), v.clone())),
    )).unwrap();

    current_thread::block_on_all(future::join_all(
        test_data.iter().map(|(k, _)| client.delete(k.clone())),
    )).unwrap();

    let cluster_data =
        current_thread::block_on_all(future::join_all(test_data.iter().map(|(k, _)| {
            client
                .get(k.clone())
                .map(move |v: Option<Vec<u8>>| (k.clone(), v))
        }))).unwrap();

    assert_eq!(test_data.len(), cluster_data.len());
    cluster_data
        .into_iter()
        .for_each(|(_, v)| assert_eq!(None, v));

    let keys: Vec<Vec<u8>> = current_thread::block_on_all(client.scan()).unwrap();
    assert!(keys.is_empty());
}

#[test]
fn set_and_scan() {
    let (nodes, listen_ports) = create_nodes(5, 20050);
    let nodes_map = setup_nodes(nodes, listen_ports.clone());

    let nodes = listen_ports_to_socket_addrs(listen_ports);
    let mut client = Client::new(nodes);

    let mut test_data = generate_random_data(1000);
    // wait for cluster to start
    collect_leader_ids(&nodes_map);
    current_thread::block_on_all(future::join_all(
        test_data
            .iter()
            .map(|(k, v)| client.set(k.clone(), v.clone())),
    )).unwrap();

    let keys: Vec<Vec<u8>> = current_thread::block_on_all(client.scan()).unwrap();

    keys.into_iter()
        .for_each(|k| assert!(test_data.remove(&k).is_some()));
    assert!(test_data.is_empty());
}

#[test]
fn set_500_add_node_and_set_500_more() {
    let (nodes, listen_ports) = create_nodes(4, 20060);
    let mut nodes_map = setup_nodes(nodes, listen_ports.clone());

    let nodes = listen_ports_to_socket_addrs(listen_ports);
    let mut client = Client::new(nodes);

    let test_data = generate_random_data(1000);
    // wait for cluster to start
    collect_leader_ids(&nodes_map);
    current_thread::block_on_all(future::join_all(
        test_data
            .iter()
            .take(500)
            .map(|(k, v)| client.set(k.clone(), v.clone())),
    )).unwrap();

    // spawn a new node
    let (nodes, _) = create_nodes(5, 20060);
    let (new_node, listen_port) = create_node(5, 20060);
    let new_nodes_map =
        setup_nodes_with_cluster_nodes(vec![new_node.clone()], vec![listen_port], None, nodes);
    nodes_map.merge(new_nodes_map);

    // Add the new node
    current_thread::block_on_all(
        client.add_node_to_cluster(new_node.get_id(), new_node.get_addr()),
    ).unwrap();

    // Set the rest of the data
    current_thread::block_on_all(future::join_all(
        test_data
            .iter()
            .skip(500)
            .take(500)
            .map(|(k, v)| client.set(k.clone(), v.clone())),
    )).unwrap();

    // Check that all data was set
    check_data_with_get(test_data, &mut client, None, None);

    // just make sure that all nodes have the same leader
    collect_leader_ids(&nodes_map);

    wait_for_snapshot_applied(&nodes_map, 5);
    // Make sure that all nodes have the same data
    compare_node_snapshots(&nodes_map);
}

#[test]
fn remove_leader_and_re_add_node() {
    let (nodes, listen_ports) = create_nodes(5, 20070);
    let mut nodes_map = setup_nodes(nodes, listen_ports.clone());

    let nodes = listen_ports_to_socket_addrs(listen_ports);
    let mut client = Client::new(nodes);

    let test_data = generate_random_data(1000);
    // wait for cluster to start
    collect_leader_ids(&nodes_map);
    current_thread::block_on_all(future::join_all(
        test_data
            .iter()
            .take(500)
            .map(|(k, v)| client.set(k.clone(), v.clone())),
    )).unwrap();

    let leader_id = collect_leader_ids(&nodes_map);
    current_thread::block_on_all(client.remove_node_from_cluster(leader_id)).unwrap();
    let removed_node_path = nodes_map.take_dir_and_remove(leader_id);

    collect_leader_ids(&nodes_map);

    // add the rest of the data
    current_thread::block_on_all(future::join_all(
        test_data
            .iter()
            .skip(500)
            .map(|(k, v)| client.set(k.clone(), v.clone())),
    )).unwrap();

    // restart the node
    nodes_map.restart_node(leader_id, removed_node_path, 20070);

    current_thread::block_on_all(
        client.add_node_to_cluster(leader_id, ([127, 0, 0, 1], 20070 + leader_id as u16).into()),
    ).unwrap();

    // Check that all data was set
    check_data_with_get(test_data, &mut client, None, None);

    // just make sure that all nodes have the same leader
    collect_leader_ids(&nodes_map);

    // Make sure that all nodes have the same data
    compare_node_snapshots(&nodes_map);
}

fn kill_nodes_until_majority_down_impl(
    base_listen_port: u16,
) -> (NodesMap, HashMap<Vec<u8>, Vec<u8>>, Client, Option<TempDir>) {
    let (nodes, listen_ports) = create_nodes(4, base_listen_port);
    let mut nodes_map = setup_nodes(nodes, listen_ports.clone());

    let nodes = listen_ports_to_socket_addrs(listen_ports);
    let mut client = Client::new(nodes);

    let test_data = generate_random_data(1500);
    // wait for cluster to start
    collect_leader_ids(&nodes_map);
    current_thread::block_on_all(future::join_all(
        test_data
            .iter()
            .take(500)
            .map(|(k, v)| client.set(k.clone(), v.clone())),
    )).unwrap();

    // Check that all data was set
    check_data_with_get(test_data.clone(), &mut client, None, Some(500));

    // Remove node with id 1
    let node_one_path = nodes_map.take_dir_and_remove(1);

    current_thread::block_on_all(future::join_all(
        test_data
            .iter()
            .skip(500)
            .take(500)
            .map(|(k, v)| client.set(k.clone(), v.clone())),
    )).unwrap();

    // Check that all data was set
    check_data_with_get(test_data.clone(), &mut client, Some(500), Some(500));

    // Remove node with id 2
    nodes_map.remove(&2);

    // wait until the majority is down
    wait_for_cluster_majority_down(&nodes_map);
    // Now the cluster majority should be gone
    assert_eq!(
        Error::ClusterMajorityDown,
        current_thread::block_on_all(future::join_all(
            test_data
                .iter()
                .skip(1000)
                .take(1)
                .map(|(k, v)| client.set(k.clone(), v.clone())),
        )).err()
        .unwrap()
    );

    (nodes_map, test_data, client, node_one_path)
}

#[test]
fn kill_nodes_until_majority_down() {
    kill_nodes_until_majority_down_impl(20080);
}

#[test]
fn kill_nodes_until_majority_down_and_bring_one_node_back() {
    let base_listen_port = 20090;
    let (mut nodes_map, test_data, mut client, node_one_path) =
        kill_nodes_until_majority_down_impl(base_listen_port);

    // restart our node with id 1
    nodes_map.restart_node(1, node_one_path, base_listen_port);
    // wait that all nodes know the leader
    collect_leader_ids(&nodes_map);

    current_thread::block_on_all(future::join_all(
        test_data
            .iter()
            .skip(1000)
            .take(500)
            .map(|(k, v)| client.set(k.clone(), v.clone())),
    )).unwrap();

    // Check that all data was set
    check_data_with_get(test_data.clone(), &mut client, None, None);
}
