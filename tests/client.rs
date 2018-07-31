extern crate futures;
extern crate rand;
extern crate schluesselwert;
extern crate tempdir;
extern crate tokio;

mod common;

use common::{
    collect_leader_ids, compare_node_snapshots, create_node, create_nodes, generate_random_data,
    setup_nodes, setup_nodes_with_cluster_nodes, wait_for_snapshot_applied,
};

use schluesselwert::Client;

use std::{collections::HashMap, net::SocketAddr};

use tokio::executor::current_thread;

use futures::{future, Future};

fn listen_ports_to_socket_addrs(listen_ports: Vec<u16>) -> Vec<SocketAddr> {
    listen_ports
        .into_iter()
        .map(|p| ([127, 0, 0, 1], p).into())
        .collect()
}

fn check_data_with_get(mut test_data: HashMap<Vec<u8>, Vec<u8>>, client: &mut Client) {
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
    let _nodes_map = setup_nodes(nodes, listen_ports.clone());

    let nodes = listen_ports_to_socket_addrs(listen_ports);
    let mut client = Client::new(nodes);

    let test_data = generate_random_data(1000);
    current_thread::block_on_all(future::join_all(
        test_data
            .iter()
            .map(|(k, v)| client.set(k.clone(), v.clone())),
    )).unwrap();

    check_data_with_get(test_data, &mut client);
}

#[test]
fn set_delete_and_get_and_scan() {
    let (nodes, listen_ports) = create_nodes(5, 20040);
    let _nodes_map = setup_nodes(nodes, listen_ports.clone());

    let nodes = listen_ports_to_socket_addrs(listen_ports);
    let mut client = Client::new(nodes);

    let test_data = generate_random_data(1000);
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
    let _nodes_map = setup_nodes(nodes, listen_ports.clone());

    let nodes = listen_ports_to_socket_addrs(listen_ports);
    let mut client = Client::new(nodes);

    let mut test_data = generate_random_data(1000);
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
        setup_nodes_with_cluster_nodes(vec![new_node.clone()], vec![listen_port], nodes);
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
    check_data_with_get(test_data, &mut client);

    // just make sure that all nodes have the same leader
    collect_leader_ids(&nodes_map, None);

    wait_for_snapshot_applied(&nodes_map, 5);
    // Make sure that all nodes have the same data
    compare_node_snapshots(&nodes_map);
}
