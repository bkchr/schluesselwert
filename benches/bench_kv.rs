#[macro_use]
extern crate criterion;
extern crate futures;
extern crate rand;
extern crate schluesselwert;
extern crate tempdir;
extern crate tokio;

#[path = "../tests/common/mod.rs"]
mod common;

use schluesselwert::Client;

use common::*;

use criterion::Criterion;

use tokio::executor::current_thread;

use futures::{future, Future};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("Set 50MB", |b| {
        b.iter(move || {
            let (nodes, listen_ports) = create_nodes(4, 20100);
            let _nodes_map = setup_nodes(nodes, listen_ports.clone());

            let nodes = listen_ports_to_socket_addrs(listen_ports);
            let mut client = Client::new(nodes);
            let data = generate_random_data_with_size(50 * 1024 * 1024);

            current_thread::block_on_all(future::join_all(
                data.iter().map(|(k, v)| client.set(k.clone(), v.clone())),
            )).unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
