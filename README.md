## SchluesselWert - High Availability Key Value Store

SchluesselWert is a key value store that uses [raft](https://github.com/pingcap/raft-rs) as consensus algorithm.
It is currently in development and not stable. It uses [Tokio](https://tokio.rs/) to handle requests asynchronously.

### Using it

Currently there is no binary available that will be build by Cargo. The current interface for talking to a 
cluster is to use the `Client` struct. This structure supports the basic operations like `Get`, `Set`, `Delete` and `Scan`.
It also supports adding and removing nodes from a cluster.

### Tests

There are several tests in the project that help to prove the correctness of the implementation. The unit tests can be found
in the individual files in the `src` directory. The integration tests are in the `tests` folder. 

### Benchmark

This implementation currently does not have any working benchmarks.

### TODO

* Use `WriteBatch` to batch multiple writes as one atomic operation. This should also increase the performance.
* Work on all the TODOs in the source code.
* Use less `unwraps`. The code uses unwraps in a lot of cases, this should be minimized.
* Merge incoming and outgoing `TCPStreams` into on structure.

### License

Licensed under either of

 * Apache License, Version 2.0
([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

License: MIT/Apache-2.0
