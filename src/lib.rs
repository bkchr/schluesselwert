#[macro_use]
extern crate failure;
extern crate bincode;
extern crate byteorder;
extern crate protobuf;
extern crate raft;
#[cfg(test)]
extern crate rand;
extern crate rocksdb;
#[cfg(test)]
extern crate tempdir;
extern crate tokio;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate futures;
extern crate serde;
extern crate tokio_timer;

#[macro_use]
mod error;
mod node;
mod storage;
