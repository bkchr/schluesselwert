#[macro_use]
extern crate failure;
extern crate byteorder;
extern crate raft;
extern crate rocksdb;
extern crate tokio;
extern crate protobuf;
#[cfg(test)]
extern crate tempdir;
#[cfg(test)]
extern crate rand;

#[macro_use]
mod error;
mod node;
mod storage;
