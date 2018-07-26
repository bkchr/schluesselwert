use failure::{self, Fail};
pub use failure::ResultExt;

use std::{mem, result };

use rocksdb;

use protobuf;

use raft;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "RocksDB Error {}", _0)]
    RocksDB(rocksdb::Error),
    #[fail(display = "Error {}", _0)]
    Custom(failure::Error),
    #[fail(display = "Protobuf Error {}", _0)]
    Protobuf(protobuf::error::ProtobufError),
    #[fail(display = "Invalid entry index key")]
    InvalidEntryIndexKey,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        mem::discriminant(self) == mem::discriminant(other)
    }
}

impl From<Error> for raft::Error {
    fn from(err: Error) -> raft::Error {
        // TODO: Hacky!
        raft::StorageError::Other(Box::new(err.compat())).into()
    }
}

impl From<failure::Error> for Error {
    fn from(err: failure::Error) -> Error {
        Error::Custom(err)
    }
}

impl From<rocksdb::Error> for Error {
    fn from(err: rocksdb::Error) -> Error {
        Error::RocksDB(err)
    }
}

impl From<protobuf::error::ProtobufError> for Error {
    fn from(err: protobuf::error::ProtobufError) -> Error {
        Error::Protobuf(err)
    }
}

impl From<&'static str> for Error {
    fn from(err: &'static str) -> Error {
        Error::Custom(::failure::err_msg::<&'static str>(err).into())
    }
}

//FIXME: Remove when upstream provides a better bail macro
macro_rules! bail {
    ($e:expr) => {
        return Err(::failure::err_msg::<&'static str>($e).into());
    };
    ($fmt:expr, $($arg:tt)+) => {
        return Err(::failure::err_msg::<String>(format!($fmt, $($arg)+)).into());
    };
}
