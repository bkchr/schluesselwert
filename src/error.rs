pub use failure::ResultExt;
use failure::{self, Fail};

use std::{mem, result, io};

use rocksdb;

use protobuf;

use raft;

use bincode;

use tokio::timer;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "IO Error {}", _0)]
    Io(io::Error),
    #[fail(display = "RocksDB Error {}", _0)]
    RocksDB(rocksdb::Error),
    #[fail(display = "Tokio Timer Error {}", _0)]
    TokioTimer(timer::Error),
    #[fail(display = "Bincode Error {}", _0)]
    Bincode(bincode::Error),
    #[fail(display = "Raft Error {}", _0)]
    Raft(raft::Error),
    #[fail(display = "Error {}", _0)]
    Custom(failure::Error),
    #[fail(display = "Protobuf Error {}", _0)]
    Protobuf(protobuf::error::ProtobufError),
    #[fail(display = "Invalid entry index key")]
    InvalidEntryIndexKey,
    #[fail(display = "Invalid data key")]
    InvalidDataKey,
    #[fail(display = "Index of out bounds")]
    IndexOutOfBounds,
    #[fail(display = "Entry index is smaller than first index")]
    EntryIndexSmallerThanFirst,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        mem::discriminant(self) == mem::discriminant(other)
    }
}

impl From<Error> for raft::Error {
    fn from(err: Error) -> raft::Error {
        match err {
            Error::Raft(e) => e,
            // TODO: Hacky!
            e @ _ => raft::StorageError::Other(Box::new(e.compat())).into(),
        }
    }
}

impl From<failure::Error> for Error {
    fn from(err: failure::Error) -> Error {
        Error::Custom(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<rocksdb::Error> for Error {
    fn from(err: rocksdb::Error) -> Error {
        Error::RocksDB(err)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Error {
        Error::Bincode(err)
    }
}

impl From<raft::StorageError> for Error {
    fn from(err: raft::StorageError) -> Error {
        Error::Raft(err.into())
    }
}

impl From<raft::Error> for Error {
    fn from(err: raft::Error) -> Error {
        Error::Raft(err.into())
    }
}

impl From<timer::Error> for Error {
    fn from(err: timer::Error) -> Error {
        Error::TokioTimer(err.into())
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
