use error::*;

use raft::{
    self,
    eraftpb::{ConfState, Entry, HardState, Snapshot},
    storage::{self, RaftState},
    StorageError,
};

use rocksdb::DB;

use byteorder::{ByteOrder, LittleEndian};

use protobuf::Message;

const HARD_STATE_KEY: &[u8] = &[0x1];
const CONF_STATE_KEY: &[u8] = &[0x2];
const ENTRY_KEY_PREFIX: u8 = 0x4;
const DATA_KEY_PREFIX: u8 = 0x8;

pub struct Storage {
    db: DB,
    first_index: Option<u64>,
    last_index: Option<u64>,
    hard_state: HardState,
    conf_state: ConfState,
}

impl Storage {
    /// Create a new `Storage` instance
    fn new() -> Storage {}

    fn set_entry(&mut self, entry: &Entry) -> Result<()> {
        let key = get_key_for_entry_index(entry.index);
        let data = get_entry_as_bytes(entry)?;
        self.db.put(&key, &data).map_err(|e| e.into())
    }

    fn get_entry(&mut self, idx: u64) -> Result<Option<Entry>> {
        let entry = self.db.get(&get_key_for_entry_index(idx))?;

        Ok(match entry {
            Some(entry) => Some(create_entry_from_bytes(&entry)?),
            None => None,
        })
    }

    pub fn set(&mut self, key: Vec<u8>, val: Vec<u8>) -> Result<()> {}

    pub fn get(&mut self, key: Vec<u8>) -> Result<Vec<u8>> {}

    pub fn delete(&mut self, key: &[u8]) {}

    pub fn scan() {}
}

impl storage::Storage for Storage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard_state.clone(),
            conf_state: self.conf_state.clone(),
        })
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        Ok(Vec::new())
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        match self.last_index {
            Some(last_index) if idx <= last_index => match self.get_entry(idx)? {
                Some(entry) => Ok(entry.term),
                None => Err(StorageError::Unavailable)?,
            },
            _ => Err(StorageError::Unavailable)?,
        }
    }

    fn first_index(&self) -> raft::Result<u64> {
        self.first_index.ok_or(StorageError::Unavailable.into())
    }

    fn last_index(&self) -> raft::Result<u64> {
        self.last_index.ok_or(StorageError::Unavailable.into())
    }

    fn snapshot(&self) -> raft::Result<Snapshot> {}
}

fn get_key_for_entry_index(idx: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = ENTRY_KEY_PREFIX;
    LittleEndian::write_u64(&mut key[1..], idx);
    key
}

fn get_entry_as_bytes(entry: &Entry) -> Result<Vec<u8>> {
    entry.write_to_bytes().map_err(|e| e.into())
}

fn create_entry_from_bytes(data: &[u8]) -> Result<Entry> {
    let mut entry = Entry::new();
    entry.merge_from_bytes(data)?;
    Ok(entry)
}

fn get_entry_index_from_key(key: &[u8]) -> u64 {
    LittleEndian::read_u64(&key[1..])
}
