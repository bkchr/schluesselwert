use error::*;

use raft::{
    self,
    eraftpb::{ConfState, Entry, HardState, Snapshot},
    storage::{self, RaftState},
    StorageError,
};

use rocksdb::{DBRawIterator, Options, DB};

use byteorder::{ByteOrder, LittleEndian};

use protobuf::Message;

use std::path::Path;

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
    /// path - The path where the data of the Node is stored/should be stored.
    fn new<T: AsRef<Path>>(path: T) -> Result<Storage> {
        let mut options = Options::default();
        options.create_if_missing(true);

        let db = DB::open(&options, path)?;
        let (first_index, last_index) = extract_first_and_last_index(&db)?;
        let (hard_state, conf_state) = extract_hard_and_conf_state(&db)?;

        Ok(Storage {
            db,
            first_index,
            last_index,
            hard_state,
            conf_state,
        })
    }

    /// Insert an entry into the storage
    fn insert_entry(&mut self, entry: &Entry) -> Result<()> {
        let key = get_key_for_entry_index(entry.index);
        let data = proto_message_as_bytes(entry)?;
        self.db.put(&key, &data).map_err(|e| e.into())
    }

    /// Get an entry from the storage
    fn get_entry(&self, idx: u64) -> Result<Option<Entry>> {
        let entry = self.db.get(&get_key_for_entry_index(idx))?;

        Ok(match entry {
            Some(entry) => Some(proto_message_from_bytes(&entry)?),
            None => None,
        })
    }

    /// Set the `HardState`.
    fn set_hard_state(&mut self, hard_state: HardState) -> Result<()> {
        self.db.put(HARD_STATE_KEY, &proto_message_as_bytes(&hard_state)?)?;
        self.hard_state = hard_state;
        Ok(())
    }

    /// Set the `ConfState`.
    fn set_conf_state(&mut self, conf_state: ConfState) -> Result<()> {
        self.db.put(CONF_STATE_KEY, &proto_message_as_bytes(&conf_state)?)?;
        self.conf_state = conf_state;
        Ok(())
    }

    /// Set the value for a key.
    pub fn set(&mut self, key: Vec<u8>, val: Vec<u8>) -> Result<()> {
        unimplemented!()
    }

    /// Get the value for a key.
    pub fn get(&mut self, key: Vec<u8>) -> Result<Vec<u8>> {
        unimplemented!()
    }

    /// Delete the value for a key.
    pub fn delete(&mut self, key: &[u8]) {}

    /// Scan for all keys.
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
        unimplemented!()
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

    fn snapshot(&self) -> raft::Result<Snapshot> {
        unimplemented!()
    }
}

fn get_key_for_entry_index(idx: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = ENTRY_KEY_PREFIX;
    LittleEndian::write_u64(&mut key[1..], idx);
    key
}

fn get_entry_index_from_key(key: &[u8]) -> Result<u64> {
    if key.len() != 9 || key[0] != ENTRY_KEY_PREFIX {
        Err(Error::InvalidEntryIndexKey)?;
    }

    Ok(LittleEndian::read_u64(&key[1..]))
}

fn proto_message_as_bytes(msg: &Message) -> Result<Vec<u8>> {
    msg.write_to_bytes().map_err(|e| e.into())
}

fn proto_message_from_bytes<T: Message + Default>(data: &[u8]) -> Result<T> {
    let mut msg = T::default();
    msg.merge_from_bytes(data)?;
    Ok(msg)
}

fn extract_first_and_last_index(db: &DB) -> Result<(Option<u64>, Option<u64>)> {
    let mut itr: DBRawIterator = db.prefix_iterator(&[ENTRY_KEY_PREFIX]).into();

    // If not valid -> new database
    if !itr.valid() {
        return Ok((None, None));
    } else {
        itr.seek_to_first();
        let first = get_entry_index_from_key(&itr.key().unwrap())?;

        itr.seek_to_last();
        let last = get_entry_index_from_key(&itr.key().unwrap())?;

        Ok((Some(first), Some(last)))
    }
}

fn extract_hard_and_conf_state(db: &DB) -> Result<(HardState, ConfState)> {
    let hard_state = match db.get(HARD_STATE_KEY)? {
        Some(data) => proto_message_from_bytes(&data)?,
        None => HardState::default(),
    };

    let conf_state = match db.get(CONF_STATE_KEY)? {
        Some(data) => proto_message_from_bytes(&data)?,
        None => ConfState::default(),
    };

    Ok((hard_state, conf_state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_key_to_bytes_and_back() {
        let idx = 1000;
        let key = get_key_for_entry_index(idx);

        assert_eq!(idx, get_entry_index_from_key(&key).unwrap());
    }

    #[test]
    fn entry_key_invalid_first_byte() {
        let idx = 1000;
        let mut key = get_key_for_entry_index(idx);
        key[0] = 45;

        assert_eq!(
            Error::InvalidEntryIndexKey,
            get_entry_index_from_key(&key).err().unwrap()
        );
    }

    #[test]
    fn entry_key_invalid_length() {
        let key: &[u8] = &[0, 1, 2, 3];
        assert_eq!(
            Error::InvalidEntryIndexKey,
            get_entry_index_from_key(&key).err().unwrap()
        );
    }

    #[test]
    fn entry_to_bytes_and_back() {
        let mut entry = Entry::new();
        entry.index = 1000;
        entry.term = 5432;

        let bytes = proto_message_as_bytes(&entry).unwrap();

        assert_eq!(entry, proto_message_from_bytes(&bytes).unwrap());
    }
}
