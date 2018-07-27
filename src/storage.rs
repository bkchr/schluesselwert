use error::*;

use raft::{
    self,
    eraftpb::{ConfState, Entry, HardState, Snapshot},
    storage::RaftState,
    Storage as RStorage, StorageError,
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
        self.db.put(&key, &data)?;

        //TODO: Check that entries are unique.

        match self.first_index {
            Some(val) if val > entry.index => self.first_index = Some(entry.index),
            None => self.first_index = Some(entry.index),
            _ => {}
        };

        match self.last_index {
            Some(val) if val < entry.index => self.last_index = Some(entry.index),
            None => self.last_index = Some(entry.index),
            _ => {}
        };

        Ok(())
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
        self.db
            .put(HARD_STATE_KEY, &proto_message_as_bytes(&hard_state)?)?;
        self.hard_state = hard_state;
        Ok(())
    }

    /// Set the `ConfState`.
    fn set_conf_state(&mut self, conf_state: ConfState) -> Result<()> {
        self.db
            .put(CONF_STATE_KEY, &proto_message_as_bytes(&conf_state)?)?;
        self.conf_state = conf_state;
        Ok(())
    }

    /// Set the value for a key.
    pub fn set<T: Into<Vec<u8>>>(&mut self, key: T, val: &[u8]) -> Result<()> {
        let key = prefix_data_key(key);
        self.db.put(&key, val).map_err(|e| e.into())
    }

    /// Get the value for a key.
    pub fn get<T: Into<Vec<u8>>>(&self, key: T) -> Result<Option<Vec<u8>>> {
        let key = prefix_data_key(key);
        self.db
            .get(&key)
            .map_err(|e| e.into())
            .map(|r| r.map(|v| v.to_vec()))
    }

    /// Delete the value for a key.
    pub fn delete<T: Into<Vec<u8>>>(&mut self, key: T) -> Result<()> {
        let key = prefix_data_key(key);
        self.db.delete(&key).map_err(|e| e.into())
    }

    /// Scan for all keys.
    pub fn scan(&self) -> Vec<Vec<u8>> {
        self.db
            .prefix_iterator(&[DATA_KEY_PREFIX])
            .map(|v| v.0.to_vec())
            .collect()
    }
}

impl RStorage for Storage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard_state.clone(),
            conf_state: self.conf_state.clone(),
        })
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        if self.first_index.is_none() {
            return Err(StorageError::Unavailable)?;
        } else if high > self.last_index.unwrap() + 1 {
            return Err(Error::IndexOutOfBounds)?;
        }

        let mut itr: DBRawIterator = self.db.prefix_iterator(&[ENTRY_KEY_PREFIX]).into();
        let mut result = Vec::new();
        let mut size = 0;

        itr.seek(&get_key_for_entry_index(low));
        while let (Some(key), Some(value)) = (itr.key(), itr.value()) {
            // Check if we are above the `max_size`, have at least one result and `NO_LIMIT` is not
            // set. If the key is greater than the maximum requested, end it also.
            if max_size != raft::NO_LIMIT
                && !result.is_empty()
                && size + value.len() > max_size as usize
                || get_entry_index_from_key(&key)? >= high
            {
                break;
            }

            size += value.len();
            let entry = proto_message_from_bytes(&value)?;
            result.push(entry);
            itr.next();
        }

        Ok(result)
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

/// Enriches the data key with the `DATA_KEY_PREFIX`
fn prefix_data_key<T: Into<Vec<u8>>>(key: T) -> Vec<u8> {
    let mut key: Vec<u8> = key.into();
    key.insert(0, DATA_KEY_PREFIX);
    key
}

/// Converts an entry index into key
fn get_key_for_entry_index(idx: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = ENTRY_KEY_PREFIX;
    LittleEndian::write_u64(&mut key[1..], idx);
    key
}

/// Converts a key into an entry index
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
    use rand::{self, Rng, RngCore};
    use tempdir::TempDir;

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

    fn create_random_entries(num: usize) -> Vec<Entry> {
        let mut entries = Vec::with_capacity(num);

        let mut rng = rand::thread_rng();
        for i in 0..num {
            let mut entry = Entry::new();
            entry.index = i as u64;
            entry.term = rng.gen();

            let mut data = vec![0; 100];
            rng.fill_bytes(&mut data);
            entry.data = data;

            entries.push(entry);
        }

        entries
    }

    #[test]
    fn first_and_last_index_recreation() {
        let entries = create_random_entries(100);
        let dir = TempDir::new("first_last_recreation").unwrap();

        {
            let mut storage = Storage::new(&dir).unwrap();

            entries
                .into_iter()
                .for_each(|e| storage.insert_entry(&e).unwrap());

            assert_eq!(0, storage.first_index().unwrap());
            assert_eq!(99, storage.last_index().unwrap());
        }

        {
            // recreate the Storage
            let storage = Storage::new(&dir).unwrap();

            assert_eq!(0, storage.first_index().unwrap());
            assert_eq!(99, storage.last_index().unwrap());
        }
    }

    #[test]
    fn hard_and_conf_state_recreation() {
        let dir = TempDir::new("hard_conf_recreation").unwrap();
        let mut hard_state = HardState::new();
        hard_state.term = 456;
        hard_state.vote = 10;
        hard_state.commit = 40;

        let mut conf_state = ConfState::new();
        conf_state.nodes = vec![1, 2, 3, 4, 5];
        conf_state.learners = vec![6, 7];

        {
            let mut storage = Storage::new(&dir).unwrap();
            storage.set_hard_state(hard_state.clone()).unwrap();
            storage.set_conf_state(conf_state.clone()).unwrap();

            assert_eq!(storage.hard_state, hard_state);
            assert_eq!(storage.conf_state, conf_state);
        }

        {
            // recreate the Storage
            let storage = Storage::new(&dir).unwrap();
            assert_eq!(storage.hard_state, hard_state);
            assert_eq!(storage.conf_state, conf_state);
        }
    }

    #[test]
    fn entries_filter_with_no_limit() {
        let entries = create_random_entries(100);
        let dir = TempDir::new("entries_filter").unwrap();

        {
            let mut storage = Storage::new(&dir).unwrap();

            entries
                .iter()
                .for_each(|e| storage.insert_entry(&e).unwrap());

            assert_eq!(
                &entries[40..50],
                &storage.entries(40, 50, raft::util::NO_LIMIT).unwrap()[..]
            );
        }

        {
            // recreate the Storage
            let storage = Storage::new(&dir).unwrap();

            assert_eq!(
                &entries[40..50],
                &storage.entries(40, 50, raft::util::NO_LIMIT).unwrap()[..]
            );
        }
    }

    #[test]
    fn entries_filter_with_with_limit() {
        let entries = create_random_entries(100);
        let dir = TempDir::new("entries_filter").unwrap();
        let max_size = (entries.get(20).unwrap().compute_size()
            + entries.get(21).unwrap().compute_size()) as u64 + 20;

        {
            let mut storage = Storage::new(&dir).unwrap();

            entries
                .iter()
                .for_each(|e| storage.insert_entry(&e).unwrap());

            assert_eq!(
                &entries[20..22],
                &storage.entries(20, 30, max_size).unwrap()[..]
            );
        }

        {
            // recreate the Storage
            let storage = Storage::new(&dir).unwrap();

            assert_eq!(
                &entries[20..22],
                &storage.entries(20, 30, max_size).unwrap()[..]
            );
        }
    }

    #[test]
    fn entries_filter_with_with_small_limit() {
        let entries = create_random_entries(100);
        let dir = TempDir::new("entries_filter").unwrap();
        let max_size = 20;

        {
            let mut storage = Storage::new(&dir).unwrap();

            entries
                .iter()
                .for_each(|e| storage.insert_entry(&e).unwrap());

            assert_eq!(
                &entries[20..21],
                &storage.entries(20, 30, max_size).unwrap()[..]
            );
        }

        {
            // recreate the Storage
            let storage = Storage::new(&dir).unwrap();

            assert_eq!(
                &entries[20..21],
                &storage.entries(20, 30, max_size).unwrap()[..]
            );
        }
    }

    #[test]
    fn entries_filter_with_no_result() {
        let entries = create_random_entries(100);
        let dir = TempDir::new("entries_filter").unwrap();

        let mut storage = Storage::new(&dir).unwrap();

        entries
            .iter()
            .for_each(|e| storage.insert_entry(&e).unwrap());

        assert!(
            &storage
                .entries(20, 20, raft::util::NO_LIMIT)
                .unwrap()
                .is_empty()
        );
    }
}
