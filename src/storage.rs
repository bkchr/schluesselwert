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

use bincode;

const HARD_STATE_KEY: &[u8] = &[0x1];
const CONF_STATE_KEY: &[u8] = &[0x2];
const LAST_APPLIED_INDEX_KEY: &[u8] = &[0x4];
const COMPACT_ENTRY_KEY: &[u8] = &[0x8];
const ENTRY_KEY_PREFIX: u8 = 0x10;
const DATA_KEY_PREFIX: u8 = 0x20;

/// Corresponds to an entry before all other entries are deleted.
#[derive(Serialize, Deserialize, Default)]
struct CompactEntry {
    index: u64,
    term: u64,
}

//TODO: Make write/delete more atomic! (WriteBatch)
pub struct Storage {
    db: DB,
    last_index: Option<u64>,
    hard_state: HardState,
    conf_state: ConfState,
    last_applied_index: u64,
    compact_entry: CompactEntry,
}

impl Storage {
    /// Create a new `Storage` instance
    /// path - The path where the data of the Node is stored/should be stored.
    pub fn new<T: AsRef<Path>>(path: T) -> Result<Storage> {
        let mut options = Options::default();
        options.create_if_missing(true);

        let db = DB::open(&options, path)?;
        let last_index = extract_last_index(&db)?;
        let (hard_state, conf_state) = extract_hard_and_conf_state(&db)?;
        let last_applied_index = extract_last_applied_index(&db)?;
        let compact_entry = extract_compact_entry(&db)?;

        Ok(Storage {
            db,
            last_index,
            hard_state,
            conf_state,
            last_applied_index,
            compact_entry,
        })
    }

    /// Append an entry into the storage.
    /// The entry index is not allowed to be smaller than `self.first_index`.
    fn append_entry(&mut self, entry: &Entry) -> Result<()> {
        let key = get_key_for_entry_index(entry.index);
        let data = proto_message_as_bytes(entry)?;
        self.db.put(&key, &data)?;

        self.last_index = Some(entry.index);

        Ok(())
    }

    /// Append a list of entries.
    pub fn append_entries(&mut self, entries: &[Entry]) -> Result<()> {
        let last_index = self.last_index;

        entries.iter().try_for_each(|e| self.append_entry(e))?;

        if let (Some(prev_last_index), Some(last_index)) = (last_index, self.last_index) {
            // delete entries that are not valid anymore
            for index in (last_index + 1)..prev_last_index {
                self.db.delete(&get_key_for_entry_index(index))?;
            }
        }

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
    pub fn set_hard_state(&mut self, hard_state: HardState) -> Result<()> {
        self.db
            .put(HARD_STATE_KEY, &proto_message_as_bytes(&hard_state)?)?;
        self.hard_state = hard_state;
        Ok(())
    }

    /// Set the `ConfState`.
    /// idx - The index of the `Entry` that requested this operation.
    fn set_conf_state(&mut self, conf_state: ConfState, idx: u64) -> Result<()> {
        self.db
            .put(CONF_STATE_KEY, &proto_message_as_bytes(&conf_state)?)?;
        self.conf_state = conf_state;

        self.set_last_applied_index(idx)?;
        Ok(())
    }

    /// Set the value for a key.
    /// idx - The index of the `Entry` that requested this operation.
    pub fn set<T: Into<Vec<u8>>>(&mut self, key: T, val: &[u8], idx: u64) -> Result<()> {
        let key = prefix_data_key(key);
        self.db.put(&key, val)?;

        self.set_last_applied_index(idx)?;
        Ok(())
    }

    /// Get the value for a key.
    /// idx - The index of the `Entry` that requested this operation.
    pub fn get<T: Into<Vec<u8>>>(&mut self, key: T, idx: u64) -> Result<Option<Vec<u8>>> {
        let key = prefix_data_key(key);
        let res = self.db.get(&key).map(|r| r.map(|v| v.to_vec()))?;

        self.set_last_applied_index(idx)?;
        Ok(res)
    }

    /// Delete the value for a key.
    /// idx - The index of the `Entry` that requested this operation.
    pub fn delete<T: Into<Vec<u8>>>(&mut self, key: T, idx: u64) -> Result<()> {
        let key = prefix_data_key(key);
        self.db.delete(&key)?;

        self.set_last_applied_index(idx)?;
        Ok(())
    }

    /// Scan for all keys.
    /// idx - The index of the `Entry` that requested this operation.
    pub fn scan(&mut self, idx: u64) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        for (key, _) in self.db.prefix_iterator(&[DATA_KEY_PREFIX]) {
            keys.push(remove_data_key_prefix(&key)?);
        }

        self.set_last_applied_index(idx)?;

        Ok(keys)
    }

    /// Set the last applied index.
    fn set_last_applied_index(&mut self, index: u64) -> Result<()> {
        let mut val = [0; 8];
        LittleEndian::write_u64(&mut val, index);

        self.db.put(LAST_APPLIED_INDEX_KEY, &val)?;
        self.last_applied_index = index;
        Ok(())
    }

    /// Apply the given `Snapshot`.
    pub fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        let last_applied_index = snapshot.get_metadata().get_index();
        let term = snapshot.get_metadata().get_term();
        let conf_state = snapshot.get_metadata().get_conf_state();

        if self.last_applied_index >= last_applied_index {
            return Err(StorageError::SnapshotOutOfDate)?;
        }

        self.compact_entry.term = term;
        self.compact_entry.index = last_applied_index;
        self.db
            .put(COMPACT_ENTRY_KEY, &bincode::serialize(&self.compact_entry)?)?;

        self.set_conf_state(conf_state.clone(), last_applied_index)?;

        self.last_index = None;
        self.delete_all_keys_with_prefix(&[ENTRY_KEY_PREFIX])?;
        self.delete_all_keys_with_prefix(&[DATA_KEY_PREFIX])?;

        let data: Vec<KeyValue> = bincode::deserialize(snapshot.get_data())?;
        data.into_iter()
            .try_for_each(|d| self.db.put(&d.key, &d.value))
            .map_err(|e| e.into())
    }

    /// Delete all keys with the given prefix.
    fn delete_all_keys_with_prefix(&mut self, prefix: &[u8]) -> Result<()> {
        self.db
            .prefix_iterator(prefix)
            .try_for_each(|(k, _)| self.db.delete(&k))
            .map_err(|e| e.into())
    }

    /// Returns the index of the last applied entry.
    pub fn get_last_applied_index(&self) -> u64 {
        self.last_applied_index
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
        if self.last_index.is_none() {
            Err(StorageError::Unavailable)?;
        } else if high > self.last_index.unwrap() + 1 {
            Err(Error::IndexOutOfBounds)?;
        } else if low <= self.compact_entry.index {
            Err(StorageError::Compacted)?;
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
            None => Ok(self.compact_entry.term),
            _ => Err(StorageError::Unavailable)?,
        }
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.compact_entry.index + 1)
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_index.unwrap_or(self.compact_entry.index))
    }

    fn snapshot(&self) -> raft::Result<Snapshot> {
        let mut data = Vec::new();
        for (key, value) in self.db.prefix_iterator(&[DATA_KEY_PREFIX]) {
            data.push(KeyValue::new(&key, &value));
        }

        let mut snapshot = Snapshot::new();
        snapshot.mut_metadata().set_index(self.last_applied_index);
        snapshot
            .mut_metadata()
            .set_term(self.term(self.last_applied_index)?);
        snapshot
            .mut_metadata()
            .set_conf_state(self.conf_state.clone());
        snapshot.set_data(bincode::serialize(&data).unwrap());

        Ok(snapshot)
    }
}

#[derive(Serialize, Deserialize)]
struct KeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl KeyValue {
    fn new(key: &[u8], value: &[u8]) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Enriches the data key with the `DATA_KEY_PREFIX`
fn prefix_data_key<T: Into<Vec<u8>>>(key: T) -> Vec<u8> {
    let mut key: Vec<u8> = key.into();
    key.insert(0, DATA_KEY_PREFIX);
    key
}

/// Remove `DATA_KEY_PREFIX` from the given key.
fn remove_data_key_prefix(key: &[u8]) -> Result<Vec<u8>> {
    if key[0] != DATA_KEY_PREFIX {
        Err(Error::InvalidDataKey)?;
    }

    Ok(key[1..].to_vec())
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

fn extract_last_index(db: &DB) -> Result<Option<u64>> {
    let mut itr = db.raw_iterator();

    itr.seek_for_prev(&[DATA_KEY_PREFIX]);
    // If not valid -> new database
    if !itr.valid() {
        return Ok(None);
    } else {
        let key = itr.key().unwrap();

        if key[0] == ENTRY_KEY_PREFIX {
            let last = get_entry_index_from_key(&itr.key().unwrap())?;

            Ok(Some(last))
        } else {
            Ok(None)
        }
    }
}

/// Extract the hard and conf state.
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

/// Extract the last applied index.
fn extract_last_applied_index(db: &DB) -> Result<u64> {
    let last_applied_index = match db.get(LAST_APPLIED_INDEX_KEY)? {
        Some(data) => LittleEndian::read_u64(&data),
        None => 0,
    };

    Ok(last_applied_index)
}

/// Extract the `CompactEntry`.
fn extract_compact_entry(db: &DB) -> Result<CompactEntry> {
    let compact_entry = match db.get(COMPACT_ENTRY_KEY)? {
        Some(data) => bincode::deserialize(&data)?,
        None => CompactEntry::default(),
    };

    Ok(compact_entry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{self, Rng, RngCore};
    use std::collections::HashMap;
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
    fn prefix_data_key_and_back() {
        let data_key = vec![1, 2, 3, 4];
        let key = prefix_data_key(data_key.clone());

        assert_eq!(&data_key[..], &remove_data_key_prefix(&key).unwrap()[..]);
    }

    #[test]
    fn prefix_data_key_invalid_first_byte() {
        let data_key = vec![1, 2, 3, 4];
        let mut key = prefix_data_key(data_key);
        key[0] = 45;

        assert_eq!(
            Error::InvalidDataKey,
            remove_data_key_prefix(&key).err().unwrap()
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
        for i in 1..=num {
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

    fn create_random_data(num: usize) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut data = HashMap::new();

        let mut rng = rand::thread_rng();
        for _ in 0..num {
            let mut key = vec![0; 50];
            let mut value = vec![0; 200];

            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut value);

            data.insert(key, value);
        }

        data
    }

    fn create_random_storage<T: AsRef<Path>>(
        path: T,
        num_entries: usize,
        num_data: usize,
    ) -> (Storage, Vec<Entry>, HashMap<Vec<u8>, Vec<u8>>) {
        let entries = create_random_entries(num_entries);
        let data = create_random_data(num_data);

        let mut storage = Storage::new(path).unwrap();

        storage.append_entries(&entries).unwrap();

        data.iter()
            .enumerate()
            .for_each(|(i, (k, v))| storage.set(k.clone(), v, (i + 1) as u64).unwrap());

        (storage, entries, data)
    }

    #[test]
    fn first_and_last_index_recreation() {
        let dir = TempDir::new("first_last_recreation").unwrap();

        {
            let (storage, _, _) = create_random_storage(&dir, 100, 100);

            assert_eq!(1, storage.first_index().unwrap());
            assert_eq!(100, storage.last_index().unwrap());
        }

        {
            // recreate the Storage
            let storage = Storage::new(&dir).unwrap();

            assert_eq!(1, storage.first_index().unwrap());
            assert_eq!(100, storage.last_index().unwrap());
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
            let (mut storage, _, _) = create_random_storage(&dir, 100, 100);
            storage.set_hard_state(hard_state.clone()).unwrap();
            storage.set_conf_state(conf_state.clone(), 101).unwrap();

            assert_eq!(storage.hard_state, hard_state);
            assert_eq!(storage.conf_state, conf_state);
        }

        {
            // recreate the Storage
            let storage = Storage::new(&dir).unwrap();
            assert_eq!(storage.hard_state, hard_state);
            assert_eq!(storage.conf_state, conf_state);
            assert_eq!(1, storage.first_index().unwrap());
            assert_eq!(100, storage.last_index().unwrap());
            assert_eq!(101, storage.last_applied_index);
        }
    }

    #[test]
    fn entries_filter_with_no_limit() {
        let dir = TempDir::new("entries_filter").unwrap();

        let entries = {
            let (storage, entries, _) = create_random_storage(&dir, 100, 100);

            assert_eq!(
                &entries[39..49],
                &storage.entries(40, 50, raft::util::NO_LIMIT).unwrap()[..]
            );

            entries
        };

        {
            // recreate the Storage
            let storage = Storage::new(&dir).unwrap();

            assert_eq!(
                &entries[39..49],
                &storage.entries(40, 50, raft::util::NO_LIMIT).unwrap()[..]
            );
            assert_eq!(1, storage.first_index().unwrap());
            assert_eq!(100, storage.last_index().unwrap());
        }
    }

    #[test]
    fn entries_filter_with_with_limit() {
        let dir = TempDir::new("entries_filter").unwrap();

        let (entries, max_size) = {
            let (storage, entries, _) = create_random_storage(&dir, 100, 100);
            let max_size = (entries.get(20).unwrap().compute_size()
                + entries.get(21).unwrap().compute_size()) as u64 + 20;

            assert_eq!(
                &entries[19..21],
                &storage.entries(20, 30, max_size).unwrap()[..]
            );

            (entries, max_size)
        };

        {
            // recreate the Storage
            let storage = Storage::new(&dir).unwrap();

            assert_eq!(
                &entries[19..21],
                &storage.entries(20, 30, max_size).unwrap()[..]
            );
        }
    }

    #[test]
    fn entries_filter_with_with_small_limit() {
        let dir = TempDir::new("entries_filter").unwrap();
        let max_size = 20;

        let entries = {
            let (storage, entries, _) = create_random_storage(&dir, 100, 100);

            assert_eq!(
                &entries[19..20],
                &storage.entries(20, 30, max_size).unwrap()[..]
            );

            entries
        };

        {
            // recreate the Storage
            let storage = Storage::new(&dir).unwrap();

            assert_eq!(
                &entries[19..20],
                &storage.entries(20, 30, max_size).unwrap()[..]
            );
        }
    }

    #[test]
    fn entries_filter_with_no_result() {
        let dir = TempDir::new("entries_filter").unwrap();

        let (storage, _, _) = create_random_storage(&dir, 100, 100);

        assert!(
            &storage
                .entries(20, 20, raft::util::NO_LIMIT)
                .unwrap()
                .is_empty()
        );
    }

    fn check_data(
        storage: &mut Storage,
        data: &HashMap<Vec<u8>, Vec<u8>>,
        skip: usize,
        take: usize,
        exist: bool,
    ) {
        let last_applied_index = storage.last_applied_index;
        data.iter().skip(skip).take(take).for_each(|(k, v)| {
            if exist {
                assert_eq!(
                    &storage.get(k.clone(), last_applied_index).unwrap().unwrap()[..],
                    &v[..]
                );
            } else {
                assert_eq!(storage.get(k.clone(), last_applied_index).unwrap(), None);
            }
        });
    }

    #[test]
    fn get_data() {
        let dir = TempDir::new("entries_filter").unwrap();

        let data = {
            let (mut storage, _, data) = create_random_storage(&dir, 100, 100);

            check_data(&mut storage, &data, 0, 100, true);

            data
        };

        {
            // recreate the Storage
            let mut storage = Storage::new(&dir).unwrap();

            check_data(&mut storage, &data, 0, 100, true);
        }
    }

    #[test]
    fn delete_half() {
        let dir = TempDir::new("entries_filter").unwrap();

        let data = {
            let (mut storage, _, data) = create_random_storage(&dir, 100, 100);

            data.iter().take(50).enumerate().for_each(|(i, (k, _))| {
                storage.delete(k.clone(), (100 + i) as u64).unwrap();
            });

            check_data(&mut storage, &data, 50, 50, true);
            check_data(&mut storage, &data, 0, 50, false);

            data
        };

        {
            // recreate the Storage
            let mut storage = Storage::new(&dir).unwrap();

            check_data(&mut storage, &data, 50, 50, true);
            check_data(&mut storage, &data, 0, 50, false);
        }
    }

    #[test]
    fn scan() {
        let dir = TempDir::new("scan_all").unwrap();

        let data = {
            let (mut storage, _, data) = create_random_storage(&dir, 100, 100);

            let keys = storage.scan(101).unwrap();
            let mut data2 = data.clone();
            keys.into_iter()
                .for_each(|k| assert!(data2.remove(&k).is_some()));
            assert!(data2.is_empty());

            data
        };

        {
            // recreate the Storage
            let mut storage = Storage::new(&dir).unwrap();

            let keys = storage.scan(101).unwrap();
            let mut data2 = data.clone();
            keys.into_iter()
                .for_each(|k| assert!(data2.remove(&k).is_some()));
            assert!(data2.is_empty());
        }
    }

    #[test]
    fn term() {
        let dir = TempDir::new("term").unwrap();
        let (storage, entries, _) = create_random_storage(&dir, 100, 100);

        entries
            .iter()
            .for_each(|e| assert_eq!(e.term, storage.term(e.index).unwrap()));

        assert_eq!(
            raft::Error::Store(StorageError::Unavailable),
            storage.term(200).err().unwrap()
        );
    }

    #[test]
    fn append_entries_overwrite() {
        let dir = TempDir::new("append_overwrite").unwrap();
        let (mut storage, _, _) = create_random_storage(&dir, 200, 100);

        assert!(storage.get_entry(150).unwrap().is_some());

        let new_entries = create_random_entries(100);
        storage.append_entries(&new_entries).unwrap();

        assert_eq!(None, storage.get_entry(150).unwrap());
        assert_eq!(100, storage.last_index().unwrap());
    }

    #[test]
    fn empty_storage() {
        let dir = TempDir::new("empty_storage").unwrap();
        let storage = Storage::new(&dir).unwrap();

        assert_eq!(1, storage.first_index().unwrap());
        assert_eq!(0, storage.last_index().unwrap());
    }

    #[test]
    fn snapshot_create_and_apply() {
        let dir0 = TempDir::new("snapshot_0").unwrap();
        let dir1 = TempDir::new("snapshot_1").unwrap();
        let (storage, _, data) = create_random_storage(&dir0, 100, 100);
        assert_eq!(100, storage.last_applied_index);
        assert!(storage.get_entry(50).unwrap().is_some());

        let snapshot = storage.snapshot().unwrap();

        {
            let mut storage2 = Storage::new(&dir1).unwrap();
            storage2.apply_snapshot(&snapshot).unwrap();
            assert_eq!(100, storage2.last_applied_index);
            assert_eq!(101, storage2.first_index().unwrap());
            assert_eq!(None, storage2.get_entry(50).unwrap());
            check_data(&mut storage2, &data, 0, 100, true);
        }
        {
            let mut storage2 = Storage::new(&dir1).unwrap();
            assert_eq!(100, storage2.last_applied_index);
            assert_eq!(101, storage2.first_index().unwrap());
            assert_eq!(None, storage2.get_entry(50).unwrap());
            check_data(&mut storage2, &data, 0, 100, true);
        }
    }

    #[test]
    fn snapshot_create_and_apply_with_overwrite() {
        let dir0 = TempDir::new("snapshot_0").unwrap();
        let dir1 = TempDir::new("snapshot_1").unwrap();
        let (storage, _, data) = create_random_storage(&dir0, 100, 100);
        assert_eq!(100, storage.last_applied_index);
        assert!(storage.get_entry(50).unwrap().is_some());

        let snapshot = storage.snapshot().unwrap();

        let (mut storage2, _, _) = create_random_storage(&dir1, 50, 50);
        storage2.apply_snapshot(&snapshot).unwrap();
        assert_eq!(100, storage2.last_applied_index);
        assert_eq!(101, storage2.first_index().unwrap());
        assert_eq!(None, storage2.get_entry(50).unwrap());
        check_data(&mut storage2, &data, 0, 100, true);
    }
}
