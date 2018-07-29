#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum Protocol {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Get {
        key: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    Scan,
    /// The result of a `Set`, `Get` or `Delete` operation.
    Result {
        /// Was the operation successful?
        successful: bool,
        /// The requested value of the `Get` operation.
        value: Option<Vec<u8>>,
    },
    /// The result of a scan operation.
    ScanResult {
        keys: Vec<Vec<u8>>,
    },
    Raft {
        msg: Vec<u8>,
    },
}
