use std::net::SocketAddr;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum Protocol {
    Request {
        id: u64,
        data: Vec<u8>,
    },
    RequestResult {
        id: u64,
        res: RequestResult,
    },
    RequestChangeConf {
        id: u64,
        req: RequestChangeConf,
    },
    RequestChangeConfResult {
        id: u64,
    },
    Raft {
        msg: Vec<u8>,
    },
    /// The Node is not the leader.
    /// `leader_addr` is the address of the leader (if known).
    NotLeader {
        leader_addr: Option<SocketAddr>,
    },
    /// The majority of the cluster is down.
    ClusterMajorityDown {
        id: u64,
    },
}

/// The requests to the store.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum Request {
    /// Set a key and value.
    Set { key: Vec<u8>, value: Vec<u8> },
    /// Get a value.
    Get { key: Vec<u8> },
    /// Delete a key and value.
    Delete { key: Vec<u8> },
    /// Scan for all keys.
    Scan,
}

/// The results to the requests.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum RequestResult {
    Set { successful: bool },
    Get { value: Option<Vec<u8>> },
    Delete { successful: bool },
    Scan { keys: Option<Vec<Vec<u8>>> },
}

/// Requests that change the config of the cluster.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum RequestChangeConf {
    /// Add a node to the cluster.
    AddNode { node_id: u64, node_addr: SocketAddr },
    /// Remove a node from the cluster.
    RemoveNode { node_id: u64 },
}
