use super::storage::Storage;

use raft::raw_node::RawNode;

struct Node {
    node: RawNode<Storage>,
}
