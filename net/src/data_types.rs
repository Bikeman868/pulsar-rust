/*
Aliases for data types for readability and to allow resizing in future
*/

pub type ContractVersionNumber = u16; // Up to 65 thousand versions of the API
pub type Timestamp = u64; // Compatible with Rust time library
pub type VersionNumber = u32; // Allows 4 billion updates to each database record
pub type PortNumber = u16; // Conforms to TCP/IP port numbering
pub type MessageCount = u16; // The number of messages to consume
pub type ErrorCode = u16; // Numeric value returned with error responses to identify the specific error

pub type NodeId = u16; // Maximum of 65 thousand nodes in a cluster
pub type TopicId = u32; // Up to 4 billion topics per cluster
pub type PartitionId = u16; // Up to 65 thousand partitions per topic
pub type LedgerId = u32; // Up to 4 billion ledgers per partition
pub type MessageId = u32; // // Up to 4 billion messages per ledger
pub type SubscriptionId = u32; // // Up to 4 billion subscriptions per topic
pub type ConsumerId = u64; // Allows us to create 1 million consumers per second for about half a million years
