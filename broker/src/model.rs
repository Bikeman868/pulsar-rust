/*
Defines the shared internal state of the application. Some of this state is
persisted to the database, and some is just updated in memory.
To recover the proper state after a restart, the applications persists an event
log that can be replayed at startup.
*/

pub mod cluster;
pub mod node;
pub mod topic;
pub mod partition;
pub mod ledger;
pub mod subscription;
pub mod messages;
pub mod request_mapping;
pub mod response_mapping;

#[derive(Debug)]
pub enum RefreshStatus {
    /// Entity was successfully refreshed from the database
    Updated,

    /// Entity contains stale data because the refresh failed
    Stale,

    /// This entity was not found in the database
    Deleted,
}
