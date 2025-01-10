use super::*;

use std::hash::Hash;
use std::cmp::Eq;
// let (persist_completed_send_ch, mut persist_completed_rx) =
// tokio::sync::mpsc::channel::<RKey>(MAX_PRESIST_TASKS as usize);

use crate::service::stats::Waits;
use aws_sdk_dynamodb::Client as DynamoClient;
//use crate::rkey::RKey_New;
#[trait_variant::make(Persistence: Send)]
pub trait Persistence_<K> {

    async fn persist(
        &mut self
        ,dyn_client: &DynamoClient
        ,table_name_: String
        ,waits : Waits
        ,persist_completed_send_ch : tokio::sync::mpsc::Sender<K>
    );
}

// =======================
//  Generic Cache that supports persistence
// =======================
// cache responsibility is to synchronise access to db across multiple Tokio tasks on a single cache entry.
// The state of the node edge will determine the type of update required, either embedded or OvB.
// Each cache update will be saved to db to keep both in sync.
// All mutations of the cache hashmap need to be serialized.
pub struct Cache<K,V>(pub HashMap<K, Arc<tokio::sync::Mutex<V>>>);

impl<K : Hash + Eq, V: Persistence<K> + Clone >  Cache<K,V>
{

    pub fn new() -> Arc<Mutex<Cache<K,V>>> {
        Arc::new(Mutex::new(Cache::<K,V>(HashMap::new())))
    }
}
