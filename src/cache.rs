use super::*;

use std::hash::Hash;
use std::cmp::Eq;
use std::fmt::Debug;
use std::collections::HashSet;

use service::lru::LruAction;
// let (persist_completed_send_ch, mut persist_completed_rx) =
// tokio::sync::mpsc::channel::<RKey>(MAX_PRESIST_TASKS as usize);

use crate::service::stats::Waits;
use aws_sdk_dynamodb::Client as DynamoClient;
//use crate::rkey::RKey_New;

pub enum CacheValue<V> {
    New(V),
    Existing(V),
}


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

pub trait NewValue<K: Clone,V> {

    fn new_with_key(key : &K) -> Arc<tokio::sync::Mutex<V>>;
}

// =======================
//  Generic Cache that supports persistence
// =======================
// cache responsibility is to synchronise access to db across multiple Tokio tasks on a single cache entry.
// The state of the node edge will determine the type of update required, either embedded or OvB.
// Each cache update will be saved to db to keep both in sync.
// All mutations of the cache hashmap need to be serialized.
#[derive(Debug)]
pub struct InnerCache<K,V>{
    pub datax : HashMap<K, Arc<tokio::sync::Mutex<V>>>,
    // channels
    persist_query_ch : tokio::sync::mpsc::Sender<QueryMsg<K>>,
    lru_ch : tokio::sync::mpsc::Sender<( K, Instant, tokio::sync::mpsc::Sender<bool>, LruAction)>,
    // state of K in cache
    evicted : HashSet<K>,
    inuse : HashSet<K>,
    persisting: HashSet<K>,
    // performance stats rep
    waits : Waits,
}

#[derive(Debug)]
pub struct Cache<K,V>(pub Arc<Mutex<InnerCache<K,V>>>);


impl<K : Hash + Eq + Debug, V: Persistence<K> + Clone + Debug>  Cache<K,V>
{

    pub fn new(
        persist_query_ch : tokio::sync::mpsc::Sender<QueryMsg<K>>
        ,lru_ch : tokio::sync::mpsc::Sender<( K, Instant, tokio::sync::mpsc::Sender<bool>, LruAction)>
        ,waits : Waits
    ) -> Self
   {
        Cache::<K,V>(Arc::new(tokio::sync::Mutex::new(InnerCache::<K,V>{
            datax: HashMap::new()
            ,persist_query_ch
            ,lru_ch
            //
            ,evicted : HashSet::new()
            ,inuse : HashSet::new()
            ,persisting: HashSet::new()
            //
            ,waits
            })))
    }
}

impl<K : Hash + Eq + Debug,V>  InnerCache<K,V>
{
    pub fn unlock(&mut self, key: &K) {
        println!("InnerCache unlock [{:?}]",key);
        self.unset_inuse(key);
    }

    pub fn set_inuse(&mut self, key: K) {
        println!("InnerCache set_inuse [{:?}]",key);
        self.inuse.insert(key);
    }

    pub fn unset_inuse(&mut self, key: &K) {
        println!("InnerCache unset_inuse [{:?}]",key);
        self.inuse.remove(key);
    }

    pub fn inuse(&self, key: &K) -> bool {
        println!("InnerCache inuse [{:?}]",key);
        match self.inuse.get(key) {
            None => false,
            Some(_) => true,
        }
    }

    pub fn set_persisting(&mut self, key: K) {
        self.persisting.insert(key);
    }

    pub fn unset_persisting(&mut self, key: &K) {
        self.persisting.remove(key);
    }

    pub fn persisting(&self, key: &K) -> bool {
        match self.persisting.get(key) {
            None => false,
            Some(_) => true,
        }
    }
    
    pub fn set_evicted(&mut self, key: K) {
        self.evicted.insert(key);
    }

    pub fn unset_evicted(&mut self, key: &K) {
        self.evicted.remove(key);
    }

    pub fn evicted(&self, key: &K) -> bool {
        match self.evicted.get(key) {
            None => false,
            Some(_) => true,
        }
    }

}

impl<K,V> Clone for Cache<K,V> {

    fn clone(&self) -> Self {
        Cache::<K,V>(self.0.clone())
    }
}

impl<K: Hash + Eq + Clone + Debug, V: Persistence<K> + Clone + NewValue<K,V> + Debug>  Cache<K,V>
{

    pub async fn unlock(&mut self, key: &K) {
        println!("cache.unlock {:?}",key);
        self.0.lock().await.unset_inuse(key);
    }

    pub async fn get(
        self
        ,key : &K
    ) -> CacheValue<Arc<tokio::sync::Mutex<V>>> {
        let (lru_client_ch, mut srv_resp_rx) = tokio::sync::mpsc::channel::<bool>(1); 

        let before:Instant;  
        let mut cache_guard = self.0.lock().await;
        match cache_guard.datax.get(&key) {
            
            None => {
                //println!("RKEY add_reverse_edge: - Not Cached: rkey {:?}", task, self);
                // acquire lock on value and release cache lock - this prevents concurrent updates to value 
                // and optimises cache concurrency by releasing lock asap
                let arc_value = V::new_with_key(key);
                // ===============================================================
                // add to cache, set Rkey,Value entry to in-use - release lock
                // ===============================================================
                cache_guard.datax.insert(key.clone(), arc_value.clone()); // self.clone(), arc_value.clone());
                //let value_guard = arc_value.lock().await;
                // ==================
                // mark key as inuse - prevents eviction
                // ==================
                cache_guard.set_inuse(key.clone());
                //drop(value_guard);
                //
                let lru_ch=cache_guard.lru_ch.clone();
                let waits = cache_guard.waits.clone();
                let persist_query_ch = cache_guard.persist_query_ch.clone();
                let persisting = cache_guard.persisting(&key);
                //let persisting = cache_guard.persisting(&key);
                drop(cache_guard);
                if persisting {
                    // =======================
                    // IS NODE BEING PERSISTED 
                    // =======================
                    self.wait_for_persist_to_complete(key.clone(),persist_query_ch, waits.clone()).await;
                }
                before =Instant::now();
                if let Err(err) = lru_ch.send((key.clone(), before, lru_client_ch, LruAction::Attach)).await {
                    panic!("Send on lru_attach_ch errored: {}", err);
                }   
                waits.record(Event::LRUSendAttach,Instant::now().duration_since(before)).await;    
                // sync'd: wait for LRU operation to complete - just like using a mutex is synchronous with operation.
                let _ = srv_resp_rx.recv().await;
                waits.record(Event::Attach,Instant::now().duration_since(before)).await; 

                return CacheValue::New(arc_value.clone());
            }
            
            Some(value_) => {

                //println!("key add_reverse_edge: - Cached key {:?}", task, self);
                // acquire lock on value and release cache lock - this prevents concurrent updates to value 
                // and optimises cache concurrency by releasing lock asap
                let arc_value=value_.clone();
                let persist_query_ch = cache_guard.persist_query_ch.clone();
                let lru_ch=cache_guard.lru_ch.clone();
                let waits = cache_guard.waits.clone();
                let persisting = cache_guard.persisting(&key);
                //drop(cache_guard);
                // acqure lock on node. Concurrent task, Evict, may have lock.
                //let mut value_guard = arc_value.lock().await;  
                println!("CACHE: - Cached key about to CHECK EVICTED STATUS {:?}", key);
                // ======================
                // IS NODE BEING EVICTED 
                // ======================
                if cache_guard.evicted(&key) {
                    cache_guard.unset_evicted(&key);
                    drop(cache_guard);
                    if persisting {
                        self.wait_for_persist_to_complete(key.clone(),persist_query_ch, waits.clone()).await;
                    }

                    // if so, must wait for the evict-persist process to complete - setup comms with persist.
                    println!("CACHE key: node read from cache but detected it has been evicted....{:?}", key);
                    
                    before =Instant::now();
                    if let Err(err)= lru_ch.send((key.clone(), before, lru_client_ch, LruAction::Attach)).await {
                        panic!("Send on lru_attach_ch failed {}",err)
                    };
                    waits.record(Event::LRUSendAttach,Instant::now().duration_since(before)).await;
                    let _ = srv_resp_rx.recv().await;
                    waits.record(Event::Attach,Instant::now().duration_since(before)).await; 

                    return CacheValue::New(arc_value.clone());           

                } else {
                    drop(cache_guard);
                    println!("key add_reverse_edge: - in cache: true about add_reverse_edge");    
                    //println!("key add_reverse_edge: - in cache: send to LRU move_to_head", task);
                    before =Instant::now();    
                    if let Err(err) = lru_ch.send((key.clone(), before, lru_client_ch, LruAction::Move_to_head)).await {
                        panic!("Send on lru_move_to_head_ch failed {}",err)
                    };
                    waits.record(Event::LRUSendMove,Instant::now().duration_since(before)).await; 
                    let _ = srv_resp_rx.recv().await;
                    waits.record(Event::MoveToHead,Instant::now().duration_since(before)).await;

                    return CacheValue::Existing(arc_value.clone());
                }
            }
        }
    }

    async fn wait_for_persist_to_complete(
        &self
        ,key: K  
        ,persist_query_ch : tokio::sync::mpsc::Sender<QueryMsg<K>>
        ,waits : Waits
    )  {
        let (persist_client_send_ch, mut persist_srv_resp_rx) = tokio::sync::mpsc::channel::<bool>(1);
        // wait for evict service to give go ahead...(completed persisting)
        // or ack that it completed already.
        println!("wait_for_persist_to_complete entered...");
        let mut before:Instant =Instant::now();
        if let Err(e) = persist_query_ch
                                .send(QueryMsg::new(key.clone(), persist_client_send_ch.clone()))
                                .await
                                {
                                    panic!("evict channel comm failed = {}", e);
                                }
        waits.record(Event::ChanPersistQuery,Instant::now().duration_since(before)).await;

        // wait for persist to complete
        before =Instant::now();
        println!("wait_for_persist_to_complete entered...wait for persist resp...");
        let persist_resp = match persist_srv_resp_rx.recv().await {
                    Some(resp) => resp,
                    None => {
                        panic!("communication with evict service failed")
                        
                    }
                    };
        waits.record(Event::ChanPersistQueryResp,Instant::now().duration_since(before)).await;
            
        //println!("RKEY add_reverse_edge: node eviced FINISHED waiting - recv'd ACK from PERSIT {:?}", task, self);
        if persist_resp {
                    // ====================================
                    // wait for completed msg from Persist
                    // ====================================
                    println!("wait_for_persist_to_complete entered...wait for io to complete...");
                    before =Instant::now();
                    persist_srv_resp_rx.recv().await;
                    waits.record(Event::ChanPersistWait,Instant::now().duration_since(before)).await;
                }
                println!("wait_for_persist_to_complete entered...EXIT");
    }
}




