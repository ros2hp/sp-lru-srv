use super::*;

use std::hash::Hash;
use std::cmp::Eq;
use std::fmt::Debug;
use std::collections::HashSet;

use tokio::time::{Sleep,Duration};

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
        ,task : usize
        ,dyn_client: &DynamoClient
        ,table_name_: String
        ,waits : Waits
        ,persist_completed_send_ch : tokio::sync::mpsc::Sender<(K, usize)>
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
    lru_ch : tokio::sync::mpsc::Sender<(usize, K, Instant, tokio::sync::mpsc::Sender<bool>, LruAction)>,
    // state of K in cache
    //evicted : HashSet<K>,
    inuse : HashMap<K,u8>,
    persisting: HashSet<K>,
    // performance stats rep
    waits : Waits,
}

#[derive(Debug)]
pub struct Cache<K,V>(pub Arc<Mutex<InnerCache<K,V>>>);


impl<K : Hash + Eq + Debug + Clone, V: Persistence<K> + Clone + Debug>  Cache<K,V>
{

    pub fn new(
        persist_query_ch : tokio::sync::mpsc::Sender<QueryMsg<K>>
        ,lru_ch : tokio::sync::mpsc::Sender<( usize, K, Instant, tokio::sync::mpsc::Sender<bool>, LruAction)>
        ,waits : Waits
    ) -> Self
   {
        Cache::<K,V>(Arc::new(tokio::sync::Mutex::new(InnerCache::<K,V>{
            datax: HashMap::new()
            ,persist_query_ch
            ,lru_ch
            //
            //,evicted : HashSet::new()
            ,inuse : HashMap::new()
            ,persisting: HashSet::new()
            //
            ,waits
            })))
    }
}

impl<K : Hash + Eq + Debug + Clone,V>  InnerCache<K,V>
{
    pub fn unlock(&mut self, key: &K) {
        println!("InnerCache unlock [{:?}]",key);
        self.unset_inuse(key);
    }

    pub fn set_inuse(&mut self, key: K) {
        println!("InnerCache set_inuse [{:?}]",key);
        self.inuse.entry(key.clone()).and_modify(|i|*i+=1).or_insert(1);
    }

    pub fn unset_inuse(&mut self, key: &K) {
        println!("InnerCache unset_inuse [{:?}]",key);
        self.inuse.entry(key.clone()).and_modify(|i|*i-=1);
    }

    pub fn inuse(&self, key: &K) -> bool {
        println!("InnerCache inuse [{:?}]",key);
        match self.inuse.get(key) {
            None => false,
            Some(i) => {*i > 0},
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

}

impl<K,V> Clone for Cache<K,V> {

    fn clone(&self) -> Self {
        Cache::<K,V>(self.0.clone())
    }
}

impl<K: Hash + Eq + Clone + Debug, V: Persistence<K> + Clone + NewValue<K,V> + Debug>  Cache<K,V>
{

    pub async fn unlock(&mut self, key: &K) {
        println!("CACHE: cache.unlock {:?}",key);
        self.0.lock().await.unset_inuse(key);
        println!("CACHE: cache.unlock DONE");
    }

    pub async fn get(
        self
        ,key : &K
        ,task : usize,
    ) -> CacheValue<Arc<tokio::sync::Mutex<V>>> {
        let (lru_client_ch, mut srv_resp_rx) = tokio::sync::mpsc::channel::<bool>(1); 

        let before:Instant;  
        let mut cache_guard = self.0.lock().await;
        match cache_guard.datax.get(&key) {
            
            None => {
                println!("{} CACHE: - Not Cached: add to cache {:?}", task, key);
                let lru_ch = cache_guard.lru_ch.clone();
                let waits = cache_guard.waits.clone();
                let persist_query_ch = cache_guard.persist_query_ch.clone();
                                // acquire lock on value and release cache lock - this prevents concurrent updates to value 
                // and optimises cache concurrency by releasing lock asap
                let arc_value = V::new_with_key(key);
                // =========================
                // add to cache, set in-use 
                // =========================
                cache_guard.datax.insert(key.clone(), arc_value.clone()); // self.clone(), arc_value.clone());
                cache_guard.set_inuse(key.clone());
                let persisting = cache_guard.persisting(&key);
                // ===============================================================
                // serialise access to value - prevents concurrent operations on key
                // ===============================================================                
                let _ = arc_value.lock().await;
                // ============================================================================================================
                // release cache lock with value still locked - value now in cache, so next get on key will go to in-cache path
                // ============================================================================================================
                drop(cache_guard);
                // =======================
                // IS NODE BEING PERSISTED 
                // =======================
                if persisting {
                    println!("{} CACHE: - Not Cached: waiting on persisting due to eviction {:?}",task, key);
                    self.wait_for_persist_to_complete(task, key.clone(),persist_query_ch, waits.clone()).await;
                }
                before =Instant::now();
                if let Err(err) = lru_ch.send((task, key.clone(), before, lru_client_ch, LruAction::Attach)).await {
                    panic!("Send on lru_attach_ch errored: {}", err);
                }   
                waits.record(Event::LRUSendAttach,Instant::now().duration_since(before)).await;    
                // sync'd: wait for LRU operation to complete - just like using a mutex is synchronous with operation.
                let _ = srv_resp_rx.recv().await;
                waits.record(Event::Attach,Instant::now().duration_since(before)).await; 

                return CacheValue::New(arc_value.clone());
            }
            
            Some(arc_value) => {

                //println!("key add_reverse_edge: - Cached key {:?}", task, self);
                // acquire lock on value and release cache lock - this prevents concurrent updates to value 
                // and optimises cache concurrency by releasing lock asap
                let arc_value=arc_value.clone();

                let persist_query_ch = cache_guard.persist_query_ch.clone();
                let lru_ch=cache_guard.lru_ch.clone();
                let waits = cache_guard.waits.clone();
                let persisting = cache_guard.persisting(&key);
                cache_guard.set_inuse(key.clone()); // prevents concurrent persist
                // =========================
                // release cache lock
                // =========================
                drop(cache_guard);
                // =============================================
                // serialise processing on concurrent key-value
                // =============================================
                let _ = arc_value.lock().await;
                // ======================
                // IS NODE persisting 
                // ======================
                if persisting {
                    println!("{} CACHE key: in CACHE check if still persisting ....{:?}", task,key);
                    self.wait_for_persist_to_complete(task, key.clone(),persist_query_ch, waits.clone()).await;       
                } 

                println!("{} CACHE key: in cache:  {:?}", task, key);
                before =Instant::now();    
                if let Err(err) = lru_ch.send((task, key.clone(), before, lru_client_ch, LruAction::Move_to_head)).await {
                    panic!("Send on lru_move_to_head_ch failed {}",err)
                };
                waits.record(Event::LRUSendMove,Instant::now().duration_since(before)).await; 
                let _ = srv_resp_rx.recv().await;
                waits.record(Event::MoveToHead,Instant::now().duration_since(before)).await;

                return CacheValue::Existing(arc_value.clone());
            }
        }
    }

    async fn wait_for_persist_to_complete(
        &self
        ,task: usize
        ,key: K  
        ,persist_query_ch : tokio::sync::mpsc::Sender<QueryMsg<K>>
        ,waits : Waits
    )  {
        let (persist_client_send_ch, mut persist_srv_resp_rx) = tokio::sync::mpsc::channel::<bool>(1);
        // wait for evict service to give go ahead...(completed persisting)
        // or ack that it completed already.
        println!("{} CACHE: wait_for_persist_to_complete entered...{:?}",task, key);
        let mut before:Instant =Instant::now();
        if let Err(e) = persist_query_ch
                                .send(QueryMsg::new(key.clone(), persist_client_send_ch.clone(),task))
                                .await
                                {
                                    panic!("evict channel comm failed = {}", e);
                                }
        waits.record(Event::ChanPersistQuery,Instant::now().duration_since(before)).await;

        // wait for persist to complete
        before =Instant::now();
        println!("{} CACHE: wait_for_persist_to_complete entered...wait for persist resp...{:?}",task,key);
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
                    println!("{} CACHE: wait_for_persist_to_complete entered...wait for io to complete...{:?}",task, key);
                    before =Instant::now();
                    persist_srv_resp_rx.recv().await;
                    waits.record(Event::ChanPersistWait,Instant::now().duration_since(before)).await;
                }
        println!("{} CACHE: wait_for_persist_to_complete entered...EXIT {:?}",task, key);
    }
}




