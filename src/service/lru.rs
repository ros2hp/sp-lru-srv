use super::*;

use crate::cache::Cache;
use crate::service::stats::{Waits,Event};
use crate::node::Evicted;

use std::collections::HashMap;
use std::sync::Arc;


use tokio::time::{sleep, Duration, Instant};
use tokio::sync::Mutex;

pub enum LruAction {
    Attach,
    Move_to_head,
}

// lru is used only to drive lru_entry eviction.
// the lru_entry cache is separate
#[derive(Clone)]
pub struct Entry<K>{
    pub key: K,
    //
    pub next: Option<Arc<Mutex<Entry<K>>>>,
    pub prev: Option<Arc<Mutex<Entry<K>>>>,
}

impl<K> Entry<K> {
    fn new(key : K) -> Entry<K> {
        Entry{key: key
            ,next: None
            ,prev: None
        }
    }
}

// impl Drop for Entry {
//         fn drop(&mut self) {
//         //println!("\nDROP LRU Entry {:?}\n",self.key);
//     }
// }


pub struct LRUevict<K,V> {
    capacity: usize,
    cnt : usize,
    // pointer to Entry value in the LRU linked list for a key
    lookup : HashMap<K,Arc<Mutex<Entry<K>>>>,
    // 
    client_ch : tokio::sync::mpsc::Sender::<bool>,
    client_rx : tokio::sync::mpsc::Receiver::<bool>,
    //
    persist_submit_ch: tokio::sync::mpsc::Sender<(K, Arc<Mutex<V>>, tokio::sync::mpsc::Sender<bool>)>,
    // record stat waits
    waits : Waits,
    //
    head: Option<Arc<Mutex<Entry<K>>>>,
    tail: Option<Arc<Mutex<Entry<K>>>>,
}
    // 371 |       let lru_server = tokio::task::spawn( async move { 
    //     |  __________________________________________^
    // 372 | |         loop {
    // 373 | |             tokio::select! {
    // 374 | |                 //biased;         // removes random number generation - normal processing will determine order so select! can follow it.
    // ...   |
    // 438 | |         }               
    // 439 | |     }); 
    //     | |_____^ future created by async block is not `Send`
    // head: Option<Rc<RefCell<Entry>>>,
    // tail: Option<Rc<RefCell<Entry>>>,


// implement attach & move_to_head as trait methods.
// // Makes more sense however for these methods to be part of the LRUevict itself - just epxeriementing with traits.
// pub trait LRU {
//     fn attach(
//         &mut self, // , cache_guard:  &mut std::sync::MutexGuard<'_, Cache>
//         key: key,
//     );

//     //pub async fn detach(&mut self
//     fn move_to_head(
//         &mut self,
//         key: key,
//     );
// }


impl<K,V> LRUevict<K,V> 
where K: std::cmp::Eq + std::hash::Hash + std::fmt::Debug + Clone + std::marker::Send, V: Evicted
{  
    
    fn new(
        cap: usize,
        persist_submit_ch: tokio::sync::mpsc::Sender<(K, Arc<tokio::sync::Mutex<V>>, tokio::sync::mpsc::Sender<bool>)>,
        client_ch : tokio::sync::mpsc::Sender::<bool>,
        client_rx : tokio::sync::mpsc::Receiver::<bool>,
        waits : Waits
    ) -> Self {
        LRUevict{
            capacity: cap,
            cnt: 0,
            lookup: HashMap::new(),
            // send channels for persist requests
            persist_submit_ch: persist_submit_ch,
            // record stat waits
            waits,
            // sync channels with Persist
            client_ch: client_ch,
            client_rx : client_rx,
            //
            head: None,
            tail: None,
        }
    }
    
    
    async fn print(&self) {

        let mut entry = self.head.clone();
        println!("*** print LRU chain ***");
        let mut i = 0;
        while let Some(entry_) = entry {
            i+=1;
            let key = entry_.lock().await.key.clone();
            println!("LRU entry {} {:?}",i, key);
            entry = entry_.lock().await.next.clone();      
        }
        println!("*** print LRU chain DONE ***");
    }

    // prerequisite - lru_entry has been confirmed NOT to be in lru-cache.
    // note: can only execute methods on LRUevict if lock has been acquired via Arc<Mutex<LRU>>
    async fn attach(
        &mut self, // , cache_guard:  &mut tokio::sync::MutexGuard<'_, Cache>
        task : usize, 
        key : K,
        cache: Arc<Mutex<Cache::<K,V>>>,
    ) {
        // calling routine (key) is hold lock on RNode(key)
        
        ////println!("   ");
        //println!("{} LRU attach {:?}. ***********",task, key);
        //self.print().await;     
        let mut lc = 0;   
        while self.cnt >= self.capacity && lc < 3 {
        
            lc += 1;
            // ================================
            // Evict the tail entry in the LRU 
            // ================================
            //println!("{} LRU: attach reached LRU capacity - evict tail  lru.cnt {}  lc {}  key {:?}", task, self.cnt, lc,  key);
            // unlink tail lru_entry from lru and notify evict service.
            // Clone REntry as about to purge it from cache.
            let lru_evict_entry = self.tail.as_ref().unwrap().clone();
            let mut evict_entry = lru_evict_entry.lock().await;
            //println!("{} LRU attach evict - about to acquire lock on  {:?}",task, evict_entry.key);

            let before = Instant::now();
            let mut cache_guard = cache.lock().await;
            let Some(arc_evict_node_) = cache_guard.0.get(&evict_entry.key)  
                        else { println!("{} LRU: PANIC - attach evict processing: expect entry in cache {:?}",task, evict_entry.key);
                               //panic!("LRU: attach evict processing: expect entry in cache {:?}",evict_entry.key)
                               continue;
                            };

            let arc_evict_node=arc_evict_node_.clone();
            let tlock_result = arc_evict_node.try_lock();
            //drop(cache_guard);
            // ==========================
            // acquire lock on evict node 
            // ==========================
            match tlock_result {

                Ok(mut evict_node_guard)  => {
                    evict_node_guard.set_evicted(true);
                    drop(evict_node_guard);
                    // ============================
                    // remove node from cache
                    // ============================
                    //let mut cache_guard = cache.lock().await;
                    println!("{} LRU attach evict - remove from Cache {:?}", task, evict_entry.key);
                    cache_guard.0.remove(&evict_entry.key);     
                    // ===================================
                    // detach evict entry from tail of LRU
                    // ===================================
                    match evict_entry.prev {
                        None => {panic!("LRU attach - evict_entry - expected prev got None")}
                        Some(ref new_tail) => {
                            let mut tail_guard = new_tail.lock().await;
                            tail_guard.next = None;
                            self.tail = Some(new_tail.clone()); 
                        }
                    }
                    evict_entry.prev=None;
                    evict_entry.next=None;
                    self.cnt-=1;
                    // =====================
                    // notify persist service
                    // =====================
                    println!("{} LRU: attach notify evict service ...passing arc_node for key {:?}",task, evict_entry.key);
                    if let Err(err) = self
                                        .persist_submit_ch
                                        .send((evict_entry.key.clone(), arc_evict_node.clone(), self.client_ch.clone()))
                                        .await
                                    {
                                        println!("{} LRU Error sending on Evict channel: [{}]",task, err);
                                    }
                    //
                    // sync with persist - so evict node exists in persisting state (hashmap'd) while the lock on evict node is active
                    if let None= self.client_rx.recv().await {
                        panic!("LRU read from client_rx is None ");
                    }
                    println!("{} LRU: attach evict - remove from lookup {:?}",task, evict_entry.key);
                    // =====================
                    // remove from lru lookup 
                    // =====================
                    self.lookup.remove(&evict_entry.key);
                    println!("{} LRU attach - release node lock {:?}",task, key);
                } 
                Err(err) =>  {
                    // Abort eviction - as node is being accessed.
                    // TODO check if error is "node locked"
                    println!("{} 3x LRU attach - lock cannot be acquired - abort eviction {:?}",task, evict_entry.key);
                    break;
                }
            }
            self.waits.record(Event::LruEvictCacheLock, Instant::now().duration_since(before)).await;  
        }
        // ======================
        // attach to head of LRU
        // ======================
        let arc_new_entry = Arc::new(Mutex::new(Entry::new(key.clone())));
        match self.head.clone() {
            None => { 
                // empty LRU 
                //println!("LRU <<< attach: empty LRU set head and tail");     
                self.head = Some(arc_new_entry.clone());
                self.tail = Some(arc_new_entry.clone());
                }
            
            Some(e) => {
                let mut new_entry_guard: tokio::sync::MutexGuard<'_, Entry<K>> = arc_new_entry.lock().await;
                let mut old_head_guard = e.lock().await;
                // set old head prev to point to new entry
                old_head_guard.prev = Some(arc_new_entry.clone());
                // set new entry next to point to old head entry & prev to NOne   
                new_entry_guard.next = Some(e.clone());
                new_entry_guard.prev = None;
                // set LRU head to point to new entry
                self.head=Some(arc_new_entry.clone());
                
                if let None = new_entry_guard.next {
                    panic!("LRU INCONSISTENCY attach: expected Some for next but got NONE {:?}",key);
                }
                if let Some(_) = new_entry_guard.prev {
                    panic!("LRU INCONSISTENCY attach: expected None for prev but got NONE {:?}",key);
                }
                if let None = old_head_guard.prev {
                    panic!("LRU INCONSISTENCY attach: expected entry to have prev set to NONE {:?}",key);
                }
            }
        }
        println!("{} LRU: attach - insert into lookup {:?}",task, key);
        self.lookup.insert(key, arc_new_entry);
        
        if let None = self.head {
            panic!("LRU INCONSISTENCY attach: expected LRU to have head but got NONE")
        }
        if let None = self.tail {
            panic!("LRU INCONSISTENCY attach: expected LRU to have tail but got NONE")
        }
        self.cnt+=1;
        println!("{} LRU: attach add cnt {}",task, self.cnt);
        //self.print().await;
    }
    
    
    // prerequisite - lru_entry has been confirmed to be in lru-cache.\/
    // to execute a method a lock has been taken out on the LRU
    async fn move_to_head(
        &mut self,
        task: usize, 
        key: K,
        cache: Arc<Mutex<Cache::<K,V>>>,
    ) {  
        //println!("--------------");
        println!("{} LRU move_to_head {:?} ********",task, key);
        // abort if lru_entry is at head of lru
        match self.head {
            None => {
                panic!("LRU empty - expected entries");
            }
            Some(ref v) => {
                let hd: K = v.lock().await.key.clone();
                if hd == key {
                    // key already at head
                    println!("{} LRU entry already at head - return {:?}",task, key);
                    return
                }    
            }
        }
        // lookup entry in map
        let lru_entry = match self.lookup.get(&key) {
            None => {  
                        // node has been evicted since keys.add_reverse_edge9) detected it was cached  - due to msg delay in LRU channel buffer
                        // attach instead.
                        self.attach(task, key, cache).await;
                        return;    
                    }
            Some(v) => v.clone()
        };
        {
            // DETACH the entry before attaching to LRU head
            
            let lru_entry_guard = lru_entry.lock().await;
            // NEW CODE to fix eviction and new request at same time on a Node
            if let None = lru_entry_guard.prev {
                ////println!("LRU INCONSISTENCY move_to_head: expected entry to have prev but got NONE {:?}",key);
                if let None = lru_entry_guard.next {
                    // should not happend
                    panic!("LRU move_to_head : got a entry with no prev or next set (ie. a new node) - some synchronisation gone astray")
                }
            }

            // check if moving tail entry
            if let None = lru_entry_guard.next {
            
                println!("{} LRU move_to_head detach tail entry {:?}",task, key);
                let mut prev_guard = lru_entry_guard.prev.as_ref().unwrap().lock().await;
                prev_guard.next = None;
                self.tail = Some(lru_entry_guard.prev.as_ref().unwrap().clone());
                
            } else {
                
                let mut prev_guard = lru_entry_guard.prev.as_ref().unwrap().lock().await;
                let mut next_guard = lru_entry_guard.next.as_ref().unwrap().lock().await;

                prev_guard.next = Some(lru_entry_guard.next.as_ref().unwrap().clone());
                next_guard.prev = Some(lru_entry_guard.prev.as_ref().unwrap().clone());

            }
            println!("{} LRU move_to_head Detach complete...{:?}",task, key);
        }
        // ATTACH
        let mut lru_entry_guard = lru_entry.lock().await;
        lru_entry_guard.next = self.head.clone();
        
        // adjust old head entry previous pointer to new entry (aka LRU head)
        match self.head {
            None => {
                 panic!("LRU empty - expected entries");
            }
            Some(ref v) => {
                let mut hd = v.lock().await;
                hd.prev = Some(lru_entry.clone()); 
                println!("{} LRU move_to_head: attach old head {:?} prev to {:?} ",task, hd.key, lru_entry_guard.key);
            }
        }
        lru_entry_guard.prev = None;
        self.head = Some(lru_entry.clone());
        println!("{} LRU move_to_head complete...{:?}",task, key);
    }
}


pub fn start_service<K:std::cmp::Eq + std::hash::Hash + std::fmt::Debug + Clone + std::marker::Send + std::marker::Sync + 'static, V: std::marker::Send + std::marker::Sync + Evicted + 'static >
(
         lru_capacity : usize
        ,mut cache: Arc<tokio::sync::Mutex<Cache::<K,V>>>
        //
        ,mut lru_rx : tokio::sync::mpsc::Receiver<(usize, K, Instant,tokio::sync::mpsc::Sender<bool>, LruAction)>
        ,mut lru_flush_rx : tokio::sync::mpsc::Receiver<tokio::sync::mpsc::Sender<()>>
        //
        ,persist_submit_ch : tokio::sync::mpsc::Sender<(K, Arc<Mutex<V>>, tokio::sync::mpsc::Sender<bool>)>
        //
        ,waits : Waits
) -> tokio::task::JoinHandle<()> 
 {
    // also consider tokio::task::spawn_blocking() which will create a OS thread and allocate task to it.
    // 
    let (lru_client_ch, lru_client_rx) = tokio::sync::mpsc::channel::<bool>(1);
   
    let mut lru_evict = lru::LRUevict::new(lru_capacity, persist_submit_ch.clone(), lru_client_ch, lru_client_rx, waits);

    let lru_server = tokio::task::spawn( async move { 
        println!("LRU service started....");
        loop {
            tokio::select! {
                biased;         // removes random number generation - normal processing will determine order so select! can follow it.
                // note: recv() is cancellable, meaning select! can cancel a recv() without loosing data in the channel.
                Some((task, key, sent_time, client_ch, action)) = lru_rx.recv() => {

                        match action {
                            LruAction::Attach => {
                                lru_evict.attach(task, key, cache.clone()).await;
                            }
                            LruAction::Move_to_head => {
                                lru_evict.move_to_head(task, key, cache.clone()).await;
                            }
                        }
                        // send response back to client...sync'd.
                        if let Err(err) = client_ch.send(true).await {
                            panic!("LRU action send to client_ch {} ",err);
                        };
                    }

                Some(client_ch) = lru_flush_rx.recv() => {
                               
                                println!("LRU service - flush lru ");
                                    let mut entry = lru_evict.head;
                                    let mut lc = 0;
                                    let cache_guard = cache.lock().await;
                                    println!("LRU flush in progress..persist entries in LRU flust {} lru entries",lru_evict.cnt);
                                    while let Some(entry_) = entry {
                                            lc += 1;     
                                            let key = entry_.lock().await.key.clone();
                                            if let Some(arc_node) = cache_guard.0.get(&key) {
                                                if let Err(err) = lru_evict.persist_submit_ch // persist_flush_ch
                                                            .send((key, arc_node.clone(), lru_evict.client_ch.clone()))
                                                            .await {
                                                                println!("Error on persist_submit_ch channel [{}]",err);
                                                            }
                                            } 
                                            // sync with Persist service
                                            if let None = lru_evict.client_rx.recv().await {
                                                panic!("LRU read from client_rx is None ");
                                            }
                                            entry = entry_.lock().await.next.clone();         
                                    }
                                    if let Err(err) = client_ch.send(()).await {
                                        panic!("LRU send on client_ch {} ",err);
                                    };
                                    println!("LRU shutdown ");
                                    return (); 
                    }                      
            }    
        } 
        ()       
    }); 

    lru_server
}
