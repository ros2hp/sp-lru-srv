use crate::cache::Cache;
use crate::cache::Persistence;

use crate::service::stats::{Waits,Event};
use crate::QueryMsg;

use std::collections::{HashMap, HashSet, VecDeque};

use std::sync::Arc;

//
use aws_sdk_dynamodb::Client as DynamoClient;


use tokio::task;
use tokio::time::{self, Duration, Instant};
use tokio::sync::Mutex;

const MAX_PRESIST_TASKS: u8 = 8;

struct Lookup<K,V>(HashMap<K, Arc<Mutex<V>>>);


impl<K,V> Lookup<K,V> {
    fn new() -> Lookup<K,V> {
        Lookup::<K,V>(HashMap::new())
    }
}

// Pending persistion queue
struct PendingQ<K>(VecDeque<K>);


impl<K: std::cmp::PartialEq> PendingQ<K> {
    fn new() -> Self {
        PendingQ::<K>(VecDeque::new())
    }

    fn remove(&mut self, K: &K) {
        let mut ri = 0;
        let mut found = false;

        for (i,v) in self.0.iter().enumerate() {
            if *v == *K {
                ri = i;
                found = true;
                break;
            }
        }
        if found {
            self.0.remove(ri);
        }
    }
}

//  container for clients querying persist service
struct QueryClient<K>(HashMap<K, tokio::sync::mpsc::Sender<bool>>);

impl<K> QueryClient<K> {
    fn new() -> Self {
        QueryClient::<K>(HashMap::new())
    }
}

// struct Persisted(HashSet<K>);


// impl Persisted {

//     fn new() -> Arc<Mutex<Persisted>> {
//        Arc::new(Mutex::new(Persisted(HashSet::new())))
//     }
// }

pub fn start_service<K,V>(
    mut cache: Cache<K,V>,
    dynamo_client: DynamoClient,
    table_name_: impl Into<String>,
    // channels
    mut submit_rx: tokio::sync::mpsc::Receiver<(K, Arc<Mutex<V>>, tokio::sync::mpsc::Sender<bool>)>,
    mut client_query_rx: tokio::sync::mpsc::Receiver<QueryMsg<K>>,
    mut shutdown_ch: tokio::sync::broadcast::Receiver<u8>,
    //
    waits_ : Waits,
) -> task::JoinHandle<()> 
where K: Clone + std::fmt::Debug + std::cmp::Eq + std::hash::Hash + std::marker::Send + std::marker::Sync + 'static, 
      V: Clone + Persistence<K> + std::marker::Send + std::marker::Sync + 'static 
{

    let table_name = table_name_.into();

    //let mut start = Instant::now();

    println!("PERSIST  starting persist service: table [{}] ", table_name);

    //let mut persisted = Persisted::new(); // temmporary - initialise to zero ovb metadata when first persisted
    let mut persisting_lookup = Lookup::new();
    let mut pending_q = PendingQ::new();
    let mut query_client = QueryClient::new();
    let mut tasks = 0;

    // persist channel used to acknowledge to a waiting client that the associated node has completed persistion.
    let (persist_completed_send_ch, mut persist_completed_rx) =
        tokio::sync::mpsc::channel::<K>(MAX_PRESIST_TASKS as usize);

    //let backoff_queue : VecDeque = VecDeque::new();
    let dyn_client = dynamo_client.clone();
    let tbl_name = table_name.clone();
    let waits = waits_.clone();

    // persist service only handles
    let persist_server = tokio::spawn(async move {
        loop {
            //let persist_complete_send_ch_=persist_completed_send_ch.clone();
            tokio::select! {
                //biased;         // removes random number generation - normal processing will determine order so select! can follow it.
                // note: recv() is cancellable, meaning select! can cancel a recv() without loosing data in the channel.
                // select! will be forced to cancel recv() if another branch event happens e.g. recv() on shutdown_channel.
                Some((key, arc_node, client_ch )) = submit_rx.recv() => {

                    //  no locks acquired  - apart from Cache in async routine, which is therefore safe.

                        // persisting_lookup arc_node for given K
                        println!("PERSIST: submit persist for {:?} tasks [{}]",key, tasks);
                        persisting_lookup.0.insert(key.clone(), arc_node.clone());
                        cache.0.lock().await.set_persisting(key.clone());
                        println!("PERSIST : cache set_persisting done :  key {:?}  tasks {}", key, tasks);
    
                        if tasks >= MAX_PRESIST_TASKS {
                            // maintain a FIFO of evicted nodes
                            println!("PERSIST: submit - max tasks reached add {:?} pending_q {}",key, pending_q.0.len());
                            pending_q.0.push_front(key.clone());                         
    
                        } else {
                            // ==============================================
                            // lock arc node - to access type persist method
                            // ==============================================
                            let node_guard_=arc_node.lock().await;

                            let mut node_guard=node_guard_.clone();
                            // spawn async task to persist node
                            let dyn_client_ = dyn_client.clone();
                            let tbl_name_ = tbl_name.clone();
                            let persist_complete_send_ch_=persist_completed_send_ch.clone();
                            let waits=waits.clone();
                            //let persisted_=persisted.clone();
                            tasks+=1;
    
                            println!("PERSIST: submit ASYNC 3 call to persist_V tasks {}",tasks);
                            tokio::spawn(async move {
    
                                // save Node data to db
                                node_guard.persist(
                                    &dyn_client_
                                    ,tbl_name_
                                    ,waits
                                    ,persist_complete_send_ch_
                                ).await;
    
                            });
                        }
                        println!("PERSIST: submit - send response");
                        if let Err(err) = client_ch.send(true).await {
                            panic!("Error in sending query_msg [{}]",err)
                        };
                        println!("PERSIST: submit - Exit");

                },

                Some(persist_K) = persist_completed_rx.recv() => {

                    tasks-=1;
                    
                    println!("PERSIST : completed msg:  key {:?}  tasks {}", persist_K, tasks);
                    persisting_lookup.0.remove(&persist_K);
                    cache.0.lock().await.unset_persisting(&persist_K);
                    println!("PERSIST : cache unset_persisting done :  key {:?}  tasks {}", persist_K, tasks);
                    // send ack to client if one is waiting on query channel
                    //println!("PERSIST : send complete persist ACK to client - if registered. {:?}",persist_K);
                    if let Some(client_ch) = query_client.0.get(&persist_K) {
                        //println!("PERSIST :   Yes.. ABOUT to send ACK to query that persist completed ");
                        // send ack of completed persistion to waiting client
                        if let Err(err) = client_ch.send(true).await {
                            panic!("Error in sending to waiting client that K is evicited [{}]",err)
                        }
                        //
                        query_client.0.remove(&persist_K);
                        //println!("PERSIST  EVIct: ABOUT to send ACK to query that persist completed - DONE");
                    }
                    //println!("PERSIST  EVIct: is client waiting..- DONE ");
                    // // process next node in persist Pending Queue
                    if let Some(queued_Key) = pending_q.0.pop_back() {
                        println!("PERSIST : persist next entry in pending_q.... {:?}", queued_Key);
                        // spawn async task to persist node
                        let dyn_client_ = dyn_client.clone();
                        let tbl_name_ = tbl_name.clone();
                        let persist_complete_send_ch_=persist_completed_send_ch.clone();
                        println!("PERSIST: start persist task from pending_q. {:?} tasks {} queue size {}",queued_Key, tasks, pending_q.0.len() );

                        let Some(arc_node_) = persisting_lookup.0.get(&queued_Key) else {panic!("Persist service: expected arc_node in Lookup {:?}",queued_Key)};
                        let arc_node=arc_node_.clone();
                        let waits=waits.clone();
                        let mut node_guard = arc_node_.lock().await.clone();
                        //let persisted_=persisted.clone();
                        tasks+=1;
       
                        tokio::spawn(async move {
                            // save Node data to db
                            node_guard.persist(
                                &dyn_client_
                                ,tbl_name_
                                ,waits
                                ,persist_complete_send_ch_
                            ).await;
                        });
                    }
                    println!("PERSIST finished completed msg:  key {:?}  tasks {} ", persist_K, tasks);
                },

                Some(query_msg) = client_query_rx.recv() => {

                    // ACK to client whether node is marked evicted
                    // println!("PERSIST : client query for {:?}",query_msg.0);
                    if let Some(_) = persisting_lookup.0.get(&query_msg.0) {
                        // register for notification of persist completion.
                        query_client.0.insert(query_msg.0.clone(), query_msg.1.clone());
                        // send ACK (true) to client 
                        //println!("PERSIST : send ACK (true) to client {:?}",query_msg.0);
                        if let Err(err) = query_msg.1.send(true).await {
                            panic!("Error in sending query_msg [{}]",err)
                        };
                        
                    } else {
                        
                        //println!("PERSIST : send ACK (false) to client {:?}",query_msg.0);
                        // send ACK (false) to client 
                        if let Err(err) = query_msg.1.send(false).await {
                            panic!("Error in sending query_msg [{}]",err)
                        };                
                    }

                    println!("PERSIST :  client_query exit {:?}",query_msg.0);
                },

                //Some(client_ch) = pre_shutdown_rx.recv() => {
                _ = shutdown_ch.recv() => {
                        println!("PERSIST shutdown:  Waiting for remaining persist tasks [{}] pending_q {} to complete...",tasks as usize, pending_q.0.len());
                        while tasks > 0 || pending_q.0.len() > 0 {
                            println!("PERSIST  ...waiting on {} tasks",tasks);
                            if tasks > 0 {
                                let Some(persist_K) = persist_completed_rx.recv().await else {panic!("Inconsistency; expected task complete msg got None...")};
                                tasks-=1;
                                // send to client if one is waiting on query channel. Does not block as buffer size is 1.
                                if let Some(client_ch) = query_client.0.get(&persist_K) {
                                    // send ack of completed persistion to waiting client
                                    if let Err(err) = client_ch.send(true).await {
                                        panic!("Error in sending to waiting client that K is evicited [{}]",err)
                                    }
                                    //
                                    query_client.0.remove(&persist_K);
                                }
                            }
                            if let Some(queued_Key) = pending_q.0.pop_back() {
                                //println!("PERSIST : persist next entry in pending_q....");
                                // spawn async task to persist node
                                let dyn_client_ = dyn_client.clone();
                                let tbl_name_ = tbl_name.clone();
                                let persist_complete_send_ch_=persist_completed_send_ch.clone();
                                let Some(arc_node_) = persisting_lookup.0.get(&queued_Key) else {panic!("Persist service: expected arc_node in Lookup")};
                                let arc_node=arc_node_.clone();
                                let waits=waits.clone();
                                let mut node_guard= arc_node_.lock().await.clone();
                                //let persisted_=persisted.clone();
                                tasks+=1;


                                println!("PERSIST: shutdown  persist task tasks {} Pending-Q {}", tasks, pending_q.0.len() );
                                // save Node data to db
                                tokio::spawn(async move {
                                    // save Node data to db
                                    node_guard.persist(
                                        &dyn_client_
                                        ,tbl_name_
                                        ,waits
                                        ,persist_complete_send_ch_
                                    ).await;
                            });
                            }
                        }
                        println!("PERSIST  shutdown completed. Tasks {}",tasks);
                        // if let Err(err) = client_ch.send(()).await {
                        //     panic!("Error in sending client_ch [{}]",err)
                        // };  
                        break;
                },
            }
        } // end-loop
    });
    persist_server
}

