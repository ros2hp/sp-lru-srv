use crate::cache::ReverseCache;
use crate::types;

use crate::node::RNode;
use crate::service::stats::{Waits,Event};
use crate::{QueryMsg, RKey};

use std::collections::{HashMap, HashSet, VecDeque};

use std::sync::Arc;

use std::mem;
//
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::operation::update_item::{UpdateItemError, UpdateItemOutput};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;

use aws_smithy_runtime_api::client::result::SdkError;

use tokio::task;
use tokio::time::{self, Duration, Instant};
use tokio::sync::Mutex;

use uuid::Uuid;

const MAX_PRESIST_TASKS: u8 = 2;

struct Lookup(HashMap<RKey, Arc<Mutex<RNode>>>);

impl Lookup {
    fn new() -> Lookup {
        Lookup(HashMap::new())
    }
}

// Pending persistion queue
struct PendingQ(VecDeque<RKey>);

impl PendingQ {
    fn new() -> Self {
        PendingQ(VecDeque::new())
    }
    
    fn remove(&mut self, rkey: &RKey) {
        let mut ri = 0;
        let mut found = false;

        for (i,v) in self.0.iter().enumerate() {
            if *v == *rkey {
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
struct QueryClient(HashMap<RKey, tokio::sync::mpsc::Sender<bool>>);

impl QueryClient {
    fn new() -> Self {
        QueryClient(HashMap::new())
    }
}

// struct Persisted(HashSet<RKey>);


// impl Persisted {

//     fn new() -> Arc<Mutex<Persisted>> {
//        Arc::new(Mutex::new(Persisted(HashSet::new())))
//     }
// }

pub fn start_service(
    dynamo_client: DynamoClient,
    table_name_: impl Into<String>,
    // channels
    mut submit_rx: tokio::sync::mpsc::Receiver<(RKey, Arc<Mutex<RNode>>, tokio::sync::mpsc::Sender<bool>)>,
    //mut _flush_rx: tokio::sync::mpsc::Receiver<(RKey, Arc<Mutex<RNode>>, tokio::sync::mpsc::Sender<bool>)>,
    mut client_query_rx: tokio::sync::mpsc::Receiver<QueryMsg>,
    mut pre_shutdown_rx: tokio::sync::mpsc::Receiver<tokio::sync::mpsc::Sender<()>>,
    stats_close_ch: tokio::sync::mpsc::Sender<bool>,
    mut shutdown_ch: tokio::sync::broadcast::Receiver<u8>,
    waits_ : Waits,
) -> task::JoinHandle<()> {

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
        tokio::sync::mpsc::channel::<RKey>(MAX_PRESIST_TASKS as usize);

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
                Some((rkey, arc_node, client_ch )) = submit_rx.recv() => {

                    //  no locks acquired  - apart from Cache in async routine, which is therefore safe.

                        // persisting_lookup arc_node for given rkey
                        persisting_lookup.0.insert(rkey.clone(), arc_node.clone());

                        println!("PERSIST: submit persist for {:?} tasks [{}]",rkey, tasks);
    
                        if tasks >= MAX_PRESIST_TASKS {
                            // maintain a FIFO of evicted nodes
                            pending_q.0.push_front(rkey.clone());                         
                            println!("PERSIST: submit - max tasks reached add {:?} pending_q {}",rkey, pending_q.0.len())
    
                        } else {
    
                            // spawn async task to persist node
                            let dyn_client_ = dyn_client.clone();
                            let tbl_name_ = tbl_name.clone();
                            let persist_complete_send_ch_=persist_completed_send_ch.clone();
                            let waits=waits.clone();
                            //let persisted_=persisted.clone();
                            tasks+=1;
    
                            //println!("PERSIST : ASYNC call to persist_rnode tasks {}",tasks);
                            tokio::spawn(async move {
    
                                // save Node data to db
                                persist_rnode(
                                    &dyn_client_
                                    ,tbl_name_
                                    ,arc_node
                                    ,persist_complete_send_ch_
                                    ,waits
                                ).await;
    
                            });
                        }
                        println!("PERSIST: submit - send response");
                        if let Err(err) = client_ch.send(true).await {
                            panic!("Error in sending query_msg [{}]",err)
                        };
                        println!("PERSIST: submit - Exit");

                },

                Some(persist_rkey) = persist_completed_rx.recv() => {

                    tasks-=1;
                    
                    println!("PERSIST : completed msg:  key {:?}  tasks {}", persist_rkey, tasks);
                    persisting_lookup.0.remove(&persist_rkey);

                    
                    // send ack to client if one is waiting on query channel
                    //println!("PERSIST : send complete persist ACK to client - if registered. {:?}",persist_rkey);
                    if let Some(client_ch) = query_client.0.get(&persist_rkey) {
                        //println!("PERSIST :   Yes.. ABOUT to send ACK to query that persist completed ");
                        // send ack of completed persistion to waiting client
                        if let Err(err) = client_ch.send(true).await {
                            panic!("Error in sending to waiting client that rkey is evicited [{}]",err)
                        }
                        //
                        query_client.0.remove(&persist_rkey);
                        //println!("PERSIST  EVIct: ABOUT to send ACK to query that persist completed - DONE");
                    }
                    //println!("PERSIST  EVIct: is client waiting..- DONE ");
                    // // process next node in persist Pending Queue
                    if let Some(queued_rkey) = pending_q.0.pop_back() {
                        println!("PERSIST : persist next entry in pending_q.... {:?}", queued_rkey);
                        // spawn async task to persist node
                        let dyn_client_ = dyn_client.clone();
                        let tbl_name_ = tbl_name.clone();
                        let persist_complete_send_ch_=persist_completed_send_ch.clone();
                        println!("PERSIST: start persist task from pending_q. {:?} tasks {} queue size {}",queued_rkey, tasks, pending_q.0.len() );

                        let Some(arc_node_) = persisting_lookup.0.get(&queued_rkey) else {panic!("Persist service: expected arc_node in Lookup {:?}",queued_rkey)};
                        let arc_node=arc_node_.clone();
                        let waits=waits.clone();
                        //let persisted_=persisted.clone();
                        tasks+=1;
       
                        tokio::spawn(async move {
                                // save Node data to db
                                persist_rnode(
                                    &dyn_client_
                                    ,tbl_name_
                                    ,arc_node.clone()
                                    ,persist_complete_send_ch_
                                    ,waits
                                ).await;
                        });
                    }
                    println!("PERSIST finished completed msg:  key {:?}  tasks {} ", persist_rkey, tasks);
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
                                let Some(persist_rkey) = persist_completed_rx.recv().await else {panic!("Inconsistency; expected task complete msg got None...")};
                                tasks-=1;
                                // send to client if one is waiting on query channel. Does not block as buffer size is 1.
                                if let Some(client_ch) = query_client.0.get(&persist_rkey) {
                                    // send ack of completed persistion to waiting client
                                    if let Err(err) = client_ch.send(true).await {
                                        panic!("Error in sending to waiting client that rkey is evicited [{}]",err)
                                    }
                                    //
                                    query_client.0.remove(&persist_rkey);
                                }
                            }
                            if let Some(queued_rkey) = pending_q.0.pop_back() {
                                //println!("PERSIST : persist next entry in pending_q....");
                                // spawn async task to persist node
                                let dyn_client_ = dyn_client.clone();
                                let tbl_name_ = tbl_name.clone();
                                let persist_complete_send_ch_=persist_completed_send_ch.clone();
                                let Some(arc_node_) = persisting_lookup.0.get(&queued_rkey) else {panic!("Persist service: expected arc_node in Lookup")};
                                let arc_node=arc_node_.clone();
                                let waits=waits.clone();
                                //let persisted_=persisted.clone();
                                tasks+=1;


                                println!("PERSIST: shutdown  persist task tasks {} Pending-Q {}", tasks, pending_q.0.len() );
                                // save Node data to db
                                tokio::spawn(async move {
                                    // save Node data to db
                                    persist_rnode(
                                        &dyn_client_
                                        ,tbl_name_
                                        ,arc_node.clone()
                                        ,persist_complete_send_ch_
                                        ,waits
                                    ).await;
                                });
                            }
                        }
                        println!("PERSIST  shutdown completed. Tasks {}",tasks);
                        // exit loop
                        if let Err(err) = stats_close_ch.send(true).await {
                            panic!("Error in sending client_ch [{}]",err)
                        };     
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


async fn persist_rnode(
    dyn_client: &DynamoClient,
    table_name_: impl Into<String>,
    arc_node: Arc<tokio::sync::Mutex<RNode>>,
    persist_completed_send_ch: tokio::sync::mpsc::Sender<RKey>,
    waits : Waits,
) {
    // at this point, cache is source-of-truth updated with db values if edge exists.
    // use db cache values to decide nature of updates to db
    // Note for LIST_APPEND Dynamodb will scan the entire attribute value before appending, so List should be relatively small < 10000.
    let table_name: String = table_name_.into();
    let mut target_uid: Vec<AttributeValue>;
    let mut target_bid: Vec<AttributeValue>;
    let mut target_id: Vec<AttributeValue>;
    let mut update_expression: &str;
    
    let mut node = arc_node.lock().await;
    let rkey = RKey::new(node.node, node.rvs_sk.clone());
    let init_cnt = node.init_cnt as usize;
    let edge_cnt = node.target_uid.len() + init_cnt;
    
    if init_cnt <= crate::EMBEDDED_CHILD_NODES {
    
        //println!("*PERSIST  ..init_cnt < EMBEDDED. {:?}", rkey);
    
        if node.target_uid.len() <= crate::EMBEDDED_CHILD_NODES - init_cnt {
            // consume all of node.target*
            target_uid = mem::take(&mut node.target_uid);
            target_id = mem::take(&mut node.target_id);
            target_bid = mem::take(&mut node.target_bid);
        } else {
            // consume portion of node.target*
            target_uid = node
                .target_uid
                .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
            std::mem::swap(&mut target_uid, &mut node.target_uid);
            target_id = node
                .target_id
                .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
            std::mem::swap(&mut target_id, &mut node.target_id);
            target_bid = node
                .target_bid
                .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
            std::mem::swap(&mut target_bid, &mut node.target_bid);
        }

        if init_cnt == 0 {
            // no data in db
            update_expression = "SET #cnt = :cnt, #target = :tuid,   #bid = :bid , #id = :id";
        } else {
            // append to existing data
           update_expression = "SET #target=list_append(#target, :tuid), #id=list_append(#id,:id), #bid=list_append(#bid,:bid), #cnt = :cnt";
        }
        let before = Instant::now();
        //update edge_item
        let result = dyn_client
            .update_item()
            .table_name(table_name.clone())
            .key(
                types::PK,
                AttributeValue::B(Blob::new(rkey.0.clone().as_bytes())),
            )
            .key(types::SK, AttributeValue::S(rkey.1.clone()))
            .update_expression(update_expression)
            // reverse edge
            .expression_attribute_names("#cnt", types::CNT)
            .expression_attribute_values(":cnt", AttributeValue::N(edge_cnt.to_string()))
            .expression_attribute_names("#target", types::TARGET_UID)
            .expression_attribute_values(":tuid", AttributeValue::L(target_uid))
            .expression_attribute_names("#id", types::TARGET_ID)
            .expression_attribute_values(":id", AttributeValue::L(target_id))
            .expression_attribute_names("#bid", types::TARGET_BID)
            .expression_attribute_values(":bid", AttributeValue::L(target_bid))
            //.return_values(ReturnValue::AllNew)
            .send()
            .await;
            waits.record(Event::Persist_Embedded, Instant::now().duration_since(before)).await;        
       
            handle_result(&rkey, result);

    }
    // consume the target_* fields by moving them into overflow batches and persisting the batch
    // note if node has been loaded from db must drive off ovb meta data which gives state of current 
    // population of overflwo batches

    println!("*PERSIST  node.target_uid.len()  {}    {:?}",node.target_uid.len(),rkey);
    while node.target_uid.len() > 0 {

        ////println!("PERSIST  logic target_uid > 0 value {}  {:?}", node.target_uid.len(), rkey );
    
        let mut target_uid: Vec<AttributeValue> = vec![];
        let mut target_bid: Vec<AttributeValue> = vec![];
        let mut target_id: Vec<AttributeValue> = vec![];
        let mut sk_w_bid : String;
        let event :Event ;

        match node.ocur {
            None => {
                // first OvB
                node.obcnt=crate::OV_MAX_BATCH_SIZE;  // force zero free space - see later.
                node.ocur = Some(0);
                continue;
                }
            Some(mut ocur) => {

                let batch_freespace = crate::OV_MAX_BATCH_SIZE - node.obcnt;
                if batch_freespace > 0 {
                
                    // consume last of node.target*
                    if node.target_uid.len() <= batch_freespace {
                    // consume all of node.target*
                        target_uid = mem::take(&mut node.target_uid);
                        target_bid = mem::take(&mut node.target_bid);
                        target_id = mem::take(&mut node.target_id);
                        node.obcnt += target_uid.len();
                        
                    } else {
                        
                        // consume portion of node.target*
                        target_uid = node
                            .target_uid
                            .split_off(batch_freespace);
                        std::mem::swap(&mut target_uid, &mut node.target_uid);
                        target_bid = node.target_bid.split_off(batch_freespace);
                        std::mem::swap(&mut target_bid, &mut node.target_bid);
                        target_id = node.target_id.split_off(batch_freespace);
                        std::mem::swap(&mut target_id, &mut node.target_id);
                        node.obcnt=crate::OV_MAX_BATCH_SIZE;

                    }                                        
                    update_expression = "SET #target=list_append(#target, :tuid), #bid=list_append(#bid, :bid), #id=list_append(#id, :id)";  
                    event = Event::Persist_ovb_append;
                    sk_w_bid = rkey.1.clone();
                    sk_w_bid.push('%');
                    sk_w_bid.push_str(&node.obid[ocur as usize].to_string());
              
                } else {
                
                    // create a new batch optionally in a new OvB
                    if node.ovb.len() < crate::MAX_OV_BLOCKS {
                        // create a new OvB
                        node.ovb.push(Uuid::new_v4());
                        //node.ovb.push(AttributeValue::B(Blob::new(Uuid::new_v4() as bytes)));
                        node.obid.push(1);
                        node.obcnt = 0;
                        node.ocur = Some(node.ovb.len() as u8 - 1);
                     
                    } else {

                        // change current ovb (ie. block)
                        ocur+=1;
                        if ocur as usize == crate::MAX_OV_BLOCKS {
                                ocur = 0;
                        }
                        node.ocur = Some(ocur);
                        //println!("PERSIST   33 node.ocur, ocur {}  {}", node.ocur.unwrap(), ocur);
                        node.obid[ocur as usize] += 1;
                        node.obcnt = 0;
                    }                     
                    if node.target_uid.len() <= crate::OV_MAX_BATCH_SIZE {

                        // consume remaining node.target*
                        target_uid = mem::take(&mut node.target_uid);
                        target_bid = mem::take(&mut node.target_bid);
                        target_id = mem::take(&mut node.target_id);
                        node.obcnt += target_uid.len();
                    
                    } else {

                        // consume leading portion of node.target*
                        target_uid = node.target_uid.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_uid, &mut node.target_uid);
                        target_bid = node.target_bid.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_bid, &mut node.target_bid);
                        target_id = node.target_id.split_off(crate::OV_MAX_BATCH_SIZE);
                        std::mem::swap(&mut target_id, &mut node.target_id);
                        node.obcnt=crate::OV_MAX_BATCH_SIZE;
                    }
                    // ================
                    // add OvB batches
                    // ================
                    sk_w_bid = rkey.1.clone();
                    sk_w_bid.push('%');
                    sk_w_bid.push_str(&node.obid[ocur as usize].to_string());
    
                    update_expression = "SET #target = :tuid, #bid=:bid, #id = :id";
                    event = Event::Persist_ovb_set;
                }
                // ================
                // add OvB batches
                // ================   
                let before = Instant::now();
                let result = dyn_client
                    .update_item()
                    .table_name(table_name.clone())
                    .key(
                        types::PK,
                        AttributeValue::B(Blob::new(
                            node.ovb[node.ocur.unwrap() as usize].as_bytes(),
                        )),
                    )
                    .key(types::SK, AttributeValue::S(sk_w_bid.clone()))
                    .update_expression(update_expression)
                    // reverse edge
                    .expression_attribute_names("#target", types::TARGET_UID)
                    .expression_attribute_values(":tuid", AttributeValue::L(target_uid))
                    .expression_attribute_names("#bid", types::TARGET_BID)
                    .expression_attribute_values(":bid", AttributeValue::L(target_bid))
                    .expression_attribute_names("#id", types::TARGET_ID)
                    .expression_attribute_values(":id", AttributeValue::L(target_id))
                    //.return_values(ReturnValue::AllNew)
                    .send()
                    .await;
                waits.record(event, Instant::now().duration_since(before)).await;        
  
                handle_result(&rkey, result);
                //println!("PERSIST : batch written.....{:?}",rkey);
            }
        }
    } // end while
    // update OvB meta on edge predicate only if OvB are used.
    if node.ovb.len() > 0 {
        update_expression = "SET  #cnt = :cnt, #ovb = :ovb, #obid = :obid, #obcnt = :obcnt, #ocur = :ocur";

        let ocur = match node.ocur {
            None => 0,
            Some(v) => v,
        };
        let before = Instant::now();
        let result = dyn_client
            .update_item()
            .table_name(table_name.clone())
            .key(types::PK, AttributeValue::B(Blob::new(rkey.0.clone())))
            .key(types::SK, AttributeValue::S(rkey.1.clone()))
            .update_expression(update_expression)
            // OvB metadata
            .expression_attribute_names("#cnt", types::CNT)
            .expression_attribute_values(":cnt", AttributeValue::N(edge_cnt.to_string()))
            .expression_attribute_names("#ovb", types::OVB)
            .expression_attribute_values(":ovb", types::uuid_to_av_lb(&node.ovb))
            .expression_attribute_names("#obid", types::OVB_BID)
            .expression_attribute_values(":obid", types::u32_to_av_ln(&node.obid))
            .expression_attribute_names("#obcnt", types::OVB_CNT)
            .expression_attribute_values(":obcnt", AttributeValue::N(node.obcnt.to_string()))
            .expression_attribute_names("#ocur", types::OVB_CUR)
            .expression_attribute_values(":ocur", AttributeValue::N(ocur.to_string()))
            //.return_values(ReturnValue::AllNew)
            .send()
            .await;
            waits.record(Event::Persist_meta, Instant::now().duration_since(before)).await;        
     
        handle_result(&rkey, result);
        
    }

    // send task completed msg to persist service
    if let Err(err) = persist_completed_send_ch.send(rkey.clone()).await {
        println!(
            "Sending completed persist msg to waiting client failed: {}",
            err
        );
    }
    println!("*PERSIST  Exit    {:?}",rkey);
}

fn handle_result(rkey: &RKey, result: Result<UpdateItemOutput, SdkError<UpdateItemError, HttpResponse>>) {
    match result {
        Ok(_out) => {
            ////println!("PERSIST  PRESIST Service: Persist successful update...")
        }
        Err(err) => match err {
            SdkError::ConstructionFailure(_cf) => {
                //println!("PERSIST   Persist Service: Persist  update error ConstructionFailure...")
            }
            SdkError::TimeoutError(_te) => {
                //println!("PERSIST   Persist Service: Persist  update error TimeoutError")
            }
            SdkError::DispatchFailure(_df) => {
                //println!("PERSIST   Persist Service: Persist  update error...DispatchFailure")
            }
            SdkError::ResponseError(_re) => {
                //println!("PERSIST   Persist Service: Persist  update error ResponseError")
            }
            SdkError::ServiceError(_se) => {
                panic!(" Persist Service: Persist  update error ServiceError {:?}",rkey);
            }
            _ => {}
        },
    }
}