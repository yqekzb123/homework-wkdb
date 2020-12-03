
#include "global.h"
#include "txn_table.h"
#include "qry_tpcc.h"
#include "tpcc.h"
#include "qry_ycsb.h"
#include "ycsb.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "row.h"
#include "pool.h"
#include "task_queue.h"
#include "message.h"

void TxnTable::init() {
  //pool_size = g_max_inflight * g_cnt_node * 2 + 1;
  pool_size = g_max_inflight + 1;
  DEBUG_M("TxnTable::init pool_node alloc\n");
  pool = (pool_node **) alloc_memory.align_alloc(sizeof(pool_node*) * pool_size);
  for(uint32_t i = 0; i < pool_size;i++) {
    pool[i] = (pool_node *) alloc_memory.align_alloc(sizeof(struct pool_node));
    pool[i]->head = NULL;
    pool[i]->tail = NULL;
    pool[i]->cnt = 0;
    pool[i]->modify = false;
    pool[i]->min_ts = UINT64_MAX;
  }
}

void TxnTable::dump() {
    for(uint64_t i = 0; i < pool_size;i++) {
        if(pool[i]->cnt  == 0) {continue;}
    
        txn_node_t t_node = pool[i]->head;

        while (t_node != NULL) {
            printf("TT (%ld,%ld)\n",t_node->txn_man->read_txn_id(),t_node->txn_man->get_batch_id());
            t_node = t_node->next;
        }
        
    }
}

bool TxnTable::is_matching_txn_node(txn_node_t t_node, uint64_t txn_id, uint64_t batch_id){
  assert(t_node);
#if ALGO == CALVIN
    return (t_node->txn_man->read_txn_id() == txn_id && t_node->txn_man->get_batch_id() == batch_id); 
#else
    return (t_node->txn_man->read_txn_id() == txn_id); 
#endif
}

void TxnTable::update_min_ts(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id,uint64_t ts){

  uint64_t pool_id = txn_id % pool_size;
  while(!ATOM_CAS(pool[pool_id]->modify,false,true)) { };
  if(ts < pool[pool_id]->min_ts)
    pool[pool_id]->min_ts = ts;
  ATOM_CAS(pool[pool_id]->modify,true,false);
}

TxnMgr * TxnTable::get_transaction_manager(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id){
  DEBUG("TxnTable::get_txn_manager %ld / %ld\n",txn_id,pool_size);
  uint64_t begintime = acquire_ts();
  uint64_t pool_id = txn_id % pool_size;

  uint64_t mtx_starttime = begintime;
  // set modify bit for this pool: txn_id % pool_size
  while(!ATOM_CAS(pool[pool_id]->modify,false,true)) { };
  INC_STATS(thd_id,mtx[7],acquire_ts()-mtx_starttime);

  txn_node_t t_node = pool[pool_id]->head;
  TxnMgr * txn_man = NULL;

  uint64_t prof_starttime = acquire_ts();
  while (t_node != NULL) {
    if(is_matching_txn_node(t_node,txn_id,batch_id)) {
      txn_man = t_node->txn_man;
      break;
    }
    t_node = t_node->next;
  }
  INC_STATS(thd_id,mtx[20],acquire_ts()-prof_starttime);


  if(!txn_man) {
    prof_starttime = acquire_ts();

    tbl_txn_pool.get(thd_id,t_node);

    INC_STATS(thd_id,mtx[21],acquire_ts()-prof_starttime);
    prof_starttime = acquire_ts();

    txn_man_pool.get(thd_id,txn_man);

    INC_STATS(thd_id,mtx[22],acquire_ts()-prof_starttime);
    prof_starttime = acquire_ts();

    txn_man->set_txn_id(txn_id);
    txn_man->set_batch_id(batch_id);
    t_node->txn_man = txn_man;
    txn_man->txn_stats.begintime = acquire_ts();
    txn_man->txn_stats.restart_begintime = txn_man->txn_stats.begintime;
    LIST_PUT_TAIL(pool[pool_id]->head,pool[pool_id]->tail,t_node);

    INC_STATS(thd_id,mtx[23],acquire_ts()-prof_starttime);
    prof_starttime = acquire_ts();

    ++pool[pool_id]->cnt;
    if(pool[pool_id]->cnt > 1) {
      INC_STATS(thd_id,txn_table_cflt_cnt,1);
      INC_STATS(thd_id,txn_table_cflt_size,pool[pool_id]->cnt-1);
    }
    INC_STATS(thd_id,txn_table_new_cnt,1);
  INC_STATS(thd_id,mtx[24],acquire_ts()-prof_starttime);

  }

  // unset modify bit for this pool: txn_id % pool_size
  ATOM_CAS(pool[pool_id]->modify,true,false);

  INC_STATS(thd_id,txn_table_get_time,acquire_ts() - begintime);
  INC_STATS(thd_id,txn_table_get_cnt,1);
  return txn_man;

}

void TxnTable::restart_txn(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id){
  uint64_t pool_id = txn_id % pool_size;
  // set modify bit for this pool: txn_id % pool_size
  while(!ATOM_CAS(pool[pool_id]->modify,false,true)) { };

  txn_node_t t_node = pool[pool_id]->head;

  while (t_node != NULL) {
    if(is_matching_txn_node(t_node,txn_id,batch_id)) {
#if ALGO == CALVIN
      task_queue.enqueue(thd_id,Msg::create_message(t_node->txn_man,WKRTXN),false);
#else
      if(IS_LOCAL(txn_id))
        task_queue.enqueue(thd_id,Msg::create_message(t_node->txn_man,WKRTXN_CONT),false);
      else
        task_queue.enqueue(thd_id,Msg::create_message(t_node->txn_man,WKRQRY_CONT),false);
#endif
      break;
    }
    t_node = t_node->next;
  }

  // unset modify bit for this pool: txn_id % pool_size
  ATOM_CAS(pool[pool_id]->modify,true,false);

}

void TxnTable::release_transaction_manager(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id){
  uint64_t begintime = acquire_ts();

  uint64_t pool_id = txn_id % pool_size;
  uint64_t mtx_starttime = begintime;
  // set modify bit for this pool: txn_id % pool_size
  while(!ATOM_CAS(pool[pool_id]->modify,false,true)) { };
  INC_STATS(thd_id,mtx[8],acquire_ts()-mtx_starttime);

  txn_node_t t_node = pool[pool_id]->head;


  uint64_t prof_starttime = acquire_ts();
  while (t_node != NULL) {
    if(is_matching_txn_node(t_node,txn_id,batch_id)) {
      LIST_REMOVE_HT(t_node,pool[txn_id % pool_size]->head,pool[txn_id % pool_size]->tail);
      --pool[pool_id]->cnt;
      break;

    }

    t_node = t_node->next;
  }
  INC_STATS(thd_id,mtx[25],acquire_ts()-prof_starttime);
  prof_starttime = acquire_ts();

  // unset modify bit for this pool: txn_id % pool_size
  ATOM_CAS(pool[pool_id]->modify,true,false);

  prof_starttime = acquire_ts();
  assert(t_node);
  assert(t_node->txn_man);

  txn_man_pool.put(thd_id,t_node->txn_man);
    
  INC_STATS(thd_id,mtx[26],acquire_ts()-prof_starttime);
  prof_starttime = acquire_ts();

  tbl_txn_pool.put(thd_id,t_node);
  INC_STATS(thd_id,mtx[27],acquire_ts()-prof_starttime);


  INC_STATS(thd_id,txn_table_release_time,acquire_ts() - begintime);
  INC_STATS(thd_id,txn_table_release_cnt,1);

}

uint64_t TxnTable::get_min_ts(uint64_t thd_id) {

  uint64_t begintime = acquire_ts();
  uint64_t min_ts = UINT64_MAX;
  for(uint64_t i = 0 ; i < pool_size; i++) {
    uint64_t pool_min_ts = pool[i]->min_ts;
    if(pool_min_ts < min_ts)
      min_ts = pool_min_ts;
  }

  INC_STATS(thd_id,txn_table_min_ts_time,acquire_ts() - begintime);
  return min_ts;

}

