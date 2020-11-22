
#include "universal.h"
#include "txn.h"
#include "row.h"
#include "workload.h"
#include "query.h"
#include "thread.h"
#include "mem_alloc.h"
// #include "occ.h"
// #include "row_occ.h"
#include "table.h"
#include "catalog.h"
#include "IndexBTree.h"
#include "IndexHash.h"
#include "msg_queue.h"
#include "pool.h"
#include "message.h"
#include "qry_ycsb.h"
#include "qry_tpcc.h"
#include "array.h"
#include "occ_template.h"


void TxnStats::init() {
  begintime=0;
  wait_begintime=acquire_ts();
  total_process_time=0;
  process_time=0;
  total_local_wait_time=0;
  local_wait_time=0;
  total_remote_wait_time=0;
  remote_wait_time=0;
  total_twopc_time=0;
  two_pc_time=0;
  write_cnt = 0;
  abort_cnt = 0;

   total_task_queue_time = 0;
   task_queue_time = 0;
   total_cc_block_time = 0;
   cc_block_time = 0;
   total_cc_time = 0;
   cc_time = 0;
   total_task_queue_cnt = 0;
   task_queue_cnt = 0;
   total_msg_queue_time = 0;
   msg_queue_time = 0;
   total_abort_time = 0;

   clear_short();
}

void TxnStats::clear_short() {

   task_queue_time_short = 0;
   cc_block_time_short = 0;
   cc_time_short = 0;
   msg_queue_time_short = 0;
   process_time_short = 0;
   network_time_short = 0;
}

void TxnStats::reset() {
  wait_begintime=acquire_ts();
  total_process_time += process_time;
  process_time = 0;
  total_local_wait_time += local_wait_time;
  local_wait_time = 0;
  total_remote_wait_time += remote_wait_time;
  remote_wait_time = 0;
  total_twopc_time += two_pc_time;
  two_pc_time = 0;
  write_cnt = 0;

  total_task_queue_time += task_queue_time;
  task_queue_time = 0;
  total_cc_block_time += cc_block_time;
  cc_block_time = 0;
  total_cc_time += cc_time;
  cc_time = 0;
  total_task_queue_cnt += task_queue_cnt;
  task_queue_cnt = 0;
  total_msg_queue_time += msg_queue_time;
  msg_queue_time = 0;

  clear_short();

}

void TxnStats::stats_abort(uint64_t thd_id) {
    total_process_time += process_time;
    total_local_wait_time += local_wait_time;
    total_remote_wait_time += remote_wait_time;
    total_twopc_time += two_pc_time;
    total_task_queue_time += task_queue_time;
    total_msg_queue_time += msg_queue_time;
    total_cc_block_time += cc_block_time;
    total_cc_time += cc_time;
    total_task_queue_cnt += task_queue_cnt;
    assert(total_process_time >= process_time);

    INC_STATS(thd_id,lat_s_rem_task_queue_time,total_task_queue_time);
    INC_STATS(thd_id,lat_s_rem_msg_queue_time,total_msg_queue_time);
    INC_STATS(thd_id,lat_s_rem_cc_block_time,total_cc_block_time);
    INC_STATS(thd_id,lat_s_rem_cc_time,total_cc_time);
    INC_STATS(thd_id,lat_s_rem_process_time,total_process_time);
}

void TxnStats::stats_commit(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id, uint64_t timespan_long, uint64_t timespan_short) {
  total_process_time += process_time;
  total_local_wait_time += local_wait_time;
  total_remote_wait_time += remote_wait_time;
  total_twopc_time += two_pc_time;
  total_task_queue_time += task_queue_time;
  total_msg_queue_time += msg_queue_time;
  total_cc_block_time += cc_block_time;
  total_cc_time += cc_time;
  total_task_queue_cnt += task_queue_cnt;
  assert(total_process_time >= process_time);

#if ALGO == CALVIN

    INC_STATS(thd_id,lat_s_loc_task_queue_time,task_queue_time);
    INC_STATS(thd_id,lat_s_loc_msg_queue_time,msg_queue_time);
    INC_STATS(thd_id,lat_s_loc_cc_block_time,cc_block_time);
    INC_STATS(thd_id,lat_s_loc_cc_time,cc_time);
    INC_STATS(thd_id,lat_s_loc_process_time,process_time);
  // latency from start of transaction at this node
  PRINT_LATENCY("lat_l %ld %ld %ld %f %f %f %f %f %f\n"
          , txn_id
          , batch_id
          , total_task_queue_cnt
          , (double) timespan_long / BILLION
          , (double) total_task_queue_time / BILLION
          , (double) total_msg_queue_time / BILLION
          , (double) total_cc_block_time / BILLION
          , (double) total_cc_time / BILLION
          , (double) total_process_time / BILLION
          );
#else
  // latency from start of transaction
  if (IS_LOCAL(txn_id)) {
    INC_STATS(thd_id,lat_l_loc_task_queue_time,total_task_queue_time);
    INC_STATS(thd_id,lat_l_loc_msg_queue_time,total_msg_queue_time);
    INC_STATS(thd_id,lat_l_loc_cc_block_time,total_cc_block_time);
    INC_STATS(thd_id,lat_l_loc_cc_time,total_cc_time);
    INC_STATS(thd_id,lat_l_loc_process_time,total_process_time);
    INC_STATS(thd_id,lat_l_loc_abort_time,total_abort_time);

    INC_STATS(thd_id,lat_s_loc_task_queue_time,task_queue_time);
    INC_STATS(thd_id,lat_s_loc_msg_queue_time,msg_queue_time);
    INC_STATS(thd_id,lat_s_loc_cc_block_time,cc_block_time);
    INC_STATS(thd_id,lat_s_loc_cc_time,cc_time);
    INC_STATS(thd_id,lat_s_loc_process_time,process_time);

    INC_STATS(thd_id,lat_short_task_queue_time,task_queue_time_short);
    INC_STATS(thd_id,lat_short_msg_queue_time,msg_queue_time_short);
    INC_STATS(thd_id,lat_short_cc_block_time,cc_block_time_short);
    INC_STATS(thd_id,lat_short_cc_time,cc_time_short);
    INC_STATS(thd_id,lat_short_process_time,process_time_short);
    INC_STATS(thd_id,lat_short_network_time,network_time_short);


  }
  else {
    INC_STATS(thd_id,lat_l_rem_task_queue_time,total_task_queue_time);
    INC_STATS(thd_id,lat_l_rem_msg_queue_time,total_msg_queue_time);
    INC_STATS(thd_id,lat_l_rem_cc_block_time,total_cc_block_time);
    INC_STATS(thd_id,lat_l_rem_cc_time,total_cc_time);
    INC_STATS(thd_id,lat_l_rem_process_time,total_process_time);
  }
  if (IS_LOCAL(txn_id)) {
    PRINT_LATENCY("lat_s %ld %ld %f %f %f %f %f %f\n"
          , txn_id
          , task_queue_cnt
          , (double) timespan_short / BILLION
          , (double) task_queue_time / BILLION
          , (double) msg_queue_time / BILLION
          , (double) cc_block_time / BILLION
          , (double) cc_time / BILLION
          , (double) process_time / BILLION
          );
   /*
  PRINT_LATENCY("lat_l %ld %ld %ld %f %f %f %f %f %f %f\n"
          , txn_id
          , total_task_queue_cnt
          , abort_cnt
          , (double) timespan_long / BILLION
          , (double) total_task_queue_time / BILLION
          , (double) total_msg_queue_time / BILLION
          , (double) total_cc_block_time / BILLION
          , (double) total_cc_time / BILLION
          , (double) total_process_time / BILLION
          , (double) total_abort_time / BILLION
          );
          */
  }
  else {
    PRINT_LATENCY("lat_rs %ld %ld %f %f %f %f %f %f\n"
          , txn_id
          , task_queue_cnt
          , (double) timespan_short / BILLION
          , (double) total_task_queue_time / BILLION
          , (double) total_msg_queue_time / BILLION
          , (double) total_cc_block_time / BILLION
          , (double) total_cc_time / BILLION
          , (double) total_process_time / BILLION
          );
  }
  /*
  if (!IS_LOCAL(txn_id) || timespan_short < timespan_long) {
    // latency from most recent start or restart of transaction
    PRINT_LATENCY("lat_s %ld %ld %f %f %f %f %f %f\n"
          , txn_id
          , task_queue_cnt
          , (double) timespan_short / BILLION
          , (double) task_queue_time / BILLION
          , (double) msg_queue_time / BILLION
          , (double) cc_block_time / BILLION
          , (double) cc_time / BILLION
          , (double) process_time / BILLION
          );
  }
  */
#endif

  if (!IS_LOCAL(txn_id)) {
      return;
  }

  INC_STATS(thd_id,txn_total_process_time,total_process_time);
  INC_STATS(thd_id,txn_process_time,process_time);
  INC_STATS(thd_id,txn_total_local_wait_time,total_local_wait_time);
  INC_STATS(thd_id,txn_local_wait_time,local_wait_time);
  INC_STATS(thd_id,txn_total_remote_wait_time,total_remote_wait_time);
  INC_STATS(thd_id,txn_remote_wait_time,remote_wait_time);
  INC_STATS(thd_id,txn_total_twopc_time,total_twopc_time);
  INC_STATS(thd_id,txn_twopc_time,two_pc_time);
  if(write_cnt > 0) {
    INC_STATS(thd_id,txn_write_cnt,1);
  }
  if(abort_cnt > 0) {
    INC_STATS(thd_id,unique_txn_abort_cnt,1);
  }

}


void Txn::init() {
  time_stamp = UINT64_MAX;
  txn_id = UINT64_MAX;
  batch_id = UINT64_MAX;
  DEBUG_M("Txn::init array insert_rows\n");
  insert_rows.init(g_max_items_per_txn + 10); 
  DEBUG_M("Txn::reset array access\n");
  access.init(MAX_ROW_PER_TXN);  

  reset(0);
}

void Txn::reset(uint64_t thd_id) {
  release_access(thd_id);
  access.clear();
  //release_insert(thd_id);
  insert_rows.clear();  
  write_cnt = 0;
  row_cnt = 0;
  twopc_state = START;
  rc = RCOK;
}

void Txn::release_access(uint64_t thd_id) {
  for(uint64_t i = 0; i < access.size(); i++) {
    acc_pool.put(thd_id,access[i]);
  }
}

void Txn::release_insert(uint64_t thd_id) {
  for(uint64_t i = 0; i < insert_rows.size(); i++) {
    RowData * row = insert_rows[i];
#if ALGO != OCCTEMPLATE
    DEBUG_M("TxnMgr::cleanup row->manager free\n");
    alloc_memory.free(row->manager, 0);
#endif
    row->free_row();
    DEBUG_M("Txn::release insert_rows free\n")
    row_pool.put(thd_id,row);
  }
}

void Txn::release(uint64_t thd_id) {
  DEBUG("Txn release\n");
  release_access(thd_id);
  DEBUG_M("Txn::release array access free\n")
  access.release();
  release_insert(thd_id);
  DEBUG_M("Txn::release array insert_rows free\n")
  insert_rows.release();
}

void TxnMgr::init(uint64_t thd_id, WLSchema * h_wl) {
  uint64_t prof_starttime = acquire_ts();
  if(!txn)  {
    DEBUG_M("Txn alloc\n");
    txn_pool.get(thd_id,txn);

  }
  INC_STATS(read_thd_id(),mtx[15],acquire_ts()-prof_starttime);
  prof_starttime = acquire_ts();
  //txn->init();
  if(!query) {
    DEBUG_M("TxnMgr::init Query alloc\n");
    query_pool.get(thd_id,query);
  }
  INC_STATS(read_thd_id(),mtx[16],acquire_ts()-prof_starttime);
  //query->init();
  //reset();
  sem_init(&rsp_mutex, 0, 1);
  return_id = UINT64_MAX;

	this->h_wl = h_wl;
#if ALGO == OCCTEMPLATE
  uc_writes = new std::set<uint64_t>();
  uc_writes_y = new std::set<uint64_t>();
  uc_reads = new std::set<uint64_t>();
#endif
#if ALGO == CALVIN
  phase = CALVIN_RW_ANALYSIS;
  locking_done = false;
  locked_rows_calvin.init(MAX_ROW_PER_TXN);
#endif
  
  txn_ready = true;
  twopl_wait_start = 0;

  txn_stats.init();
}

// reset after abort
void TxnMgr::reset() {
	lock_ready = false;
  lock_ready_cnt = 0;
  locking_done = true;
	ready_part = 0;
  response_cnt = 0;
  aborted = false;
  return_id = UINT64_MAX;
  twopl_wait_start = 0;

  //ready = true;

  // MaaT
  latest_wts = 0;
  latest_rts = 0;
  commit_timestamp = 0;
#if ALGO == OCCTEMPLATE
  uc_writes->clear();
  uc_writes_y->clear();
  uc_reads->clear();
#endif

#if ALGO == CALVIN
  phase = CALVIN_RW_ANALYSIS;
  locking_done = false;
  locked_rows_calvin.clear();
#endif

  assert(txn);
  assert(query);
  txn->reset(read_thd_id());

  // Stats
  txn_stats.reset();

}

void
TxnMgr::release() {
  uint64_t prof_starttime = acquire_ts();
  query_pool.put(read_thd_id(),query);
  INC_STATS(read_thd_id(),mtx[0],acquire_ts()-prof_starttime);
  query = NULL;
  prof_starttime = acquire_ts();
  txn_pool.put(read_thd_id(),txn);
  INC_STATS(read_thd_id(),mtx[1],acquire_ts()-prof_starttime);
  txn = NULL;

#if ALGO == OCCTEMPLATE
  delete uc_writes;
  delete uc_writes_y;
  delete uc_reads;
#endif
#if ALGO == CALVIN
  locked_rows_calvin.release();
#endif
  txn_ready = true;
}

void TxnMgr::reset_query() {
#if WORKLOAD == YCSB
  ((QryYCSB*)query)->reset();
#elif WORKLOAD == TPCC
  ((QryTPCC*)query)->reset();
#elif WORKLOAD == PPS
  ((PPSQry*)query)->reset();
#endif
}

RC TxnMgr::commit() {
  DEBUG("Commit %ld\n",read_txn_id());
  release_locks(RCOK);
#if ALGO == OCCTEMPLATE
  txn_timestamp_bounds.release(read_thd_id(),read_txn_id());
#endif
  stats_commit();
#if LOGGING
    RecordLog * record = logger.createRecord(read_txn_id(),L_NOTIFY,0,0);
    if(g_cnt_repl > 0) {
      msg_queue.enqueue(read_thd_id(),Msg::create_message(record,WKLOG_MSG),g_node_id + g_cnt_node + g_cl_node_cnt); 
    }
  logger.enqueueRecord(record);
  return WAIT;
#endif
  return Commit;
}

RC TxnMgr::abort() {
  if(aborted)
    return Abort;
  DEBUG("Abort %ld\n",read_txn_id());
  txn->rc = Abort;
  INC_STATS(read_thd_id(),total_txn_abort_cnt,1);
  txn_stats.abort_cnt++;
  if(IS_LOCAL(read_txn_id())) {
    INC_STATS(read_thd_id(), local_txn_abort_cnt, 1);
  } else {
    INC_STATS(read_thd_id(), remote_txn_abort_cnt, 1);
    txn_stats.stats_abort(read_thd_id());
  }

  aborted = true;
  release_locks(Abort);
#if ALGO == OCCTEMPLATE
  //assert(txn_timestamp_bounds.get_state(read_txn_id()) == OCCTEMPLATE_ABORTED);
  txn_timestamp_bounds.release(read_thd_id(),read_txn_id());
#endif

  uint64_t timespan = acquire_ts() - txn_stats.restart_begintime;
  if (IS_LOCAL(read_txn_id()) && warmup_done) {
      INC_STATS_ARR(read_thd_id(),start_abort_commit_latency, timespan);
  }

  return Abort;
}

RC TxnMgr::start_abort() {
  txn->rc = Abort;
  DEBUG("%ld start_abort\n",read_txn_id());
  if(query->parts_touched.size() > 1) {
    send_finish_msg();
    abort();
    return Abort;
  } 
  return abort();
}

RC TxnMgr::start_commit() {
  RC rc = RCOK;
  DEBUG("%ld start_commit RO?%d\n",read_txn_id(),query->readonly());

  rc = validate();
  if(rc == RCOK)
    rc = commit();
  else
    start_abort();
  
  return rc;
}

void TxnMgr::send_prepare_msg() {
  response_cnt = query->parts_touched.size() - 1;
  DEBUG("%ld Send PREPARE messages to %d\n",read_txn_id(),response_cnt);
  for(uint64_t i = 0; i < query->parts_touched.size(); i++) {
    if(GET_NODE_ID(query->parts_touched[i]) == g_node_id) {
      continue;
    }
    msg_queue.enqueue(read_thd_id(),Msg::create_message(this,WKRPREPARE),GET_NODE_ID(query->parts_touched[i]));
  }
}

void TxnMgr::send_finish_msg() {
  response_cnt = query->parts_touched.size() - 1;
  assert(IS_LOCAL(read_txn_id()));
  DEBUG("%ld Send FINISH messages to %d\n",read_txn_id(),response_cnt);
  for(uint64_t i = 0; i < query->parts_touched.size(); i++) {
    if(GET_NODE_ID(query->parts_touched[i]) == g_node_id) {
      continue;
    }
    msg_queue.enqueue(read_thd_id(),Msg::create_message(this,WKRFIN),GET_NODE_ID(query->parts_touched[i]));
  }
}

int TxnMgr::received_rps(RC rc) {
  assert(txn->rc == RCOK || txn->rc == Abort);
  if(txn->rc == RCOK)
    txn->rc = rc;
#if ALGO == CALVIN
  ++response_cnt;
#else
  --response_cnt;
#endif
  return response_cnt;
}

bool TxnMgr::waiting_for_rps() {
  return response_cnt > 0;
}

bool TxnMgr::is_multi_part() {
  return query->parts_touched.size() > 1;
  //return query->parts_assign.size() > 1;
}

void TxnMgr::stats_commit() {
    uint64_t commit_time = acquire_ts();
    uint64_t timespan_short = commit_time - txn_stats.restart_begintime;
    uint64_t timespan_long  = commit_time - txn_stats.begintime;
    INC_STATS(read_thd_id(),total_txn_commit_cnt,1);

    if(!IS_LOCAL(read_txn_id()) && ALGO != CALVIN) {
      INC_STATS(read_thd_id(),remote_txn_commit_cnt,1);
      txn_stats.stats_commit(read_thd_id(),read_txn_id(),get_batch_id(), timespan_long, timespan_short);
      return;
    }


    INC_STATS(read_thd_id(),txn_cnt,1);
    INC_STATS(read_thd_id(),local_txn_commit_cnt,1);
    INC_STATS(read_thd_id(), txn_run_time, timespan_long);
    if(query->parts_touched.size() > 1) {
      INC_STATS(read_thd_id(),multi_part_txn_cnt,1);
      INC_STATS(read_thd_id(),multi_part_txn_run_time,timespan_long);
    } else {
      INC_STATS(read_thd_id(),single_part_txn_cnt,1);
      INC_STATS(read_thd_id(),single_part_txn_run_time,timespan_long);
    }
    /*if(cflt) {
      INC_STATS(read_thd_id(),cflt_cnt_txn,1);
    }*/
    txn_stats.stats_commit(read_thd_id(),read_txn_id(),get_batch_id(),timespan_long, timespan_short);
  #if ALGO == CALVIN
    return;
  #endif

    INC_STATS_ARR(read_thd_id(),start_abort_commit_latency, timespan_short);
    INC_STATS_ARR(read_thd_id(),last_start_commit_latency, timespan_short);
    INC_STATS_ARR(read_thd_id(),first_start_commit_latency, timespan_long);
#if WORKLOAD != TEST
    assert(query->parts_touched.size() > 0);
    INC_STATS(read_thd_id(),parts_touched,query->parts_touched.size());
    INC_STATS(read_thd_id(),part_cnt[query->parts_touched.size()-1],1);
    for(uint64_t i = 0 ; i < query->parts_touched.size(); i++) {
        INC_STATS(read_thd_id(),part_acc[query->parts_touched[i]],1);
    }
#endif
}

void TxnMgr::register_thread(Thread * h_thd) {
  this->h_thd = h_thd;
#if ALGO == HSTORE || ALGO == HSTORE_SPEC
  this->active_part = GET_PART_ID_FROM_IDX(read_thd_id());
#endif
}

void TxnMgr::set_txn_id(txnid_t txn_id) {
	txn->txn_id = txn_id;
}

txnid_t TxnMgr::read_txn_id() {
	return txn->txn_id;
}

WLSchema * TxnMgr::get_wl() {
	return h_wl;
}

uint64_t TxnMgr::read_thd_id() {
  if(h_thd)
    return h_thd->read_thd_id();
  else
    return 0;
}

BaseQry * TxnMgr::get_query() {
	return query;
}
void TxnMgr::set_query(BaseQry * qry) {
	query = qry;
}

void TxnMgr::set_timestamp(ts_t time_stamp) {
	txn->time_stamp = time_stamp;
}

ts_t TxnMgr::get_timestamp() {
	return txn->time_stamp;
}

uint64_t TxnMgr::incr_lr() {
  //ATOM_ADD(this->response_cnt,i);
  uint64_t result;
  sem_wait(&rsp_mutex);
  result = ++this->lock_ready_cnt;
  sem_post(&rsp_mutex);
  return result;
}

uint64_t TxnMgr::decr_lr() {
  //ATOM_SUB(this->response_cnt,i);
  uint64_t result;
  sem_wait(&rsp_mutex);
  result = --this->lock_ready_cnt;
  sem_post(&rsp_mutex);
  return result;
}
uint64_t TxnMgr::incr_rsp(int i) {
  //ATOM_ADD(this->response_cnt,i);
  uint64_t result;
  sem_wait(&rsp_mutex);
  result = ++this->response_cnt;
  sem_post(&rsp_mutex);
  return result;
}

uint64_t TxnMgr::decr_rsp(int i) {
  //ATOM_SUB(this->response_cnt,i);
  uint64_t result;
  sem_wait(&rsp_mutex);
  result = --this->response_cnt;
  sem_post(&rsp_mutex);
  return result;
}

void TxnMgr::release_last_lock() {
  assert(txn->row_cnt > 0);
  RowData * orig_r = txn->access[txn->row_cnt-1]->orig_row;
  access_t type = txn->access[txn->row_cnt-1]->type;
  orig_r->return_row(RCOK, type, this, NULL);
  //txn->access[txn->row_cnt-1]->orig_row = NULL;
}

void TxnMgr::cleanup_row(RC rc, uint64_t rid) {
    access_t type = txn->access[rid]->type;
    if (type == WR && rc == Abort && ALGO != OCCTEMPLATE) {
        type = XP;
    }

    // Handle calvin elsewhere
#if ALGO != CALVIN
#if ISOLATION_LEVEL != READ_UNCOMMITTED
    RowData * orig_r = txn->access[rid]->orig_row;
    if (ROLL_BACK && type == XP &&
                (ALGO == DL_DETECT ||
      ALGO == HSTORE ||
      ALGO == HSTORE_SPEC
      ))
    {
        orig_r->return_row(rc,type, this, txn->access[rid]->orig_data);
    } else {
#if ISOLATION_LEVEL == READ_COMMITTED
        if(type == WR) {
          orig_r->return_row(rc,type, this, txn->access[rid]->data);
        }
#else
        orig_r->return_row(rc,type, this, txn->access[rid]->data);
#endif
    }
#endif

#if ROLL_BACK && (ALGO == HSTORE || ALGO == HSTORE_SPEC)
    if (type == WR) {
        //printf("free 10 %ld\n",read_txn_id());
              txn->access[rid]->orig_data->free_row();
        DEBUG_M("TxnMgr::cleanup RowData free\n");
        row_pool.put(read_thd_id(),txn->access[rid]->orig_data);
        if(rc == RCOK) {
            INC_STATS(read_thd_id(),record_write_cnt,1);
            ++txn_stats.write_cnt;
        }
    }
#endif
#endif
    txn->access[rid]->data = NULL;

}

void TxnMgr::cleanup(RC rc) {

    ts_t begintime = acquire_ts();
    uint64_t row_cnt = txn->access.get_count();
    assert(txn->access.get_count() == txn->row_cnt);
    //assert((WORKLOAD == YCSB && row_cnt <= g_per_qry_req) || (WORKLOAD == TPCC && row_cnt <= g_max_items_per_txn*2 + 3));


    DEBUG("Cleanup %ld %ld\n",read_txn_id(),row_cnt);
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
	    cleanup_row(rc,rid);
	}
#if ALGO == CALVIN
	// cleanup locked rows
    for (uint64_t i = 0; i < locked_rows_calvin.size(); i++) {
        RowData * row = locked_rows_calvin[i];
        row->return_row(rc,RD,this,row);
    }
#endif

	if (rc == Abort) {
	    txn->release_insert(read_thd_id());
	    txn->insert_rows.clear();

      INC_STATS(read_thd_id(), abort_time, acquire_ts() - begintime);
	} 
}

RC TxnMgr::get_lock(RowData * row, access_t type) {
    if (locked_rows_calvin.contains(row)) {
        return RCOK;
    }
    locked_rows_calvin.add(row);
    RC rc = row->get_lock(type, this);
    if(rc == WAIT) {
      INC_STATS(read_thd_id(), txn_wait_cnt, 1);
    }
    return rc;
}

RC TxnMgr::get_row(RowData * row, access_t type, RowData *& row_rtn) {
    uint64_t begintime = acquire_ts();
    uint64_t timespan;
    RC rc = RCOK;
    DEBUG_M("TxnMgr::get_row access alloc\n");
    Access * access;
    acc_pool.get(read_thd_id(),access);
    //uint64_t row_cnt = txn->row_cnt;
    //assert(txn->access.get_count() - 1 == row_cnt);

    this->last_row = row;
    this->last_type = type;

    rc = row->get_row(type, this, access->data);

    if (rc == Abort || rc == WAIT) {
        row_rtn = NULL;
        DEBUG_M("TxnMgr::get_row(abort) access free\n");
        acc_pool.put(read_thd_id(),access);
        timespan = acquire_ts() - begintime;
        INC_STATS(read_thd_id(), txn_manager_time, timespan);
        INC_STATS(read_thd_id(), txn_conflict_cnt, 1);
        //cflt = true;
#if DEBUG_TIMELINE
        printf("CONFLICT %ld %ld\n",read_txn_id(),acquire_ts());
#endif
        return rc;
    }
	access->type = type;
	access->orig_row = row;
#if ROLL_BACK && (ALGO == DL_DETECT || ALGO == HSTORE || ALGO == HSTORE_SPEC)
	if (type == WR) {
    //printf("alloc 10 %ld\n",read_txn_id());
    uint64_t part_id = row->get_part_id();
    DEBUG_M("TxnMgr::get_row RowData alloc\n")
    row_pool.get(read_thd_id(),access->orig_data);
    access->orig_data->init(row->get_table(), part_id, 0);
    access->orig_data->copy(row);
    assert(access->orig_data->get_tbl_schema() == row->get_tbl_schema());

    // ARIES-style physiological logging
#if LOGGING
    //RecordLog * record = logger.createRecord(LRT_UPDATE,L_UPDATE,read_txn_id(),part_id,row->get_table()->get_table_id(),row->get_primary_key());
    RecordLog * record = logger.createRecord(read_txn_id(),L_UPDATE,row->get_table()->get_table_id(),row->get_primary_key());
    if(g_cnt_repl > 0) {
      msg_queue.enqueue(read_thd_id(),Msg::create_message(record,WKLOG_MSG),g_node_id + g_cnt_node + g_cl_node_cnt); 
    }
    logger.enqueueRecord(record);
#endif

	}
#endif

	++txn->row_cnt;
	if (type == WR)
		++txn->write_cnt;

  txn->access.add(access);

	timespan = acquire_ts() - begintime;
	INC_STATS(read_thd_id(), txn_manager_time, timespan);
	row_rtn  = access->data;


  if(ALGO == HSTORE || ALGO == HSTORE_SPEC || ALGO == CALVIN)
    assert(rc == RCOK);
  return rc;
}

RC TxnMgr::get_row_post_wait(RowData *& row_rtn) {
  assert(ALGO != HSTORE && ALGO != HSTORE_SPEC);

	uint64_t begintime = acquire_ts();
  RowData * row = this->last_row;
  access_t type = this->last_type;
  assert(row != NULL);
  DEBUG_M("TxnMgr::get_row_post_wait access alloc\n")
  Access * access;
  acc_pool.get(read_thd_id(),access);

  row->get_row_post_wait(type,this,access->data);

	access->type = type;
	access->orig_row = row;
#if ROLL_BACK && (ALGO == DL_DETECT)
	if (type == WR) {
	  uint64_t part_id = row->get_part_id();
    //printf("alloc 10 %ld\n",read_txn_id());
    DEBUG_M("TxnMgr::get_row_post_wait RowData alloc\n")
    row_pool.get(read_thd_id(),access->orig_data);
		access->orig_data->init(row->get_table(), part_id, 0);
		access->orig_data->copy(row);
	}
#endif

	++txn->row_cnt;
	if (type == WR)
		++txn->write_cnt;


  txn->access.add(access);
	uint64_t timespan = acquire_ts() - begintime;
	INC_STATS(read_thd_id(), txn_manager_time, timespan);
	this->last_row_rtn  = access->data;
	row_rtn  = access->data;
  return RCOK;

}

void TxnMgr::insert_row(RowData * row, TableSchema * table) {
	if (ALGO == HSTORE || ALGO == HSTORE_SPEC)
		return;
	assert(txn->insert_rows.size() < MAX_ROW_PER_TXN);
  txn->insert_rows.add(row);
}

itemidData *
TxnMgr::index_read(INDEX * index, idx_key_t key, int part_id) {
	uint64_t begintime = acquire_ts();

	itemidData * item;
	index->index_read(key, item, part_id, read_thd_id());

  uint64_t t = acquire_ts() - begintime;
  INC_STATS(read_thd_id(), txn_index_time, t);
  //txn_time_idx += t;

	return item;
}

itemidData *
TxnMgr::index_read(INDEX * index, idx_key_t key, int part_id, int count) {
	uint64_t begintime = acquire_ts();

	itemidData * item;
	index->index_read(key, count, item, part_id);

  uint64_t t = acquire_ts() - begintime;
  INC_STATS(read_thd_id(), txn_index_time, t);
  //txn_time_idx += t;

	return item;
}


RC TxnMgr::validate() {
#if MODE != NORMAL_MODE
  return RCOK;
#endif
  if (ALGO != OCCTEMPLATE) {
      return RCOK;
  }
  RC rc = RCOK;
  uint64_t begintime = acquire_ts();
  if(ALGO == OCCTEMPLATE && rc == RCOK) {
    rc = occ_template_man.validate(this);
    // Note: home node must be last to validate
    if(IS_LOCAL(read_txn_id()) && rc == RCOK) {
      rc = occ_template_man.find_bound(this);
    }
  }
  INC_STATS(read_thd_id(),txn_validate_time,acquire_ts() - begintime);
  return rc;
}

RC
TxnMgr::send_reads_remote() {
  assert(ALGO == CALVIN);
#if !YCSB_ABORT_MODE && WORKLOAD == YCSB
  return RCOK;
#endif
  assert(query->nodes_active.size() == g_cnt_node);
  for(uint64_t i = 0; i < query->nodes_active.size(); i++) {
    if(i == g_node_id)
      continue;
    if(query->nodes_active[i] == 1) {
      DEBUG("(%ld,%ld) send_remote_read to %ld\n",read_txn_id(),get_batch_id(),i);
      msg_queue.enqueue(read_thd_id(),Msg::create_message(this,WKRFWD),i);
    }
  }
  return RCOK;

}

bool TxnMgr::calvin_exec_phase_done() {
  bool ready =  (phase == CALVIN_DONE) && (get_rc() != WAIT);
  if(ready) {
    DEBUG("(%ld,%ld) calvin exec phase done!\n",txn->txn_id,txn->batch_id);
  }
  return ready;
}

bool TxnMgr::calvin_collect_phase_done() {
  bool ready =  (phase == CALVIN_COLLECT_RD) && (get_rsp_cnt() == calvin_expected_rsp_cnt);
  if(ready) {
    DEBUG("(%ld,%ld) calvin collect phase done!\n",txn->txn_id,txn->batch_id);
  }
  return ready;
}

void TxnMgr::release_locks(RC rc) {
	uint64_t begintime = acquire_ts();

  cleanup(rc);

	uint64_t timespan = (acquire_ts() - begintime);
	INC_STATS(read_thd_id(), txn_cleanup_time,  timespan);
}
