
#include "global.h"
#include "universal.h"
#include "ycsb.h"
#include "qry_ycsb.h"
#include "workload.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "IndexHash.h"
#include "IndexBTree.h"
#include "catalog.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "msg_queue.h"
#include "message.h"

void TxnManYCSB::init(uint64_t thd_id, WLSchema * h_wl) {
	TxnMgr::init(thd_id, h_wl);
	_wl = (WLYCSB *) h_wl;
  reset();
}

void TxnManYCSB::reset() {
  state = YCSB_0;
  next_record_id = 0;
	TxnMgr::reset();
}

RC TxnManYCSB::acquire_locks() {
  uint64_t begintime = acquire_ts();
  assert(ALGO == CALVIN);
  QryYCSB* ycsb_query = (QryYCSB*) query;
  locking_done = false;
  RC rc = RCOK;
  incr_lr();
  assert(ycsb_query->requests.size() == g_per_qry_req);
  assert(phase == CALVIN_RW_ANALYSIS);
	for (uint32_t rid = 0; rid < ycsb_query->requests.size(); rid ++) {
		rqst_ycsb * req = ycsb_query->requests[rid];
		uint64_t part_id = _wl->key_to_part( req->key );
    DEBUG("LK Acquire (%ld,%ld) %d,%ld -> %ld\n",read_txn_id(),get_batch_id(),req->acctype,req->key,GET_NODE_ID(part_id));
    if(GET_NODE_ID(part_id) != g_node_id)
      continue;
		INDEX * index = _wl->the_index;
		itemidData * item;
		item = index_read(index, req->key, part_id);
		RowData * row = ((RowData *)item->location);
		RC rc2 = get_lock(row,req->acctype);
    if(rc2 != RCOK) {
      rc = rc2;
    }
	}
  if(decr_lr() == 0) {
    if(ATOM_CAS(lock_ready,false,true))
      rc = RCOK;
  }
  txn_stats.wait_begintime = acquire_ts();
  /*
  if(rc == WAIT && lock_ready_cnt == 0) {
    if(ATOM_CAS(lock_ready,false,true))
    //lock_ready = true;
      rc = RCOK;
  }
  */
  INC_STATS(read_thd_id(),calvin_sched_time,acquire_ts() - begintime);
  locking_done = true;
  return rc;
}


RC TxnManYCSB::run_txn() {
  RC rc = RCOK;
  assert(ALGO != CALVIN);

  if(IS_LOCAL(txn->txn_id) && state == YCSB_0 && next_record_id == 0) {
    DEBUG("Running txn %ld\n",txn->txn_id);
    //query->print();
    query->parts_touched.add_unique(GET_PART_ID(0,g_node_id));
  }

  uint64_t begintime = acquire_ts();

  while(rc == RCOK && !is_done()) {
    rc = run_txn_state();
  }

  uint64_t curr_time = acquire_ts();
  txn_stats.process_time += curr_time - begintime;
  txn_stats.process_time_short += curr_time - begintime;
  txn_stats.wait_begintime = acquire_ts();

  if(IS_LOCAL(read_txn_id())) {
    if(is_done() && rc == RCOK) 
      rc = start_commit();
    else if(rc == Abort)
      rc = start_abort();
  } else if(rc == Abort){
    rc = abort();
  }

  return rc;

}

RC TxnManYCSB::run_txn_post_wait() {
    get_row_post_wait(row);
    next_ycsb_state();
    return RCOK;
}

bool TxnManYCSB::is_done() {
  return next_record_id == ((QryYCSB*)query)->requests.size();
}

void TxnManYCSB::next_ycsb_state() {
  switch(state) {
    case YCSB_0:
      state = YCSB_1;
      break;
    case YCSB_1:
      next_record_id++;
      if(!IS_LOCAL(txn->txn_id) || !is_done()) {
        state = YCSB_0;
      }
      else {
        state = YCSB_FIN;
      }
      break;
    case YCSB_FIN:
      break;
    default:
      assert(false);
  }
}

bool TxnManYCSB::is_local_request(uint64_t idx) {
  return GET_NODE_ID(_wl->key_to_part(((QryYCSB*)query)->requests[idx]->key)) == g_node_id;
}


void TxnManYCSB::copy_remote_requests(QueryMsgYCSB * message) {
  QryYCSB* ycsb_query = (QryYCSB*) query;
  //message->requests.init(ycsb_query->requests.size());
  uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);

  while(next_record_id < ycsb_query->requests.size() && !is_local_request(next_record_id) && GET_NODE_ID(ycsb_query->requests[next_record_id]->key) == dest_node_id) {
    QryYCSB::copy_request_to_msg(ycsb_query,message,next_record_id++);
  }
}

RC TxnManYCSB::run_txn_state() {
  QryYCSB* ycsb_query = (QryYCSB*) query;
	rqst_ycsb * req = ycsb_query->requests[next_record_id];
	uint64_t part_id = _wl->key_to_part( req->key );
  bool loc = GET_NODE_ID(part_id) == g_node_id;

	RC rc = RCOK;

	switch (state) {
		case YCSB_0 :
      if(loc) {
        rc = run_ycsb_0(req,row);
      } 
      break;
		case YCSB_1 :
      rc = run_ycsb_1(req->acctype,row);
      break;
    case YCSB_FIN :
      state = YCSB_FIN;
      break;
    default:
			assert(false);
  }

  if(rc == RCOK)
    next_ycsb_state();

  return rc;
}

RC TxnManYCSB::run_ycsb_0(rqst_ycsb * req,RowData *& row_local) {
    RC rc = RCOK;
		int part_id = _wl->key_to_part( req->key );
		access_t type = req->acctype;
	  itemidData * m_item;

		m_item = index_read(_wl->the_index, req->key, part_id);

		RowData * row = ((RowData *)m_item->location);
			
		rc = get_row(row, type,row_local);

    return rc;

}

RC TxnManYCSB::run_ycsb_1(access_t acctype, RowData * row_local) {
  if (acctype == RD || acctype == SCAN) {
    int fid = 0;
		char * data = row_local->get_data();
		uint64_t fval __attribute__ ((unused));
    fval = *(uint64_t *)(&data[fid * 100]);
#if ISOLATION_LEVEL == READ_COMMITTED || ISOLATION_LEVEL == READ_UNCOMMITTED
    // Release lock after read
    release_last_lock(); 
#endif

  } else {
    assert(acctype == WR);
		int fid = 0;
	  char * data = row_local->get_data();
	  *(uint64_t *)(&data[fid * 100]) = 0;
#if YCSB_ABORT_MODE
    if(data[0] == 'a')
      return RCOK;
#endif

#if ISOLATION_LEVEL == READ_UNCOMMITTED
    // Release lock after write
    release_last_lock(); 
#endif
  } 
  return RCOK;
}
RC TxnManYCSB::run_calvin_txn() {
  RC rc = RCOK;
  uint64_t begintime = acquire_ts();
  QryYCSB* ycsb_query = (QryYCSB*) query;
  DEBUG("(%ld,%ld) Run calvin txn\n",txn->txn_id,txn->batch_id);
  while(!calvin_exec_phase_done() && rc == RCOK) {
    DEBUG("(%ld,%ld) phase %d\n",txn->txn_id,txn->batch_id,this->phase);
    switch(this->phase) {
      case CALVIN_RW_ANALYSIS:

        // Phase 1: Read/write set analysis
        calvin_expected_rsp_cnt = ycsb_query->get_participants(_wl);
#if YCSB_ABORT_MODE
        if(query->related_nodes[g_node_id] == 1) {
          calvin_expected_rsp_cnt--;
        }
#else
        calvin_expected_rsp_cnt = 0;
#endif
        DEBUG("(%ld,%ld) expects %d responses;\n",txn->txn_id,txn->batch_id,calvin_expected_rsp_cnt);

        this->phase = CALVIN_LOC_RD;
        break;
      case CALVIN_LOC_RD:
        // Phase 2: Perform local reads
        DEBUG("(%ld,%ld) local reads\n",txn->txn_id,txn->batch_id);
        rc = run_ycsb();
        //release_read_locks(query);

        this->phase = CALVIN_SERVE_RD;
        break;
      case CALVIN_SERVE_RD:
        // Phase 3: Serve remote reads
        // If there is any abort logic, relevant reads need to be sent to all active nodes...
        if(query->related_nodes[g_node_id] == 1) {
          rc = send_reads_remote();
        }
        if(query->nodes_active[g_node_id] == 1) {
          this->phase = CALVIN_COLLECT_RD;
          if(calvin_collect_phase_done()) {
            rc = RCOK;
          } else {
            DEBUG("(%ld,%ld) wait in collect phase; %d / %d rfwds received\n",txn->txn_id,txn->batch_id,response_cnt,calvin_expected_rsp_cnt);
            rc = WAIT;
          }
        } else { // Done
          rc = RCOK;
          this->phase = CALVIN_DONE;
        }

        break;
      case CALVIN_COLLECT_RD:
        // Phase 4: Collect remote reads
        this->phase = CALVIN_EXEC_WR;
        break;
      case CALVIN_EXEC_WR:
        // Phase 5: Execute transaction / perform local writes
        DEBUG("(%ld,%ld) execute writes\n",txn->txn_id,txn->batch_id);
        rc = run_ycsb();
        this->phase = CALVIN_DONE;
        break;
      default:
        assert(false);
    }
  }
  uint64_t curr_time = acquire_ts();
  txn_stats.process_time += curr_time - begintime;
  txn_stats.process_time_short += curr_time - begintime;
  txn_stats.wait_begintime = acquire_ts();
  return rc;
}

RC TxnManYCSB::run_ycsb() {
  RC rc = RCOK;
  assert(ALGO == CALVIN);
  QryYCSB* ycsb_query = (QryYCSB*) query;
  
  for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
	  rqst_ycsb * req = ycsb_query->requests[i];
    if(this->phase == CALVIN_LOC_RD && req->acctype == WR)
      continue;
    if(this->phase == CALVIN_EXEC_WR && req->acctype == RD)
      continue;

		uint64_t part_id = _wl->key_to_part( req->key );
    bool loc = GET_NODE_ID(part_id) == g_node_id;

    if(!loc)
      continue;

    rc = run_ycsb_0(req,row);
    assert(rc == RCOK);

    rc = run_ycsb_1(req->acctype,row);
    assert(rc == RCOK);
  }
  return rc;

}

