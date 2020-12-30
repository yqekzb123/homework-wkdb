
#include "global.h"
#include "manager.h"
#include "thread.h"
#include "task_thread.h"
#include "txn.h"
#include "workload.h"
#include "query.h"
#include "qry_ycsb.h"
#include "qry_tpcc.h"
#include "test.h"
#include "math.h"
#include "universal.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "task_queue.h"
#include "logMan.h"
#include "message.h"
#include "abort_txn_queue.h"
#include "occ_template.h"

void TaskThread::setup() {

	if( read_thd_id() == 0) {
	send_init_done_to_nodes();
  }
  _thd_txn_id = 0;

}

void TaskThread::process(Msg * message) {
  RC rc __attribute__ ((unused));

  DEBUG("%ld Processing %ld %d\n",read_thd_id(),message->read_txn_id(),message->get_rtype());
  assert(message->get_rtype() == WKCQRY || message->read_txn_id() != UINT64_MAX);
  uint64_t begintime = acquire_ts();
		switch(message->get_rtype()) {
			case WKRPREPARE: 
        rc = rprepare_process(message);
				break;
			case WKRFWD:
        rc = rfwd_process(message);
				break;
			case WKRQRY:
        rc = rqry_process(message);
				break;
			case WKRQRY_CONT:
        rc = rqry_cont_process(message);
				break;
			case WKRQRY_RSP:
        rc = rqry_rsp_process(message);
				break;
			case WKRFIN: 
        rc = rfin_process(message);
				break;
			case WKRACK_PREP:
        rc = rack_prep_process(message);
				break;
			case WKRACK_FIN:
        rc = rack_rfin_process(message);
				break;
			case WKRTXN_CONT:
        rc = rtxn_cont_process(message);
				break;
      case WKCQRY:
			case WKRTXN:
#if ALGO == CALVIN
        rc = calvin_rtxn_process(message);
#else
        rc = rtxn_process(message);
#endif
				break;
			case WKLOG_FLUSHED:
        rc = log_flushed_process(message);
				break;
			case WKLOG_MSG:
        rc = log_msg_process(message);
				break;
			case WKLOG_MSG_RSP:
        rc = log_msg_rsp_process(message);
				break;
			default:
        printf("Msg: %d\n",message->get_rtype());
        fflush(stdout);
				assert(false);
				break;
		}
  uint64_t timespan = acquire_ts() - begintime;
  INC_STATS(read_thd_id(),worker_process_cnt,1);
  INC_STATS(read_thd_id(),worker_process_time,timespan);
  INC_STATS(read_thd_id(),worker_process_cnt_by_type[message->rtype],1);
  INC_STATS(read_thd_id(),worker_process_time_by_type[message->rtype],timespan);
  DEBUG("%ld EndProcessing %d %ld\n",read_thd_id(),message->get_rtype(),message->read_txn_id());
}

void TaskThread::check_if_done(RC rc) {
  if(txn_man->waiting_for_rps())
	return;
  if(rc == Commit)
	commit();
  if(rc == Abort)
	abort();
}

void TaskThread::release_txn_manager() {
  txn_table.release_transaction_manager(read_thd_id(),txn_man->read_txn_id(),txn_man->get_batch_id());
  txn_man = NULL;
}

void TaskThread::calvin_wrapup() {
  txn_man->release_locks(RCOK);
  txn_man->stats_commit();
  DEBUG("(%ld,%ld) calvin ack to %ld\n",txn_man->read_txn_id(),txn_man->get_batch_id(),txn_man->return_id);
  if(txn_man->return_id == g_node_id) {
    task_queue.sequencer_enqueue(_thd_id,Msg::create_message(txn_man,WKACK_CALVIN));
  } else {
    msg_queue.enqueue(read_thd_id(),Msg::create_message(txn_man,WKACK_CALVIN),txn_man->return_id);
  }
  release_txn_manager();
}

// Can't use txn_man after this function
void TaskThread::commit() {
  //TxnMgr * txn_man = txn_table.get_transaction_manager(txn_id,0);
  //txn_man->release_locks(RCOK);
  //        txn_man->stats_commit();
  assert(txn_man);
  assert(IS_LOCAL(txn_man->read_txn_id()));

  uint64_t timespan = acquire_ts() - txn_man->txn_stats.begintime;
  DEBUG("COMMIT %ld %f -- %f\n",txn_man->read_txn_id(),simulate_man->seconds_from_begin(acquire_ts()),(double)timespan/ BILLION);

  // Send result back to client
#if !SERVER_GENERATE_QUERIES
  msg_queue.enqueue(read_thd_id(),Msg::create_message(txn_man,WKCL_RSP),txn_man->client_id);
#endif
  // remove txn from pool
  release_txn_manager();
  // Do not use txn_man after this

}

void TaskThread::abort() {

  DEBUG("ABORT %ld -- %f\n",txn_man->read_txn_id(),(double)acquire_ts() - run_begintime/ BILLION);
  // TODO: TPCC Rollback here

  ++txn_man->abort_cnt;
  txn_man->reset();
#if WORKLOAD != DA
  uint64_t penalty = abort_queue.enqueue(read_thd_id(), txn_man->read_txn_id(),txn_man->get_abort_cnt());
  txn_man->txn_stats.total_abort_time += penalty;
#endif

}

TxnMgr * TaskThread::get_transaction_manager(Msg * message) {
#if ALGO == CALVIN
  TxnMgr * local_txn_man = txn_table.get_transaction_manager(read_thd_id(),message->read_txn_id(),message->get_batch_id());
#else
  TxnMgr * local_txn_man = txn_table.get_transaction_manager(read_thd_id(),message->read_txn_id(),0);
#endif
  return local_txn_man;
}

char type2char(DATxnType txn_type)
{
  switch (txn_type)
  {
    case DA_READ:
      return 'R';
    case DA_WRITE:
      return 'W';
    case DA_COMMIT:
      return 'C';
    case DA_ABORT:
      return 'A';
    case DA_SCAN:
      return 'S';
    default:
      return 'U';
  }
}

RC TaskThread::run() {
  tsetup();
  printf("Running TaskThread %ld\n",_thd_id);

  uint64_t ready_starttime;
  uint64_t idle_starttime = 0;

	while(!simulate_man->is_done()) {
    txn_man = NULL;
    heartbeat();

    progress_stats();
    Msg * message;
    // #define TEST_MSG_order
    #ifdef TEST_MSG_order
      while(1)
      {
      message = task_queue.dequeue(read_thd_id());
      if (!message) {
        if (idle_starttime == 0) idle_starttime = acquire_ts();
        continue;
      }
      printf("s seq_id:%lu type:%c trans_id:%lu item:%c state:%lu next_state:%lu\n",
      ((DAClientQueryMessage*)message)->seq_id,
      type2char(((DAClientQueryMessage*)message)->txn_type),
      ((DAClientQueryMessage*)message)->trans_id,
      static_cast<char>('x'+((DAClientQueryMessage*)message)->item_id),
      ((DAClientQueryMessage*)message)->state,
      (((DAClientQueryMessage*)message)->next_state));
      fflush(stdout);
      }
    #endif
    message = task_queue.dequeue(read_thd_id());
    if(!message) {
      if(idle_starttime ==0)
      idle_starttime = acquire_ts();
      continue;
    }

    simulate_man->last_da_query_time = acquire_ts();

    if(idle_starttime > 0) {
      INC_STATS(_thd_id,worker_idle_time,acquire_ts() - idle_starttime);
      idle_starttime = 0;
    }
    //uint64_t begintime = acquire_ts();

    if(message->rtype != WKCQRY || ALGO == CALVIN) {
      txn_man = get_transaction_manager(message);

	  if (ALGO != CALVIN && IS_LOCAL(txn_man->read_txn_id())) {
		if (message->rtype != WKRTXN_CONT && ((message->rtype != WKRACK_PREP) || (txn_man->get_rsp_cnt() == 1))) {
		  txn_man->txn_stats.task_queue_time_short += message->lat_task_queue_time;
		  txn_man->txn_stats.cc_block_time_short += message->lat_cc_block_time;
		  txn_man->txn_stats.cc_time_short += message->lat_cc_time;
		  txn_man->txn_stats.msg_queue_time_short += message->lat_msg_queue_time;
  	  txn_man->txn_stats.process_time_short += message->lat_process_time;

		  txn_man->txn_stats.network_time_short += message->lat_network_time;
		}

	  } else {
		  txn_man->txn_stats.clear_short();
	  }
	  if (ALGO != CALVIN) {
      txn_man->txn_stats.lat_network_time_start = message->lat_network_time;
      txn_man->txn_stats.lat_other_time_start = message->lat_other_time;
	  }
	  txn_man->txn_stats.msg_queue_time += message->mq_time;
	  txn_man->txn_stats.msg_queue_time_short += message->mq_time;
	  message->mq_time = 0;
	  txn_man->txn_stats.task_queue_time += message->wq_time;
	  txn_man->txn_stats.task_queue_time_short += message->wq_time;
	  //txn_man->txn_stats.network_time += message->ntwk_time;
	  message->wq_time = 0;
	  txn_man->txn_stats.task_queue_cnt += 1;


	  ready_starttime = acquire_ts();
	  bool ready = txn_man->unset_ready();
	  INC_STATS(read_thd_id(),worker_activate_txn_time,acquire_ts() - ready_starttime);
	  if(!ready) {
      // Return to work queue, end processing
      task_queue.enqueue(read_thd_id(),message,true);
      continue;
	  }
	  txn_man->register_thread(this);
	}

	process(message);

	ready_starttime = acquire_ts();
	if(txn_man) {
	  bool ready = txn_man->set_ready();
	  assert(ready);
	}
	INC_STATS(read_thd_id(),worker_deactivate_txn_time,acquire_ts() - ready_starttime);

	// delete message
	ready_starttime = acquire_ts();
#if ALGO != CALVIN
    message->release();
#endif
    INC_STATS(read_thd_id(),worker_release_msg_time,acquire_ts() - ready_starttime);

	}
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

RC TaskThread::rfin_process(Msg * message) {
  DEBUG("WKRFIN %ld\n",message->read_txn_id());
  assert(ALGO != CALVIN);

  M_ASSERT_V(!IS_LOCAL(message->read_txn_id()),"WKRFIN local: %ld %ld/%d\n",message->read_txn_id(),message->read_txn_id()%g_cnt_node,g_node_id);
#if ALGO == OCCTEMPLATE
  txn_man->set_commit_timestamp(((FinishMessage*)message)->commit_timestamp);
#endif

  if(((FinishMessage*)message)->rc == Abort) {
    txn_man->abort();
    txn_man->reset();
    txn_man->reset_query();
    msg_queue.enqueue(read_thd_id(),Msg::create_message(txn_man,WKRACK_FIN),GET_NODE_ID(message->read_txn_id()));
    return Abort;
  } 
  txn_man->commit();

  if(!((FinishMessage*)message)->readonly || ALGO == OCCTEMPLATE)
    msg_queue.enqueue(read_thd_id(),Msg::create_message(txn_man,WKRACK_FIN),GET_NODE_ID(message->read_txn_id()));
  release_txn_manager();

  return RCOK;
}

RC TaskThread::rack_prep_process(Msg * message) {
  DEBUG("RPREP_ACK %ld\n",message->read_txn_id());

  RC rc = RCOK;

  int responses_left = txn_man->received_rps(((AckMessage*)message)->rc);
  assert(responses_left >=0);
#if ALGO == OCCTEMPLATE
  // Integrate bounds
  uint64_t lower = ((AckMessage*)message)->lower;
  uint64_t upper = ((AckMessage*)message)->upper;
  if(lower > txn_timestamp_bounds.get_lower(read_thd_id(),message->read_txn_id())) {
	txn_timestamp_bounds.set_lower(read_thd_id(),message->read_txn_id(),lower);
  }
  if(upper < txn_timestamp_bounds.get_upper(read_thd_id(),message->read_txn_id())) {
	txn_timestamp_bounds.set_upper(read_thd_id(),message->read_txn_id(),upper);
  }
  DEBUG("%ld bound set: [%ld,%ld] -> [%ld,%ld]\n",message->read_txn_id(),lower,upper,txn_timestamp_bounds.get_lower(read_thd_id(),message->read_txn_id()),txn_timestamp_bounds.get_upper(read_thd_id(),message->read_txn_id()));
  if(((AckMessage*)message)->rc != RCOK) {
	txn_timestamp_bounds.set_state(read_thd_id(),message->read_txn_id(),OCCTEMPLATE_ABORTED);
  }
#endif
  if(responses_left > 0) 
	return WAIT;

  // Done waiting 
  if(txn_man->get_rc() == RCOK) {
	rc  = txn_man->validate();
  }
  if(rc == Abort || txn_man->get_rc() == Abort) {
	txn_man->txn->rc = Abort;
	rc = Abort;
  }
  txn_man->send_finish_msg();
  if(rc == Abort) {
	txn_man->abort();
  } else {
	txn_man->commit();
  }

  return rc;
}

RC TaskThread::rack_rfin_process(Msg * message) {
  DEBUG("RFIN_ACK %ld\n",message->read_txn_id());

  RC rc = RCOK;

  int responses_left = txn_man->received_rps(((AckMessage*)message)->rc);
  assert(responses_left >=0);
  if(responses_left > 0) 
	return WAIT;

  // Done waiting 
  txn_man->txn_stats.two_pc_time += acquire_ts() - txn_man->txn_stats.wait_begintime;

  if(txn_man->get_rc() == RCOK) {
	//txn_man->commit();
	commit();
  } else {
	//txn_man->abort();
	abort();
  }
  return rc;
}

RC TaskThread::rqry_rsp_process(Msg * message) {
  DEBUG("WKRQRY_RSP %ld\n",message->read_txn_id());
  assert(IS_LOCAL(message->read_txn_id()));

  txn_man->txn_stats.remote_wait_time += acquire_ts() - txn_man->txn_stats.wait_begintime;

  if(((QueryResponseMessage*)message)->rc == Abort) {
	txn_man->start_abort();
	return Abort;
  }

  RC rc = txn_man->run_txn();
  check_if_done(rc);
  return rc;

}

RC TaskThread::rqry_process(Msg * message) {
  DEBUG("WKRQRY %ld\n",message->read_txn_id());
  M_ASSERT_V(!IS_LOCAL(message->read_txn_id()),"WKRQRY local: %ld %ld/%d\n",message->read_txn_id(),message->read_txn_id()%g_cnt_node,g_node_id);
  assert(!IS_LOCAL(message->read_txn_id()));
  RC rc = RCOK;

  message->copy_to_txn(txn_man);

#if ALGO == OCCTEMPLATE
          txn_timestamp_bounds.init(read_thd_id(),txn_man->read_txn_id());
#endif

  rc = txn_man->run_txn();

  // Send response
  if(rc != WAIT) {
    msg_queue.enqueue(read_thd_id(),Msg::create_message(txn_man,WKRQRY_RSP),txn_man->return_id);
  }
  return rc;
}

RC TaskThread::rqry_cont_process(Msg * message) {
  DEBUG("WKRQRY_CONT %ld\n",message->read_txn_id());
  assert(!IS_LOCAL(message->read_txn_id()));
  RC rc = RCOK;

  txn_man->run_txn_post_wait();
  rc = txn_man->run_txn();

  // Send response
  if(rc != WAIT) {
    msg_queue.enqueue(read_thd_id(),Msg::create_message(txn_man,WKRQRY_RSP),txn_man->return_id);
  }
  return rc;
}


RC TaskThread::rtxn_cont_process(Msg * message) {
  DEBUG("WKRTXN_CONT %ld\n",message->read_txn_id());
  assert(IS_LOCAL(message->read_txn_id()));

  txn_man->txn_stats.local_wait_time += acquire_ts() - txn_man->txn_stats.wait_begintime;

  txn_man->run_txn_post_wait();
  RC rc = txn_man->run_txn();
  check_if_done(rc);
  return RCOK;
}

RC TaskThread::rprepare_process(Msg * message) {
  DEBUG("RPREP %ld\n",message->read_txn_id());
    RC rc = RCOK;

    // Validate transaction
    rc  = txn_man->validate();
    txn_man->set_rc(rc);
    msg_queue.enqueue(read_thd_id(),Msg::create_message(txn_man,WKRACK_PREP),message->return_node_id);
    // Clean up as soon as abort is possible
    if(rc == Abort) {
      txn_man->abort();
    }

    return rc;
}

uint64_t TaskThread::get_next_txn_id() {
  uint64_t txn_id = ( get_node_id() + read_thd_id() * g_cnt_node) 
							+ (g_thd_cnt * g_cnt_node * _thd_txn_id);
  ++_thd_txn_id;
  return txn_id;
}

RC TaskThread::rtxn_process(Msg * message) {
  RC rc = RCOK;
  uint64_t txn_id = UINT64_MAX;

  if(message->get_rtype() == WKCQRY) {
    // This is a new transaction

    // Only set new txn_id when txn first starts
    #if WORKLOAD == DA
    message->txn_id=((DAClientQueryMessage*)message)->trans_id;
    txn_id=((DAClientQueryMessage*)message)->trans_id;
    #else
    txn_id = get_next_txn_id();
    message->txn_id = txn_id;
    #endif

    // Put txn in txn_table
    txn_man = txn_table.get_transaction_manager(read_thd_id(),txn_id,0);
    txn_man->register_thread(this);
    uint64_t ready_starttime = acquire_ts();
    bool ready = txn_man->unset_ready();
    INC_STATS(read_thd_id(),worker_activate_txn_time,acquire_ts() - ready_starttime);
    assert(ready);

    txn_man->txn_stats.begintime = acquire_ts();
    txn_man->txn_stats.restart_begintime = txn_man->txn_stats.begintime;
    message->copy_to_txn(txn_man);
    DEBUG("START %ld %f %lu\n",txn_man->read_txn_id(),simulate_man->seconds_from_begin(acquire_ts()),txn_man->txn_stats.begintime);
  #if WORKLOAD==DA
    if(da_start_trans_tab.count(txn_man->read_txn_id())==0)
    {
      da_start_trans_tab.insert(txn_man->read_txn_id());
      INC_STATS(read_thd_id(),local_txn_start_cnt,1);
    }
  #else
    INC_STATS(read_thd_id(), local_txn_start_cnt, 1);
  #endif
  } else {
    txn_man->txn_stats.restart_begintime = acquire_ts();
    DEBUG("RESTART %ld %f %lu\n",txn_man->read_txn_id(),simulate_man->seconds_from_begin(acquire_ts()),txn_man->txn_stats.begintime);
  }
      // Get new timestamps
  if(is_cc_new_timestamp()) {
  #if WORKLOAD==DA //mvcc use timestamp
    if(da_stamp_tab.count(txn_man->read_txn_id())==0)
    {
      da_stamp_tab[txn_man->read_txn_id()]=get_next_ts();
      txn_man->set_timestamp(da_stamp_tab[txn_man->read_txn_id()]);
    }
  else
    txn_man->set_timestamp(da_stamp_tab[txn_man->read_txn_id()]);
  #else
    txn_man->set_timestamp(get_next_ts());
  #endif
  }
#if ALGO == OCCTEMPLATE
  #if WORKLOAD==DA
  if(da_start_stamp_tab.count(txn_man->read_txn_id())==0)
  {
    da_start_stamp_tab[txn_man->read_txn_id()]=1;
    txn_timestamp_bounds.init(read_thd_id(),txn_man->read_txn_id());
    assert(txn_timestamp_bounds.get_lower(read_thd_id(),txn_man->read_txn_id()) == 0);
    assert(txn_timestamp_bounds.get_upper(read_thd_id(),txn_man->read_txn_id()) == UINT64_MAX);
    assert(txn_timestamp_bounds.get_state(read_thd_id(),txn_man->read_txn_id()) == OCCTEMPLATE_RUNNING);
  }
  #else
  txn_timestamp_bounds.init(read_thd_id(),txn_man->read_txn_id());
  assert(txn_timestamp_bounds.get_lower(read_thd_id(),txn_man->read_txn_id()) == 0);
  assert(txn_timestamp_bounds.get_upper(read_thd_id(),txn_man->read_txn_id()) == UINT64_MAX);
  assert(txn_timestamp_bounds.get_state(read_thd_id(),txn_man->read_txn_id()) == OCCTEMPLATE_RUNNING);
  #endif
#endif

	rc = init_phase();
	if(rc != RCOK)
	  	return rc;
	#if WORKLOAD == DA
		printf("thd_id:%lu stxn_id:%lu batch_id:%lu seq_id:%lu type:%c rtype:%d trans_id:%lu item:%c laststate:%lu state:%lu next_state:%lu\n",
		this->_thd_id,
		((DAClientQueryMessage*)message)->txn_id,
		((DAClientQueryMessage*)message)->batch_id,
		((DAClientQueryMessage*)message)->seq_id,
		type2char(((DAClientQueryMessage*)message)->txn_type),
		((DAClientQueryMessage*)message)->rtype,
		((DAClientQueryMessage*)message)->trans_id,
		static_cast<char>('x'+((DAClientQueryMessage*)message)->item_id),
		((DAClientQueryMessage*)message)->last_state,
		((DAClientQueryMessage*)message)->state,
		((DAClientQueryMessage*)message)->next_state);
		fflush(stdout);
	#endif
	// Execute transaction
	if (WORKLOAD == TEST)
	  	rc = runTest(txn_man);
	else 
	  	rc = txn_man->run_txn();
  	check_if_done(rc);
	return rc;
}

RC TaskThread::runTest(TxnMgr * txn)
{
	RC rc = RCOK;
	if (g_test_case == READ_WRITE) {
		rc = ((TxnManCTEST *)txn)->run_txn(g_test_case, 0);
#if ALGO == OCC
		txn->start_ts = get_next_ts(); 
#endif
		rc = ((TxnManCTEST *)txn)->run_txn(g_test_case, 1);
		printf("READ_WRITE TEST PASSED\n");
		return FINISH;
	}
	else if (g_test_case == CONFLICT) {
		rc = ((TxnManCTEST *)txn)->run_txn(g_test_case, 0);
		if (rc == RCOK)
			return FINISH;
		else 
			return rc;
	}
	assert(false);
	return RCOK;
}

RC TaskThread::init_phase() {
  RC rc = RCOK;
  return rc;
}


RC TaskThread::log_msg_process(Msg * message) {
  assert(ISREPLICA);
  DEBUG("REPLICA PROCESS %ld\n",message->read_txn_id());
  RecordLog * record = logger.createRecord(&((LogMessage*)message)->record);
  logger.enqueueRecord(record);
  return RCOK;
}

RC TaskThread::log_msg_rsp_process(Msg * message) {
  DEBUG("REPLICA RSP %ld\n",message->read_txn_id());
  txn_man->repl_finished = true;
  if(txn_man->log_flushed)
    commit();
  return RCOK;
}

RC TaskThread::log_flushed_process(Msg * message) {
  DEBUG("LOG FLUSHED %ld\n",message->read_txn_id());
  if(ISREPLICA) {
    msg_queue.enqueue(read_thd_id(),Msg::create_message(message->txn_id,WKLOG_MSG_RSP),GET_NODE_ID(message->txn_id)); 
    return RCOK;
  }

  txn_man->log_flushed = true;
  if(g_cnt_repl == 0 || txn_man->repl_finished)
    commit();
  return RCOK; 
}

RC TaskThread::rfwd_process(Msg * message) {
  DEBUG("WKRFWD (%ld,%ld)\n",message->read_txn_id(),message->get_batch_id());
  txn_man->txn_stats.remote_wait_time += acquire_ts() - txn_man->txn_stats.wait_begintime;
  assert(ALGO == CALVIN);
  int responses_left = txn_man->received_rps(((ForwardMessage*)message)->rc);
  assert(responses_left >=0);
  if(txn_man->calvin_collect_phase_done()) {
	assert(ISSERVERN(txn_man->return_id));
	RC rc = txn_man->run_calvin_txn();
	if(rc == RCOK && txn_man->calvin_exec_phase_done()) {
	  calvin_wrapup();
	  return RCOK;
	}   
  }
  return WAIT;

}

RC TaskThread::calvin_rtxn_process(Msg * message) {

  DEBUG("START %ld %f %lu\n",txn_man->read_txn_id(),simulate_man->seconds_from_begin(acquire_ts()),txn_man->txn_stats.begintime);
  assert(ISSERVERN(txn_man->return_id));
  txn_man->txn_stats.local_wait_time += acquire_ts() - txn_man->txn_stats.wait_begintime;
  // Execute
  RC rc = txn_man->run_calvin_txn();
  //if((txn_man->phase==6 && rc == RCOK) || txn_man->active_cnt == 0 || txn_man->participant_cnt == 1) {
  if(rc == RCOK && txn_man->calvin_exec_phase_done()) {
	calvin_wrapup();
  }
  return RCOK;

}


bool TaskThread::is_cc_new_timestamp() {
  return true;
}

ts_t TaskThread::get_next_ts() {
	if (g_alloc_ts_batch) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = global_manager.get_ts(read_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = global_manager.get_ts(read_thd_id());
		return _curr_ts;
	}
}


