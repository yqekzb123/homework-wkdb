
#include "global.h"
#include "sequencer.h"
#include "qry_ycsb.h"
#include "qry_tpcc.h"
#include "da_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "workload.h"
#include "universal.h"
#include "msg_queue.h"
#include "msg_thread.h"
#include "task_queue.h"
#include "message.h" 
#include "stats.h"
#include <boost/lockfree/queue.hpp>

void Sequencer::init(WLSchema * wl) {
  next_txn_id = 0;
  response_cnt = g_cnt_node + g_cl_node_cnt;
  _wl = wl;
  last_batch = 0;
  workload_head = NULL;
  workload_tail = NULL;
  fill_queue = new boost::lockfree::queue<Msg*, boost::lockfree::capacity<65526> > [g_cnt_node];
}

// Assumes 1 thread does sequencer work
void Sequencer::process_ack(Msg * message, uint64_t thd_id) {
  qlite_ll * en = workload_head;
  while(en != NULL && en->epoch != message->get_batch_id()) {
    en = en->next;
  }
  assert(en);
  qlite * wait_txn_list = en->list;
  assert(wait_txn_list != NULL);
  assert(en->left_txns > 0);

  uint64_t id = message->read_txn_id() / g_cnt_node;
  uint64_t prof_stat = acquire_ts();
  assert(wait_txn_list[id].server_ack_cnt > 0);

  // Decrement the number of acks needed for this txn
  uint32_t qry_left_acks = ATOM_SUB_FETCH(wait_txn_list[id].server_ack_cnt, 1);

  if (wait_txn_list[id].skew_start_ts == 0) {
      wait_txn_list[id].skew_start_ts = acquire_ts();
  }

  if (qry_left_acks == 0) {
      en->left_txns--;
      ATOM_FETCH_ADD(total_finished_txns,1);
      INC_STATS(thd_id,seq_txn_cnt,1);
      // free message, queries
#if WORKLOAD == YCSB
      QueryMsgYCSBcl* cl_msg = (QueryMsgYCSBcl*)wait_txn_list[id].message;
      for(uint64_t i = 0; i < cl_msg->requests.size(); i++) {
          DEBUG_M("Sequencer::process_ack() rqst_ycsb free\n");
          alloc_memory.free(cl_msg->requests[i],sizeof(rqst_ycsb));
      }
#elif WORKLOAD == TPCC
      QueryMsgClientTPCC* cl_msg = (QueryMsgClientTPCC*)wait_txn_list[id].message;
      if(cl_msg->txn_type == TPCC_NEW_ORDER) {
          for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
              DEBUG_M("Sequencer::process_ack() items free\n");
              alloc_memory.free(cl_msg->items[i],sizeof(Item_no));
          }
      }
#elif WORKLOAD == TEST
      QueryMsgClientTPCC* cl_msg = (QueryMsgClientTPCC*)wait_txn_list[id].message;
      if(cl_msg->txn_type == TPCC_NEW_ORDER) {
          for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
              DEBUG_M("Sequencer::process_ack() items free\n");
              alloc_memory.free(cl_msg->items[i],sizeof(Item_no));
          }
      }
#elif WORKLOAD == PPS
      PPSClientQueryMessage* cl_msg = (PPSClientQueryMessage*)wait_txn_list[id].message;
#elif WORKLOAD == DA
			DAClientQueryMessage* cl_msg = (DAClientQueryMessage*)wait_txn_list[id].message;
#endif
#if WORKLOAD == PPS
      if ( WORKLOAD == PPS && ALGO == CALVIN && ((cl_msg->txn_type == PPS_GETPARTBYSUPPLIER) ||
              (cl_msg->txn_type == PPS_GETPARTBYPRODUCT) ||
              (cl_msg->txn_type == PPS_ORDERPRODUCT))
              && (cl_msg->recon || ((AckMessage*)message)->rc == Abort)) {
          int abort_cnt = wait_txn_list[id].abort_cnt;
          if (cl_msg->recon) {
              // Copy over part keys
              cl_msg->part_keys.copy( ((AckMessage*)message)->part_keys);
              DEBUG("Finished RECON (%ld,%ld)\n",message->read_txn_id(),message->get_batch_id());
          }
          else {
              uint64_t timespan = acquire_ts() - wait_txn_list[id].seq_start_ts;
              if (warmup_done) {
                INC_STATS_ARR(0,start_abort_commit_latency, timespan);
              }
              cl_msg->part_keys.clear();
              DEBUG("Aborted (%ld,%ld)\n",message->read_txn_id(),message->get_batch_id());
              INC_STATS(0,total_txn_abort_cnt,1);
              abort_cnt++;
          }

          cl_msg->return_node_id = wait_txn_list[id].client_id;
          wait_txn_list[id].total_batch_time += en->batch_time_send - wait_txn_list[id].seq_start_ts;
          // restart
          process_txn(cl_msg, thd_id, wait_txn_list[id].seq_start_ts_first, wait_txn_list[id].seq_start_ts, wait_txn_list[id].total_batch_time, abort_cnt);
      }
      else {
#endif
          uint64_t curr_clock = acquire_ts();
          uint64_t timespan = curr_clock - wait_txn_list[id].seq_start_ts_first;
          uint64_t timespan2 = curr_clock - wait_txn_list[id].seq_start_ts;
          uint64_t skew_timespan = acquire_ts() - wait_txn_list[id].skew_start_ts;
          wait_txn_list[id].total_batch_time += en->batch_time_send - wait_txn_list[id].seq_start_ts;
          if (warmup_done) {
            INC_STATS_ARR(0,first_start_commit_latency, timespan);
            INC_STATS_ARR(0,last_start_commit_latency, timespan2);
            INC_STATS_ARR(0,start_abort_commit_latency, timespan2);
          }
          if (wait_txn_list[id].abort_cnt > 0) {
              INC_STATS(0,unique_txn_abort_cnt,1);
          }

    INC_STATS(0,lat_l_loc_msg_queue_time,wait_txn_list[id].total_batch_time);
    INC_STATS(0,lat_l_loc_process_time,skew_timespan);

    INC_STATS(0,lat_short_task_queue_time,message->lat_task_queue_time);
    INC_STATS(0,lat_short_msg_queue_time,message->lat_msg_queue_time);
    INC_STATS(0,lat_short_cc_block_time,message->lat_cc_block_time);
    INC_STATS(0,lat_short_cc_time,message->lat_cc_time);
    INC_STATS(0,lat_short_process_time,message->lat_process_time);

    if (message->return_node_id != g_node_id) {
      /*
          if (message->lat_network_time/BILLION > 1.0) {
            printf("%ld %d %ld -> %d: %f %f\n",message->txn_id, message->rtype, message->return_node_id,g_node_id ,message->lat_network_time/BILLION, message->lat_other_time/BILLION);
          } 
          */
      INC_STATS(0,lat_short_network_time,message->lat_network_time);
    }
    INC_STATS(0,lat_short_batch_time,wait_txn_list[id].total_batch_time);

          PRINT_LATENCY("lat_l_seq %ld %ld %d %f %f %f\n"
                  , message->read_txn_id()
                  , message->get_batch_id()
                  , wait_txn_list[id].abort_cnt
                  , (double) timespan / BILLION
                  , (double) skew_timespan / BILLION
                  , (double) wait_txn_list[id].total_batch_time / BILLION
                  );

          cl_msg->release();

          ClientRspMessage * rsp_msg = (ClientRspMessage*)Msg::create_message(message->read_txn_id(),WKCL_RSP);
          rsp_msg->client_start_ts = wait_txn_list[id].client_start_ts;
          msg_queue.enqueue(thd_id,rsp_msg,wait_txn_list[id].client_id);
#if WORKLOAD == PPS
      }
#endif

      INC_STATS(thd_id,seq_complete_cnt,1);

  }

  // If we have all acks for this batch, send qry responses to all clients
  if (en->left_txns == 0) {
      DEBUG("FINISHED BATCH %ld\n",en->epoch);
      LIST_REMOVE_HT(en,workload_head,workload_tail);
      alloc_memory.free(en->list,sizeof(qlite) * en->max_size);
      alloc_memory.free(en,sizeof(qlite_ll));

  }
  INC_STATS(thd_id,seq_ack_time,acquire_ts() - prof_stat);
}

// Assumes 1 thread does sequencer work
void Sequencer::process_txn( Msg * message,uint64_t thd_id, uint64_t early_start, uint64_t last_start, uint64_t wait_time, uint32_t abort_cnt) {

    uint64_t begintime = acquire_ts();
    DEBUG("SEQ Processing message\n");
    qlite_ll * en = workload_tail;

    // LL is potentially a bottleneck here
    if(!en || en->epoch != simulate_man->get_seq_epoch()+1) {
      DEBUG("SEQ new wait list for epoch %ld\n",simulate_man->get_seq_epoch()+1);
      // First txn of new wait list
      en = (qlite_ll *) alloc_memory.alloc(sizeof(qlite_ll));
      en->epoch = simulate_man->get_seq_epoch()+1;
      en->max_size = 1000;
      en->size = 0;
      en->left_txns = 0;
      en->list = (qlite *) alloc_memory.alloc(sizeof(qlite) * en->max_size);
      LIST_PUT_TAIL(workload_head,workload_tail,en)
    }
    if(en->size == en->max_size) {
      en->max_size *= 2;
      en->list = (qlite *) alloc_memory.realloc(en->list,sizeof(qlite) * en->max_size);
    }

    txnid_t txn_id = g_node_id + g_cnt_node * next_txn_id;
    next_txn_id++;
    uint64_t id = txn_id / g_cnt_node;
    message->batch_id = en->epoch;
    message->txn_id = txn_id;
    assert(txn_id != UINT64_MAX);

#if WORKLOAD == YCSB
    std::set<uint64_t> participants = QryYCSB::participants(message,_wl);
#elif WORKLOAD == TPCC
    std::set<uint64_t> participants = QryTPCC::participants(message,_wl);
#elif WORKLOAD == PPS
    std::set<uint64_t> participants = PPSQry::participants(message,_wl);
#elif WORKLOAD == TEST
    std::set<uint64_t> participants = QryTPCC::participants(message,_wl);
#elif WORKLOAD == DA
		std::set<uint64_t> participants = DAQuery::participants(message,_wl);
#endif
    uint32_t server_ack_cnt = participants.size();
    assert(server_ack_cnt > 0);
    assert(ISCLIENTN(message->get_return_id()));
    en->list[id].client_id = message->get_return_id();
    en->list[id].client_start_ts = ((ClientQryMsg*)message)->client_start_ts;
    //en->list[id].seq_start_ts = acquire_ts();

    en->list[id].total_batch_time = wait_time;
    en->list[id].abort_cnt = abort_cnt;
    en->list[id].skew_start_ts = 0;
    en->list[id].server_ack_cnt = server_ack_cnt;
    en->list[id].message = message;
    en->size++;
    en->left_txns++;
    // Note: Modifying message!
    message->return_node_id = g_node_id;
    message->lat_network_time = 0;
    message->lat_other_time = 0;
#if ALGO == CALVIN && WORKLOAD == PPS
    PPSClientQueryMessage* cl_msg = (PPSClientQueryMessage*) message;
    if (cl_msg->txn_type == PPS_GETPARTBYSUPPLIER ||
            cl_msg->txn_type == PPS_GETPARTBYPRODUCT ||
            cl_msg->txn_type == PPS_ORDERPRODUCT) {
        if (cl_msg->part_keys.size() == 0) {
            cl_msg->recon = true;
            en->list[id].seq_start_ts = acquire_ts();
        }
        else {
            cl_msg->recon = false;
            en->list[id].seq_start_ts = last_time;
        }

    }
    else {
        cl_msg->recon = false;
        en->list[id].seq_start_ts = acquire_ts();
    }
#else
    en->list[id].seq_start_ts = acquire_ts();
#endif
    if (early_start == 0) {
        en->list[id].seq_start_ts_first = en->list[id].seq_start_ts;
    } else {
        en->list[id].seq_start_ts_first = early_start;
    }
    assert(en->size == en->left_txns);
    assert(en->size <= ((uint64_t)g_max_inflight * g_cnt_node));

    // Add new txn to fill queue
    for(auto participant = participants.begin(); participant != participants.end(); participant++) {
      DEBUG("SEQ adding (%ld,%ld) to fill queue (recon: %d)\n",message->read_txn_id(),message->get_batch_id(),((PPSClientQueryMessage*)message)->recon);
      while(!fill_queue[*participant].push(message) && !simulate_man->is_done()) {}
    }

	INC_STATS(thd_id,seq_proc_cnt,1);
	INC_STATS(thd_id,seq_process_time,acquire_ts() - begintime);
	ATOM_ADD(total_received_txns,1);

}


// Assumes 1 thread does sequencer work
void Sequencer::send_batch_next(uint64_t thd_id) {
  uint64_t prof_stat = acquire_ts();
  qlite_ll * en = workload_tail;
  bool empty = true;
  if(en && en->epoch == simulate_man->get_seq_epoch()) {
    DEBUG("SEND NEXT BATCH %ld [%ld,%ld] %ld\n",thd_id,simulate_man->get_seq_epoch(),en->epoch,en->size);
    empty = false;

    en->batch_time_send = prof_stat;
  }

  Msg * message;
  for(uint64_t j = 0; j < g_cnt_node; j++) {
    while(fill_queue[j].pop(message)) {
      if(j == g_node_id) {
          task_queue.sched_enqueue(thd_id,message);
      } else {
        msg_queue.enqueue(thd_id,message,j);
      }
    }
    if(!empty) {
      DEBUG("Seq WKRDONE %ld\n",simulate_man->get_seq_epoch())
    }
    message = Msg::create_message(WKRDONE);
    message->batch_id = simulate_man->get_seq_epoch(); 
    if(j == g_node_id) {
      task_queue.sched_enqueue(thd_id,message);
    } else {
      msg_queue.enqueue(thd_id,message,j);
    }
  }

  if(last_batch > 0) {
    INC_STATS(thd_id,seq_batch_time,acquire_ts() - last_batch);
  }
  last_batch = acquire_ts();

	INC_STATS(thd_id,seq_cnt_batch,1);
  if(!empty) {
    INC_STATS(thd_id,seq_cnt_full_batch,1);
  }
  INC_STATS(thd_id,seq_prep_time,acquire_ts() - prof_stat);
  next_txn_id = 0;
}

