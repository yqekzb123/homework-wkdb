
#include "mem_alloc.h"
#include "abort_txn_queue.h"
#include "message.h"
#include "task_queue.h"

void AbortTxnQueue::init() {
  pthread_mutex_init(&mtx,NULL);
}

uint64_t AbortTxnQueue::enqueue(uint64_t thd_id, uint64_t txn_id, uint64_t abort_cnt) {
  uint64_t begintime = acquire_ts();
  uint64_t penalty = g_penalty_abort;
#if BACKOFF
  penalty = max(penalty * 2^abort_cnt,g_abort_penalty_max);
#endif
  penalty += begintime;
  //abort_txn_entry * entry = new abort_txn_entry(penalty,txn_id);
  DEBUG_M("AbortTxnQueue::enqueue entry alloc\n");
  abort_txn_entry * entry = (abort_txn_entry*)alloc_memory.alloc(sizeof(abort_txn_entry));
  entry->penalty_end = penalty;
  entry->txn_id = txn_id;
  uint64_t mtx_time_begin = acquire_ts();
  pthread_mutex_lock(&mtx);
  INC_STATS(thd_id,mtx[0],acquire_ts() - mtx_time_begin);
  DEBUG("AQ Enqueue %ld %f -- %f\n",entry->txn_id,float(penalty - begintime)/BILLION,simulate_man->seconds_from_begin(begintime));
  INC_STATS(thd_id,abort_queue_penalty,penalty - begintime);
  INC_STATS(thd_id,abort_queue_enqueue_cnt,1);
  queue.push(entry);
  pthread_mutex_unlock(&mtx);
  
  INC_STATS(thd_id,abort_queue_enqueue_time,acquire_ts() - begintime);

  return penalty - begintime;
}

void AbortTxnQueue::process(uint64_t thd_id) {
  if(queue.empty())
    return;
  abort_txn_entry * entry;
  uint64_t mtx_time_begin = acquire_ts();
  pthread_mutex_lock(&mtx);
  INC_STATS(thd_id,mtx[1],acquire_ts() - mtx_time_begin);
  uint64_t begintime = acquire_ts();
  while(!queue.empty()) {
    entry = queue.top();
    if(entry->penalty_end < begintime) {
      queue.pop();
      DEBUG("AQ Dequeue %ld %f -- %f\n",entry->txn_id,float(begintime - entry->penalty_end)/BILLION,simulate_man->seconds_from_begin(begintime));
      INC_STATS(thd_id,abort_queue_penalty_extra,begintime - entry->penalty_end);
      INC_STATS(thd_id,abort_queue_dequeue_cnt,1);
      Msg * message = Msg::create_message(WKRTXN);
      message->txn_id = entry->txn_id;
      task_queue.enqueue(thd_id,message,false);
      //entry = queue.top();
      DEBUG_M("AbortTxnQueue::dequeue entry free\n");
      alloc_memory.free(entry,sizeof(abort_txn_entry));
    } else {
      break;
    }

  }
  pthread_mutex_unlock(&mtx);

  INC_STATS(thd_id,abort_queue_dequeue_time,acquire_ts() - begintime);

}

