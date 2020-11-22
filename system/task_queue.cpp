
#include "task_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "message.h"
#include "client_query.h"
#include <boost/lockfree/queue.hpp>

void TaskQueue::init() {

  last_sched_dq = NULL;
  sched_ptr = 0;
  seq_queue = new boost::lockfree::queue<task_quequ_start* > (0);
  task_queue = new boost::lockfree::queue<task_quequ_start* > (0);
  new_txn_queue = new boost::lockfree::queue<task_quequ_start* >(0);
  sched_queue = new boost::lockfree::queue<task_quequ_start* > * [g_cnt_node];
  for ( uint64_t i = 0; i < g_cnt_node; i++) {
    sched_queue[i] = new boost::lockfree::queue<task_quequ_start* > (0);
  }

}

void TaskQueue::sequencer_enqueue(uint64_t thd_id, Msg * message) {
  uint64_t begintime = acquire_ts();
  assert(message);
  DEBUG_M("SeqQueue::enqueue task_quequ_start alloc\n");
  task_quequ_start * entry = (task_quequ_start*)alloc_memory.align_alloc(sizeof(task_quequ_start));
  entry->message = message;
  entry->rtype = message->rtype;
  entry->txn_id = message->txn_id;
  entry->batch_id = message->batch_id;
  entry->begintime = acquire_ts();
  assert(ISSERVER);

  DEBUG("Seq Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
  while(!seq_queue->push(entry) && !simulate_man->is_done()) {}

  INC_STATS(thd_id,seq_queue_enqueue_time,acquire_ts() - begintime);
  INC_STATS(thd_id,seq_queue_enq_cnt,1);

}

Msg * TaskQueue::sequencer_dequeue(uint64_t thd_id) {
  uint64_t begintime = acquire_ts();
  assert(ISSERVER);
  Msg * message = NULL;
  task_quequ_start * entry = NULL;
  bool valid = seq_queue->pop(entry);
  
  if(valid) {
    message = entry->message;
    assert(message);
    DEBUG("Seq Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
    uint64_t queue_time = acquire_ts() - entry->begintime;
    INC_STATS(thd_id,seq_queue_wait_time,queue_time);
    INC_STATS(thd_id,seq_queue_cnt,1);
    //DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",message->txn_id,message->batch_id,message->return_node_id,queue_time,message->rtype,(uint64_t)message);
  DEBUG_M("SeqQueue::dequeue task_quequ_start free\n");
    alloc_memory.free(entry,sizeof(task_quequ_start));
    INC_STATS(thd_id,seq_queue_dequeue_time,acquire_ts() - begintime);
  }

  return message;

}

void TaskQueue::sched_enqueue(uint64_t thd_id, Msg * message) {
  assert(ALGO == CALVIN);
  assert(message);
  assert(ISSERVERN(message->return_node_id));
  uint64_t begintime = acquire_ts();

  DEBUG_M("TaskQueue::sched_enqueue task_quequ_start alloc\n");
  task_quequ_start * entry = (task_quequ_start*)alloc_memory.alloc(sizeof(task_quequ_start));
  entry->message = message;
  entry->rtype = message->rtype;
  entry->txn_id = message->txn_id;
  entry->batch_id = message->batch_id;
  entry->begintime = acquire_ts();

  DEBUG("Sched Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
  uint64_t mtx_time_begin = acquire_ts();
  while(!sched_queue[message->get_return_id()]->push(entry) && !simulate_man->is_done()) {}
  INC_STATS(thd_id,mtx[37],acquire_ts() - mtx_time_begin);

  INC_STATS(thd_id,sched_queue_enqueue_time,acquire_ts() - begintime);
  INC_STATS(thd_id,sched_queue_enq_cnt,1);
}

Msg * TaskQueue::sched_dequeue(uint64_t thd_id) {
  uint64_t begintime = acquire_ts();

  assert(ALGO == CALVIN);
  Msg * message = NULL;
  task_quequ_start * entry = NULL;

  bool valid = sched_queue[sched_ptr]->pop(entry);

  if(valid) {

    message = entry->message;
    DEBUG("Sched Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);

    uint64_t queue_time = acquire_ts() - entry->begintime;
    INC_STATS(thd_id,sched_queue_wait_time,queue_time);
    INC_STATS(thd_id,sched_queue_cnt,1);

    DEBUG_M("TaskQueue::sched_enqueue task_quequ_start free\n");
    alloc_memory.free(entry,sizeof(task_quequ_start));

    if(message->rtype == WKRDONE) {
      // Advance to next queue or next epoch
      DEBUG("Sched WKRDONE %ld %ld\n",sched_ptr,simulate_man->get_worker_epoch());
      assert(message->get_batch_id() == simulate_man->get_worker_epoch());
      if(sched_ptr == g_cnt_node - 1) {
        INC_STATS(thd_id,sched_epoch_cnt,1);
        INC_STATS(thd_id,sched_epoch_diff,acquire_ts()-simulate_man->last_worker_epoch_time);
        simulate_man->next_worker_epoch();
      }
      sched_ptr = (sched_ptr + 1) % g_cnt_node;
      message->release();
      message = NULL;

    } else {
      simulate_man->inc_epoch_txn_cnt();
      DEBUG("Sched message dequeue %ld (%ld,%ld) %ld\n",sched_ptr,message->txn_id,message->batch_id,simulate_man->get_worker_epoch());
      assert(message->batch_id == simulate_man->get_worker_epoch());
    }

    INC_STATS(thd_id,sched_queue_dequeue_time,acquire_ts() - begintime);
  }


  return message;

}


void TaskQueue::enqueue(uint64_t thd_id, Msg * message,bool busy) {
  uint64_t begintime = acquire_ts();
  assert(message);
  DEBUG_M("TaskQueue::enqueue task_quequ_start alloc\n");
  task_quequ_start * entry = (task_quequ_start*)alloc_memory.align_alloc(sizeof(task_quequ_start));
  entry->message = message;
  entry->rtype = message->rtype;
  entry->txn_id = message->txn_id;
  entry->batch_id = message->batch_id;
  entry->begintime = acquire_ts();
  assert(ISSERVER || ISREPLICA);
  DEBUG("Work Enqueue (%ld,%ld) %d\n",entry->txn_id,entry->batch_id,entry->rtype);

  uint64_t mtx_start_write_time = acquire_ts();
  if(message->rtype == WKCQRY) {
    while(!new_txn_queue->push(entry) && !simulate_man->is_done()) {}
  } else {
    while(!task_queue->push(entry) && !simulate_man->is_done()) {}
  }
  INC_STATS(thd_id,mtx[13],acquire_ts() - mtx_start_write_time);

  if(busy) {
    INC_STATS(thd_id,task_queue_conflict_cnt,1);
  }
  INC_STATS(thd_id,task_queue_enqueue_time,acquire_ts() - begintime);
  INC_STATS(thd_id,task_queue_enq_cnt,1);
}

Msg * TaskQueue::dequeue(uint64_t thd_id) {
  uint64_t begintime = acquire_ts();
  assert(ISSERVER || ISREPLICA);
  Msg * message = NULL;
  task_quequ_start * entry = NULL;
  uint64_t mtx_start_write_time = acquire_ts();
  bool valid = task_queue->pop(entry);
  if(!valid) {
#if SERVER_GENERATE_QUERIES
    if(ISSERVER) {
      BaseQry * m_query = qry_queue_client.get_next_query(thd_id,thd_id);
      if(m_query) {
        assert(m_query);
        message = Msg::create_message((BaseQry*)m_query,WKCQRY);
      }
    }
#else
    valid = new_txn_queue->pop(entry);
#endif
  }
  INC_STATS(thd_id,mtx[14],acquire_ts() - mtx_start_write_time);
  
  if(valid) {
    message = entry->message;
    assert(message);
    //printf("%ld WQdequeue %ld\n",thd_id,entry->txn_id);
    uint64_t queue_time = acquire_ts() - entry->begintime;
    INC_STATS(thd_id,task_queue_wait_time,queue_time);
    INC_STATS(thd_id,task_queue_cnt,1);
    if(message->rtype == WKCQRY) {
      INC_STATS(thd_id,task_queue_new_wait_time,queue_time);
      INC_STATS(thd_id,task_queue_new_cnt,1);
    } else {
      INC_STATS(thd_id,task_queue_old_wait_time,queue_time);
      INC_STATS(thd_id,task_queue_old_cnt,1);
    }
    message->wq_time = queue_time;
    //DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",message->txn_id,message->batch_id,message->return_node_id,queue_time,message->rtype,(uint64_t)message);
    DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
    DEBUG_M("TaskQueue::dequeue task_quequ_start free\n");
    alloc_memory.free(entry,sizeof(task_quequ_start));
    INC_STATS(thd_id,task_queue_dequeue_time,acquire_ts() - begintime);
  }

#if SERVER_GENERATE_QUERIES
  if(message && message->rtype == WKCQRY) {
    INC_STATS(thd_id,task_queue_new_wait_time,acquire_ts() - begintime);
    INC_STATS(thd_id,task_queue_new_cnt,1);
  }
#endif
  return message;
}

