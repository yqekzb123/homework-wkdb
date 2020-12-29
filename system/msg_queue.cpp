
#include "msg_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "pool.h"
#include "message.h"
#include <boost/lockfree/queue.hpp>

void MsgQueue::init() {
  //m_queue = new boost::lockfree::queue<entry_message* > (0);
  m_queue = new boost::lockfree::queue<entry_message* > * [g_send_thd_cnt_this];
#if NETWORK_DELAY_TEST
  cl_m_queue = new boost::lockfree::queue<entry_message* > * [g_send_thd_cnt_this];
#endif
  for(uint64_t i = 0; i < g_send_thd_cnt_this; i++) {
    m_queue[i] = new boost::lockfree::queue<entry_message* > (0);
#if NETWORK_DELAY_TEST
    cl_m_queue[i] = new boost::lockfree::queue<entry_message* > (0);
#endif
  }
  ctr = new  uint64_t * [g_send_thd_cnt_this];
  for(uint64_t i = 0; i < g_send_thd_cnt_this; i++) {
    ctr[i] = (uint64_t*) alloc_memory.align_alloc(sizeof(uint64_t));
    *ctr[i] = i % g_thd_cnt;
  }
  for(uint64_t i = 0; i < g_send_thd_cnt_this;i++)
    sthd_cache.push_back(NULL);
}

void MsgQueue::enqueue(uint64_t thd_id, Msg * message,uint64_t dest) {
  DEBUG("MQ Enqueue %ld\n",dest)
  assert(dest < g_node_total_cnt);
  assert(dest != g_node_id);
  DEBUG_M("MsgQueue::enqueue entry_message alloc\n");
  entry_message * entry = (entry_message*) alloc_memory.alloc(sizeof(struct entry_message));
  //message_pool.get(entry);
  entry->message = message;
  entry->dest = dest;
  entry->begintime = acquire_ts();
  assert(entry->dest < g_node_total_cnt);
  uint64_t mtx_time_begin = acquire_ts();
#if ALGO == CALVIN
  // Need to have strict message ordering for sequencer thread
  uint64_t rand = thd_id % g_send_thd_cnt_this;
#elif WORKLOAD == DA
  uint64_t rand = 0;
#else
  uint64_t rand = mtx_time_begin % g_send_thd_cnt_this;
#endif
#if NETWORK_DELAY_TEST
  if(ISCLIENTN(dest)) {
    while(!cl_m_queue[rand]->push(entry) && !simulate_man->is_done()) {}
    return;
  }
#endif
  while(!m_queue[rand]->push(entry) && !simulate_man->is_done()) {}
  INC_STATS(thd_id,mtx[3],acquire_ts() - mtx_time_begin);
  INC_STATS(thd_id,message_queue_cnt,1);


}

uint64_t MsgQueue::dequeue(uint64_t thd_id, Msg *& message) {
  entry_message * entry = NULL;
  uint64_t dest = UINT64_MAX;
  uint64_t mtx_time_begin = acquire_ts();
  bool valid = false;
#if NETWORK_DELAY_TEST
  valid = cl_m_queue[thd_id%g_send_thd_cnt_this]->pop(entry);
  if(!valid) {
    entry = sthd_cache[thd_id % g_send_thd_cnt_this];
    if(entry)
      valid = true;
    else
      valid = m_queue[thd_id%g_send_thd_cnt_this]->pop(entry);
  }
#elif WORKLOAD == DA
  valid = m_queue[0]->pop(entry);
#else
  //uint64_t ctr_id = thd_id % g_send_thd_cnt_this;
  //uint64_t start_ctr = *ctr[ctr_id];
  valid = m_queue[thd_id%g_send_thd_cnt_this]->pop(entry);
#endif
  /*
  while(!valid && !simulate_man->is_done()) {
    ++(*ctr[ctr_id]);
    if(*ctr[ctr_id] >= g_this_thread_cnt)
      *ctr[ctr_id] = 0;
    valid = m_queue[*ctr[ctr_id]]->pop(entry);
    if(*ctr[ctr_id] == start_ctr)
      break;
  }
  */
  INC_STATS(thd_id,mtx[4],acquire_ts() - mtx_time_begin);
  uint64_t curr_time = acquire_ts();
  if(valid) {
    assert(entry);
#if NETWORK_DELAY_TEST
    if(!ISCLIENTN(entry->dest)) {
      if(ISSERVER && (acquire_ts() - entry->begintime) < g_delay_net) {
        sthd_cache[thd_id%g_send_thd_cnt_this] = entry;
        INC_STATS(thd_id,mtx[5],acquire_ts() - curr_time);
        return UINT64_MAX;
      } else {
        sthd_cache[thd_id%g_send_thd_cnt_this] = NULL;
      }
      if(ISSERVER) {
        INC_STATS(thd_id,mtx[38],1);
        INC_STATS(thd_id,mtx[39],curr_time - entry->begintime);
      }
    }

#endif
    dest = entry->dest;
    assert(dest < g_node_total_cnt);
    message = entry->message;
    DEBUG("MsgQueue Dequeue %ld\n",dest)
    INC_STATS(thd_id,msg_queue_delay_time,curr_time - entry->begintime);
    INC_STATS(thd_id,msg_queue_cnt,1);
    message->mq_time = curr_time - entry->begintime;
    //message_pool.put(entry);
    DEBUG_M("MsgQueue::enqueue entry_message free\n");
    alloc_memory.free(entry,sizeof(struct entry_message));
  } else {
    message = NULL;
    dest = UINT64_MAX;
  }
  INC_STATS(thd_id,mtx[5],acquire_ts() - curr_time);
  return dest;
}
