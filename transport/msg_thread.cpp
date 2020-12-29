
#include "msg_thread.h"
#include "msg_queue.h"
#include "message.h"
#include "mem_alloc.h"
#include "transport.h"
#include "query.h"
#include "qry_ycsb.h"
#include "qry_tpcc.h"
#include "pool.h"
#include "global.h"

void MsgThread::init(uint64_t thd_id) { 
  buffer_cnt = g_node_total_cnt;
#if ALGO == CALVIN
  buffer_cnt++;
#endif
  DEBUG_M("MsgThread::init buffer[] alloc\n");
  buffer = (mbuf **) alloc_memory.align_alloc(sizeof(mbuf*) * buffer_cnt);
  for(uint64_t n = 0; n < buffer_cnt; n++) {
    DEBUG_M("MsgThread::init mbuf alloc\n");
    buffer[n] = (mbuf *)alloc_memory.align_alloc(sizeof(mbuf));
    buffer[n]->init(n);
    buffer[n]->reset(n);
  }
  _thd_id = thd_id;
}

void MsgThread::check_and_send_batches() {
  uint64_t begintime = acquire_ts();
  for(uint64_t dest_node_id = 0; dest_node_id < buffer_cnt; dest_node_id++) {
    if(buffer[dest_node_id]->ready()) {
      send_batch(dest_node_id);
    }
  }
  INC_STATS(_thd_id,mtx[11],acquire_ts() - begintime);
}

void MsgThread::send_batch(uint64_t dest_node_id) {
  uint64_t begintime = acquire_ts();
    mbuf * sbuf = buffer[dest_node_id];
    assert(sbuf->cnt > 0);
	  ((uint32_t*)sbuf->buffer)[2] = sbuf->cnt;
    INC_STATS(_thd_id,mbuf_send_intv_time,acquire_ts() - sbuf->begintime);

    DEBUG("Send batch of %ld msgs to %ld\n",sbuf->cnt,dest_node_id);
    tport_manager.send_msg(_thd_id,dest_node_id,sbuf->buffer,sbuf->ptr);

    INC_STATS(_thd_id,msg_batch_size_msgs,sbuf->cnt);
    INC_STATS(_thd_id,msg_batch_size_bytes,sbuf->ptr);
    if(ISSERVERN(dest_node_id)) {
      INC_STATS(_thd_id,msg_batch_size_bytes_to_server,sbuf->ptr);
    } else if (ISCLIENTN(dest_node_id)){
      INC_STATS(_thd_id,msg_batch_size_bytes_to_client,sbuf->ptr);
    }
    INC_STATS(_thd_id,msg_batch_cnt,1);
    sbuf->reset(dest_node_id);
  INC_STATS(_thd_id,mtx[12],acquire_ts() - begintime);
}

char type2char1(DATxnType txn_type)
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

void MsgThread::run() {
  
  uint64_t begintime = acquire_ts();
  Msg * message = NULL;
  uint64_t dest_node_id;
  mbuf * sbuf;


  dest_node_id = msg_queue.dequeue(read_thd_id(), message);
  if(!message) {
    check_and_send_batches();
    INC_STATS(_thd_id,mtx[9],acquire_ts() - begintime);
    return;
  }
  assert(message);
  assert(dest_node_id < g_node_total_cnt);
  assert(dest_node_id != g_node_id);

  sbuf = buffer[dest_node_id];

  if(!sbuf->fits(message->get_size())) {
    assert(sbuf->cnt > 0);
    send_batch(dest_node_id);
  }
  #if WORKLOAD == DA
  if(!is_server&&true)
  printf("cl seq_id:%lu type:%c trans_id:%lu item:%c state:%lu next_state:%lu write_version:%lu\n",
      ((DAClientQueryMessage*)message)->seq_id,
      type2char1(((DAClientQueryMessage*)message)->txn_type),
      ((DAClientQueryMessage*)message)->trans_id,
      static_cast<char>('x'+((DAClientQueryMessage*)message)->item_id),
      ((DAClientQueryMessage*)message)->state,
      (((DAClientQueryMessage*)message)->next_state),
      ((DAClientQueryMessage*)message)->write_version);
      fflush(stdout);
  #endif
  
  uint64_t copy_starttime = acquire_ts();
  message->copy_to_buf(&(sbuf->buffer[sbuf->ptr]));
  INC_STATS(_thd_id,msg_copy_output_time,acquire_ts() - copy_starttime);
  DEBUG("%ld Buffered Msg %d, (%ld,%ld) to %ld\n",_thd_id,message->rtype,message->txn_id,message->batch_id,dest_node_id);
  sbuf->cnt += 1;
  sbuf->ptr += message->get_size();
  // Free message here, no longer needed unless CALVIN sequencer
  if(ALGO != CALVIN) {
    Msg::release_message(message);
  }
  if(sbuf->begintime == 0)
    sbuf->begintime = acquire_ts();

  check_and_send_batches();
  INC_STATS(_thd_id,mtx[10],acquire_ts() - begintime);

}

