
#include "global.h"
#include "universal.h"
#include "manager.h"
#include "thread.h"
#include "io_thread.h"
#include "query.h"
#include "qry_ycsb.h"
#include "qry_tpcc.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "message.h"
#include "client_txn.h"
#include "task_queue.h"

void InputThread::setup() {

  std::vector<Msg*> * msgs;
  while(!simulate_man->is_setup_done()) {
    msgs = tport_manager.recv_msg(read_thd_id());
    if(msgs == NULL)
      continue;
    while(!msgs->empty()) {
      Msg * message = msgs->front();
      if(message->rtype == WKINIT_DONE) {
        printf("Received WKINIT_DONE from node %ld\n",message->return_node_id);
        fflush(stdout);
        simulate_man->process_setup_msg();
      } else {
        assert(ISSERVER || ISREPLICA);
        //printf("Received Msg %d from node %ld\n",message->rtype,message->return_node_id);
#if ALGO == CALVIN
      if(message->rtype == WKACK_CALVIN ||(message->rtype == WKCQRY && ISCLIENTN(message->get_return_id()))) {
        task_queue.sequencer_enqueue(read_thd_id(),message);
        msgs->erase(msgs->begin());
        continue;
      }
      if( message->rtype == WKRDONE || message->rtype == WKCQRY) {
        assert(ISSERVERN(message->get_return_id()));
        task_queue.sched_enqueue(read_thd_id(),message);
        msgs->erase(msgs->begin());
        continue;
      }
#endif
        task_queue.enqueue(read_thd_id(),message,false);
      }
      msgs->erase(msgs->begin());
    }
    delete msgs;
  }
}

RC InputThread::run() {
  tsetup();
  printf("Running InputThread %ld\n",_thd_id);

  if(ISCLIENT) {
    client_recv_loop();
  } else {
    server_recv_loop();
  }

  return FINISH;

}

RC InputThread::client_recv_loop() {
	int rsp_cnts[g_servers_per_client];
	memset(rsp_cnts, 0, g_servers_per_client * sizeof(int));

	run_begintime = acquire_ts();
  uint64_t return_node_offset;
  uint64_t inf;

  std::vector<Msg*> * msgs;

	while (!simulate_man->is_done()) {
    heartbeat();
    uint64_t begintime = acquire_ts();
		msgs = tport_manager.recv_msg(read_thd_id());
    INC_STATS(_thd_id,mtx[28], acquire_ts() - begintime);
    begintime = acquire_ts();
    //while((m_query = task_queue.get_next_query(read_thd_id())) != NULL) {
    //Msg * message = task_queue.dequeue();
    if(msgs == NULL)
      continue;
    while(!msgs->empty()) {
      Msg * message = msgs->front();
			assert(message->rtype == WKCL_RSP);
      return_node_offset = message->return_node_id - g_server_start_node;
      assert(return_node_offset < g_servers_per_client);
      rsp_cnts[return_node_offset]++;
      INC_STATS(read_thd_id(),txn_cnt,1);
      uint64_t timespan = acquire_ts() - ((ClientRspMessage*)message)->client_start_ts; 
      INC_STATS(read_thd_id(),txn_run_time, timespan);
      if (warmup_done) {
        INC_STATS_ARR(read_thd_id(),client_client_latency, timespan);
      }
      //INC_STATS_ARR(read_thd_id(),all_lat,timespan);
      inf = client_man.dec_inflight(return_node_offset);
      DEBUG("Recv %ld from %ld, %ld -- %f\n",((ClientRspMessage*)message)->txn_id,message->return_node_id,inf,float(timespan)/BILLION);
      assert(inf >=0);
      // delete message here
      msgs->erase(msgs->begin());
    }
    delete msgs;
    INC_STATS(_thd_id,mtx[29], acquire_ts() - begintime);

	}

  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

RC InputThread::server_recv_loop() {

	myrand rdm;
	rdm.init(read_thd_id());
	RC rc = RCOK;
	assert (rc == RCOK);
  uint64_t begintime;

  std::vector<Msg*> * msgs;
	while (!simulate_man->is_done()) {
    heartbeat();
    begintime = acquire_ts();

		msgs = tport_manager.recv_msg(read_thd_id());

    INC_STATS(_thd_id,mtx[28], acquire_ts() - begintime);
    begintime = acquire_ts();

    if(msgs == NULL)
      continue;
    while(!msgs->empty()) {
      Msg * message = msgs->front();
      if(message->rtype == WKINIT_DONE) {
        msgs->erase(msgs->begin());
        continue;
      }
#if ALGO == CALVIN
      if(message->rtype == WKACK_CALVIN ||(message->rtype == WKCQRY && ISCLIENTN(message->get_return_id()))) {
        task_queue.sequencer_enqueue(read_thd_id(),message);
        msgs->erase(msgs->begin());
        continue;
      }
      if( message->rtype == WKRDONE || message->rtype == WKCQRY) {
        assert(ISSERVERN(message->get_return_id()));
        task_queue.sched_enqueue(read_thd_id(),message);
        msgs->erase(msgs->begin());
        continue;
      }
#endif
      task_queue.enqueue(read_thd_id(),message,false);
      msgs->erase(msgs->begin());
    }
    delete msgs;
    INC_STATS(_thd_id,mtx[29], acquire_ts() - begintime);

	}
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

void OutputThread::setup() {
  DEBUG_M("OutputThread::setup MsgThread alloc\n");
  messager = (MsgThread *) alloc_memory.alloc(sizeof(MsgThread));
  messager->init(_thd_id);
	while (!simulate_man->is_setup_done()) {
    messager->run();
  }
}

RC OutputThread::run() {

  tsetup();
  printf("Running OutputThread %ld\n",_thd_id);

	while (!simulate_man->is_done()) {
    heartbeat();
    messager->run();
  }

  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}


