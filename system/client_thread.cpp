
#include "global.h"
#include "thread.h"
#include "client_thread.h"
#include "query.h"
#include "qry_ycsb.h"
#include "qry_tpcc.h"
#include "client_query.h"
#include "transport.h"
#include "client_txn.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "workload.h"
#include "message.h"

void ClientThread::setup() {
	if( _thd_id == 0) {
	send_init_done_to_nodes();
  }
#if LOAD_METHOD == LOAD_RATE
  assert(g_per_server_load > 0);
  // send ~twice as frequently due to delays in context switching
  send_interval = (g_cl_thd_cnt * BILLION) / g_per_server_load / 1.8;
  printf("Client interval: %ld\n",send_interval);
#endif

}

RC ClientThread::run() {

  tsetup();
  printf("Running ClientThread %ld\n",_thd_id);
  BaseQry * m_query;
	uint64_t iters = 0;
	uint32_t num_txns_sent = 0;
	int txns_sent[g_servers_per_client];
  for (uint32_t i = 0; i < g_servers_per_client; ++i)
      txns_sent[i] = 0;

	run_begintime = acquire_ts();

  while(!simulate_man->is_done()) {
    heartbeat();
#if SERVER_GENERATE_QUERIES
    break;
#endif
		//uint32_t next_node = iters++ % g_cnt_node;
    progress_stats();
    int32_t inf_cnt;
		uint32_t next_node = (((iters++) * g_cl_thd_cnt) + _thd_id )% g_servers_per_client;
		uint32_t next_node_id = next_node + g_server_start_node;
		// Just in case...
		if (iters == UINT64_MAX)
			iters = 0;
#if LOAD_METHOD == LOAD_MAX
#if WORKLOAD != DA
		if ((inf_cnt = client_man.inc_inflight(next_node)) < 0)
			continue;
#endif
		m_query = qry_queue_client.get_next_query(next_node,_thd_id);
    if(last_send_time > 0) {
      INC_STATS(read_thd_id(),cl_send_intv,acquire_ts() - last_send_time);
    }
    last_send_time = acquire_ts();
    simulate_man->last_da_query_time = acquire_ts();
#elif LOAD_METHOD == LOAD_RATE
		if ((inf_cnt = client_man.inc_inflight(next_node)) < 0)
			continue;
    uint64_t gate_time;
    while((gate_time = acquire_ts()) - last_send_time < send_interval) { }
    if(last_send_time > 0) {
      INC_STATS(read_thd_id(),cl_send_intv,gate_time - last_send_time);
    }
    last_send_time = gate_time;
		m_query = qry_queue_client.get_next_query(next_node,_thd_id);
#else
    assert(false);
#endif
    assert(m_query);

		DEBUG("Client: thread %lu sending query to node: %u, %d, %f\n",
				_thd_id, next_node_id,inf_cnt,simulate_man->seconds_from_begin(acquire_ts()));

    Msg * message = Msg::create_message((BaseQry*)m_query,WKCQRY);
    ((ClientQryMsg*)message)->client_start_ts = acquire_ts();
    msg_queue.enqueue(read_thd_id(),message,next_node_id);
		num_txns_sent++;
		txns_sent[next_node]++;
    INC_STATS(read_thd_id(),txn_sent_cnt,1);
		#if WORKLOAD==DA
			delete m_query;
		#endif
	}


	for (uint64_t l = 0; l < g_servers_per_client; ++l)
		printf("Txns sent to node %lu: %d\n", l+g_server_start_node, txns_sent[l]);

  //SET_STATS(read_thd_id(), total_runtime, acquire_ts() - simulate_man->run_begintime); 

  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
	return FINISH;
}
