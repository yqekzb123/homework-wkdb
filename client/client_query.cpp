
#include "client_query.h"
#include "mem_alloc.h"
#include "workload.h"
#include "table.h"
#include "qry_ycsb.h"
#include "qry_tpcc.h"
#include "da_query.h"

/*************************************************/
//     class Qry_queue
/*************************************************/

typedef struct
{
	void* context;
	int thd_id;
}FUNC_ARGS;

void 
Qry_queue_client::init(WLSchema * h_wl) {
	_wl = h_wl;


#if SERVER_GENERATE_QUERIES
	if(ISCLIENT)
		return;
	size = g_thd_cnt;
#else
 	size = g_servers_per_client;
#endif
	query_cnt = new uint64_t * [size];
	for ( UInt32 id = 0; id < size; id ++) {
		std::vector<BaseQry*> new_queries(g_per_part_max_txn+4,NULL);
		queries.push_back(new_queries);
		query_cnt[id] = (uint64_t*)alloc_memory.align_alloc(sizeof(uint64_t));
	}
	next_tid = 0;

#if WORKLOAD == DA
	FUNC_ARGS *arg=(FUNC_ARGS*)alloc_memory.align_alloc(sizeof(FUNC_ARGS));
	arg->context=this;
	arg->thd_id=g_init_parallelism - 1;
	pthread_t  p_thds_main;
	pthread_create(&p_thds_main, NULL, initQueriesHelper, (void*)arg );
	pthread_detach(p_thds_main);
#else
	pthread_t * p_threads = new pthread_t[g_init_parallelism - 1];
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
		FUNC_ARGS *arg=(FUNC_ARGS*)alloc_memory.align_alloc(sizeof(FUNC_ARGS));
		arg->context=this;
		arg->thd_id=i;
	  	pthread_create(&p_threads[i], NULL, initQueriesHelper,  (void*)arg);
	}
	FUNC_ARGS *arg=(FUNC_ARGS*)alloc_memory.align_alloc(sizeof(FUNC_ARGS));
	arg->context=this;
	arg->thd_id=g_init_parallelism - 1;
	initQueriesHelper( (void*)arg);

	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
	  pthread_join(p_threads[i], NULL);
	}
#endif

}

void * 
Qry_queue_client::initQueriesHelper(void * args) {
  ((Qry_queue_client*)((FUNC_ARGS*)args)->context)->initQueriesParallel(((FUNC_ARGS*)args)->thd_id);
  return NULL;
}

void 
Qry_queue_client::initQueriesParallel(uint64_t thd_id) {
#if WORKLOAD != DA
	UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
  	uint64_t request_cnt;
	request_cnt = g_per_part_max_txn + 4;
	
	uint32_t final_request;
	if (tid == g_init_parallelism-1) {
		final_request = request_cnt;
	} else {
		final_request = request_cnt / g_init_parallelism * (tid+1);
	}
#endif

#if WORKLOAD == YCSB	
	QueryGenYCSB * gen = new QueryGenYCSB;
	gen->init();
#elif WORKLOAD == TPCC
	QueryGenTPCC * gen = new QueryGenTPCC;
#elif WORKLOAD == PPS
	PPSQueryGenerator * gen = new PPSQueryGenerator;
#elif WORKLOAD == TEST
	QueryGenTPCC * gen = new QueryGenTPCC;
#elif WORKLOAD == DA
	DAQueryGenerator  * gen = new DAQueryGenerator;
#endif
#if SERVER_GENERATE_QUERIES
  for ( UInt32 thread_id = 0; thread_id < g_thd_cnt; thread_id ++) {
	for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request; query_id ++) {
	  queries[thread_id][query_id] = gen->create_query(_wl,g_node_id);
	}
  }
#elif WORKLOAD == DA
  gen->create_query(_wl,thd_id);
#else
  for ( UInt32 server_id = 0; server_id < g_servers_per_client; server_id ++) {
	for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request; query_id ++) {
	  queries[server_id][query_id] = gen->create_query(_wl,server_id+g_server_start_node);
	}
  }
#endif

}

bool
Qry_queue_client::done() { 	
  	return false;
}

BaseQry * 
Qry_queue_client::get_next_query(uint64_t server_id,uint64_t thd_id) { 	
#if WORKLOAD == DA
  BaseQry * query;
  query=da_gen_qry_queue.pop_data();
  return query;
#else
	assert(server_id < size);
	uint64_t query_id = __sync_fetch_and_add(query_cnt[server_id], 1);
	if(query_id > g_per_part_max_txn) {
		__sync_bool_compare_and_swap(query_cnt[server_id],query_id+1,0);
		query_id = __sync_fetch_and_add(query_cnt[server_id], 1);
	}
	BaseQry * query = queries[server_id][query_id];
	return query;
#endif
}


