
#include "query.h"
#include "mem_alloc.h"
#include "workload.h"
#include "table.h"
#include "qry_ycsb.h"
#include "qry_tpcc.h"
#include "da_query.h"

/*************************************************/
//     class Qry_queue
/*************************************************/

void 
Qry_queue::init(WLSchema * h_wl) {
	all_queries = new Qry_thd * [g_thd_cnt];
	_wl = h_wl;
	for (UInt32 tid = 0; tid < g_thd_cnt; tid ++)
		init(tid);
}

void 
Qry_queue::init(int thd_id) {	
	all_queries[thd_id] = (Qry_thd *) alloc_memory.alloc(sizeof(Qry_thd));
	all_queries[thd_id]->init(_wl, thd_id);
}

BaseQry * 
Qry_queue::get_next_query(uint64_t thd_id) { 	
	BaseQry * query = all_queries[thd_id]->get_next_query();
	return query;
}

void 
Qry_thd::init(WLSchema * h_wl, int thd_id) {
	uint64_t request_cnt;
	q_idx = 0;
	request_cnt = WARMUP / g_thd_cnt + MAX_TXN_PER_PART + 4;
#if WORKLOAD == YCSB	
	queries = (QryYCSB *) 
		alloc_memory.alloc(sizeof(QryYCSB) * request_cnt);
#elif WORKLOAD == TPCC
	queries = (QryTPCC *) 
		alloc_memory.alloc(sizeof(QryTPCC) * request_cnt);
#elif WORKLOAD == PPS
	queries = (PPSQry *) 
		alloc_memory.alloc(sizeof(PPSQry) * request_cnt);
#elif WORKLOAD == TEST
	queries = (QryTPCC *) 
		alloc_memory.alloc(sizeof(QryTPCC) * request_cnt);
#endif
	for (UInt32 qid = 0; qid < request_cnt; qid ++) {
#if WORKLOAD == YCSB	
		new(&queries[qid]) QryYCSB();
#elif WORKLOAD == TPCC
		new(&queries[qid]) QryTPCC();
#elif WORKLOAD == PPS
		new(&queries[qid]) PPSQry();
#elif WORKLOAD == TEST
		new(&queries[qid]) QryTPCC();
#elif WORKLOAD == DA
		new(&queries[qid]) DAQuery();
#endif
		queries[qid].init(thd_id, h_wl);
	}


}

BaseQry * 
Qry_thd::get_next_query() {
	BaseQry * query = &queries[q_idx++];
	return query;
}

void BaseQry::init() { 
  DEBUG_M("BaseQry::init array parts_assign\n");
  parts_assign.init(g_cnt_part);
  DEBUG_M("BaseQry::init array parts_touched\n");
  parts_touched.init(g_cnt_part);
  DEBUG_M("BaseQry::init array nodes_active\n");
  nodes_active.init(g_cnt_node);
  DEBUG_M("BaseQry::init array related_nodes\n");
  related_nodes.init(g_cnt_node);
}

void BaseQry::clear() { 
  parts_assign.clear();
  parts_touched.clear();
  nodes_active.clear();
  related_nodes.clear();
} 

void BaseQry::release() { 
  parts_assign.release();
  parts_touched.release();
  nodes_active.release();
  related_nodes.release();
} 
