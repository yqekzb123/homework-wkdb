
#include "manager.h"
#include "row.h"
#include "txn.h"
#include "pthread.h"
//#include <jemallloc.h>

void Manager::init() {
	time_stamp = 1;
	last_ts_time_min = 0;
	min_ts = 0; 
	all_ts = (ts_t *) malloc(sizeof(ts_t) * (g_thd_cnt * g_cnt_node));
	_all_txns = new TxnMgr * [g_thd_cnt + g_rem_thd_cnt];
	for (UInt32 i = 0; i < g_thd_cnt + g_rem_thd_cnt; i++) {
		//all_ts[i] = 0;
		//all_ts[i] = UINT64_MAX;
		_all_txns[i] = NULL;
	}
	for (UInt32 i = 0; i < CNT_BUCKET; i++)
		pthread_mutex_init( &mutexes[i], NULL );
  for (UInt32 i = 0; i < g_thd_cnt * g_cnt_node; ++i)
      all_ts[i] = 0;
}

uint64_t 
Manager::get_ts(uint64_t thd_id) {
	if (g_alloc_ts_batch)
		assert(g_alloc_ts == TS_CAS);
	uint64_t time;
	uint64_t begintime = acquire_ts();
	switch(g_alloc_ts) {
	case TS_MUTEX :
		pthread_mutex_lock( &ts_mutex_t );
		time = ++time_stamp;
		pthread_mutex_unlock( &ts_mutex_t );
		break;
	case TS_CAS :
		if (g_alloc_ts_batch)
			time = ATOM_FETCH_ADD(time_stamp, g_ts_batch_num);
		else 
			time = ATOM_FETCH_ADD(time_stamp, 1);
		break;
	case TS_HW :
		assert(false);
		break;
	case TS_CLOCK :
		time = get_clock_wall() * (g_cnt_node + g_thd_cnt) + (g_node_id * g_thd_cnt + thd_id);
		break;
	default :
		assert(false);
	}
	INC_STATS(thd_id, ts_alloc_time, acquire_ts() - begintime);
	return time;
}

void Manager::set_txn_man(TxnMgr * txn) {
	int thd_id = txn->read_thd_id();
	_all_txns[thd_id] = txn;
}


uint64_t Manager::hash(RowData * row) {
	uint64_t addr = (uint64_t)row / MEM_ALLI;
    return (addr * 1103515247 + 12345) % CNT_BUCKET;
}
 
void Manager::lock_row(RowData * row) {
	int row_hash = hash(row);
  uint64_t mtx_time_begin = acquire_ts();
	pthread_mutex_lock( &mutexes[row_hash] );	
  INC_STATS(0,mtx[2],acquire_ts() - mtx_time_begin);
}

void Manager::release_row(RowData * row) {
	int row_hash = hash(row);
	pthread_mutex_unlock( &mutexes[row_hash] );
}
