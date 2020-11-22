
#ifndef _MANAGER_H_
#define _MANAGER_H_

#include "universal.h"
#include "global.h"

class RowData;
class TxnMgr;

class Manager {
public:
	void 			init();
	// returns the next time_stamp.
	ts_t			get_ts(uint64_t thd_id);

	// HACK! the following mutexes are used to model a centralized
	// lock/time_stamp manager. 
 	void 			lock_row(RowData * row);
	void 			release_row(RowData * row);
	
	TxnMgr * 		txn_get_manager(int thd_id) { return _all_txns[thd_id]; };
	void 			set_txn_man(TxnMgr * txn);
private:
	pthread_mutex_t ts_mutex_t;
	uint64_t 		time_stamp;
	pthread_mutex_t mutexes[CNT_BUCKET];
	uint64_t 		hash(RowData * row);
	ts_t * volatile all_ts;
	TxnMgr ** 		_all_txns;
	ts_t			last_ts_time_min;
	ts_t			min_ts;
};

#endif
