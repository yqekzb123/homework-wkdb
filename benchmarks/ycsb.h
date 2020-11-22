
#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "workload.h"
#include "txn.h"
#include "global.h"
#include "universal.h"

class QryYCSB;
class QueryMsgYCSB;
class rqst_ycsb;

enum YCSBRemTxnType {
  YCSB_0,
  YCSB_1,
  YCSB_FIN,
  YCSB_RDONE
};

class WLYCSB : public WLSchema {
public :
	RC init();
	RC init_table();
	RC init_schema(const char * schema_file);
	RC txn_get_manager(TxnMgr *& txn_manager);
	int key_to_part(uint64_t key);
	INDEX * the_index;
	TableSchema * the_table;
private:
	void init_table_parallel();
	void * init_table_slice();
	static void * threadInitTable(void * This) {
		((WLYCSB *)This)->init_table_slice(); 
		return NULL;
	}
	pthread_mutex_t insert_lock;
	//  For parallel initialization
	static int next_tid;
};

class TxnManYCSB : public TxnMgr
{
public:
	void init(uint64_t thd_id, WLSchema * h_wl);
	void reset();
	void partial_reset();
  RC acquire_locks(); 
	RC run_txn();
  RC run_txn_post_wait(); 
	RC run_calvin_txn();
  void copy_remote_requests(QueryMsgYCSB * message); 
private:
  void next_ycsb_state();
  RC run_txn_state();
  RC run_ycsb_0(rqst_ycsb * req,RowData *& row_local);
  RC run_ycsb_1(access_t acctype, RowData * row_local);
  RC run_ycsb();
  bool is_done() ;
  bool is_local_request(uint64_t idx) ;


  RowData * row;
	WLYCSB * _wl;
	YCSBRemTxnType state;
  uint64_t next_record_id;
};

#endif
