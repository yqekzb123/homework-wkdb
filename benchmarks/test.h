#ifndef _TEST_H_
#define _TEST_H_

#include "workload.h"
#include "txn.h"
#include "global.h"

class WLCTEST : public WLSchema
{
public:
	RC init();
	RC init_table();
	RC init_schema(const char * schema_file);
	RC txn_get_manager(TxnMgr *& txn_manager);
	void summarize();
	void tick() { time = acquire_ts(); };
	INDEX * the_index;
	TableSchema * the_table;
private:
	uint64_t time;
};

class TxnManCTEST : public TxnMgr 
{
public:
	void init(uint64_t part_id, WLSchema * h_wl); 
	RC run_txn(int type, int access_num);
	RC run_txn() { assert(false); };
	RC run_txn_post_wait() { assert(false); };
	RC run_calvin_txn() { assert(false); };
	RC acquire_locks() { assert(false); };
	
private:
	RC testReadwrite(int access_num);
	RC testConflict(int access_num);
	
	WLCTEST * _wl;
};

#endif
