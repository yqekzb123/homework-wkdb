
#include "row.h"
#include "txn.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "manager.h"
#include "universal.h"

void Row_mvcc::init() {
  	latch = (pthread_mutex_t *)mem_allocator.alloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(latch, NULL);
	row_version_len = 0;
}

RC Row_mvcc::clear_history(access_t type, ts_t ts) {
	RC rc = RCOK;
	return rc;
}


void Row_mvcc::insert_version(ts_t ts, row_t *row) {

}

RC Row_mvcc::access(access_t type, TxnMgr * txn, RowData * row) {
	RC rc = RCOK;
	return rc;
}

RC commit(access_t type, TxnMgr * txn, RowData * data) {
	RC rc = RCOK;
	return rc;
}

RC abort(access_t type, TxnMgr * txn){
	RC rc = RCOK;
	return rc;
}