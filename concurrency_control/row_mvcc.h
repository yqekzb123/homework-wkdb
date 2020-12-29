
#ifndef ROW_MVCC_H
#define ROW_MVCC_H

typedef uint64_t ts_t; // used for timestamp

// corresponding versions of a record, using a linked list.
struct RowVersionEntry {
	ts_t ts;	// commit timestamp
	RowData *version_data;	// data
	RowVersionEntry * next;	// pointer to the next version according to version chain
	RowVersionEntry * prev;  // pointer to the previous version
};

class Row_mvcc {
public:
	void init(); // init function
	/*
	 * access() function:
	 * To read or write a particular version of this record, 
	 * as well as do concurrency control.
	 */
	RC access(access_t type, TxnMgr * txn, RowData * row);
	/*
	 * commit() function:
	 * To be called when a transaction is committing.
	 * It may include two parts:
	 * 1. Call insert_version() to write a new version, which can also be called in access()
	 *    instead.
	 * 2. Call clear_version_list() to cleanup outdated version.
	 */
	RC commit(access_t type, TxnMgr * txn, RowData * data);
	/*
	 * abort() function:
	 * To be called when a transaction is aborting.
	 * It may include two parts:
	 * 1. Cleanup the intermediate version that is produced when processing a transaction.
	 * 2. Call clear_version_list() to cleanup outdated version.
	 */
	RC abort(access_t type, TxnMgr * txn);

private:
 	pthread_mutex_t * latch;
	/*
	 * insert_version() function:
	 * To insert a new version.
	 */
	void insert_version(ts_t ts, RowData * row);
	/*
	 * clear_version_list() function:
	 * To clean up outdated versions in the version chain.
	 */
	RC clear_version_list(access_t type, ts_t ts);

    RowVersionEntry * row_version_list;	// version chain
	uint64_t row_version_len;		// length of version chain
	
};

#endif
