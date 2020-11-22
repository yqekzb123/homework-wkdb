
#ifndef _WORKLOAD_H_
#define _WORKLOAD_H_

#include "global.h"

class RowData;
class TableSchema;
class IndexHash;
class IndexBTree;
class CatalogSchema;
class lock_man;
class TxnMgr;
class Thread;
class IndexBase;
class Timestamp;
class Mvcc;

class WLSchema
{
public:
//	TableSchema * table;
	// tables indexed by table name
  map<string, TableSchema *> tables;
  map<string, INDEX *> indexes;

  void index_delete_all(); 
	
	// FOR TPCC
/*	*/
	// initialize the tables and indexes.
	virtual RC init();
	virtual RC init_schema(const char * schema_file);
	virtual RC init_table()=0;
	virtual RC txn_get_manager(TxnMgr *& txn_manager)=0;
	// get the global time_stamp.
//	uint64_t get_ts(uint64_t thd_id);
	//uint64_t cur_txn_id;
  uint64_t done_cnt;
  uint64_t txn_cnt;
protected:
	void index_insert(string index_name, uint64_t key, RowData * row);
	void index_insert(INDEX * index, uint64_t key, RowData * row, int64_t part_id = -1);
	void index_insert_non_unique(INDEX * index, uint64_t key, RowData * row, int64_t part_id = -1);
};

#endif
