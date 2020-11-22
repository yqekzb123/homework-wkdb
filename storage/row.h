
#ifndef _ROW_H_
#define _ROW_H_

#include <cassert>
#include "global.h"

#define DECL_SET_VALUE(type) \
	void set_value(int col_id, type value);

#define SET_VALUE(type) \
	void RowData::set_value(int col_id, type value) { \
		set_value(col_id, &value); \
	}

#define DECL_GET_VALUE(type)\
	void get_row_value(int col_id, type & value);

/*
#define GET_VALUE(type)\
	void RowData::get_row_value(int col_id, type & value) {\
    value = *(type *)data; \
	}
  */
#define GET_VALUE(type)\
	void RowData::get_row_value(int col_id, type & value) {\
		int pos = get_tbl_schema()->get_field_index_no(col_id);\
    DEBUG("get_row_value pos %d -- %lx\n",pos,(uint64_t)this); \
		value = *(type *)&data[pos];\
	}

class TableSchema;
class CatalogSchema;
class TxnMgr;
class Row_occ_template;
class Row_specex;

class RowData
{
public:

	RC init(TableSchema * host_table, uint64_t part_id, uint64_t row_id = 0);
	RC switch_schema(TableSchema * host_table);
	// not every row has a manager
	void init_manager(RowData * row);

	TableSchema * get_table();
	CatalogSchema * get_tbl_schema();
	const char * get_table_name();
	uint64_t get_field_cnt();
	uint64_t get_tuple_size();
	uint64_t get_row_id() { return _row_id; };

	void copy(RowData * src);

	void 		set_primary_key(uint64_t key) { _primary_key = key; };
	uint64_t 	get_primary_key() {return _primary_key; };
	uint64_t 	get_part_id() { return _part_id; };

	void set_value(int id, void * ptr);
	void set_value(int id, void * ptr, int size);
	void set_value(const char * col_name, void * ptr);
	char * get_row_value(int id);
	char * get_row_value(char * col_name);
	
	DECL_SET_VALUE(uint64_t);
	DECL_SET_VALUE(int64_t);
	DECL_SET_VALUE(double);
	DECL_SET_VALUE(UInt32);
	DECL_SET_VALUE(SInt32);

	DECL_GET_VALUE(uint64_t);
	DECL_GET_VALUE(int64_t);
	DECL_GET_VALUE(double);
	DECL_GET_VALUE(UInt32);
	DECL_GET_VALUE(SInt32);


	void set_data(char * data);
	char * get_data();

	void free_row();

	// for concurrency control. can be lock, time_stamp etc.
  	RC get_lock(access_t type, TxnMgr * txn); 
	RC get_row(access_t type, TxnMgr * txn, RowData *& row);
	RC get_row_post_wait(access_t type, TxnMgr * txn, RowData *& row); 
	void return_row(RC rc, access_t type, TxnMgr * txn, RowData * row);

  #if ALGO == OCCTEMPLATE 
  	Row_occ_template * manager;
  #elif ALGO == HSTORE_SPEC
  	Row_specex * manager;
  #elif ALGO == AVOID
    Row_avoid * manager;
  #endif
	char * data;
  int tuple_size;
	TableSchema * table;
private:
	// primary key should be calculated from the data stored in the row.
	uint64_t 		_primary_key;
	uint64_t		_part_id;
	bool part_info;
	uint64_t _row_id;
};

#endif
