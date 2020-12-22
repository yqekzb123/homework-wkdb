
#include "global.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "txn.h"
#include "row_occ_template.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "manager.h"

#define SIM_FULL_ROW true

RC 
RowData::init(TableSchema * host_table, uint64_t part_id, uint64_t row_id) {
	part_info = true;
	_row_id = row_id;
	_part_id = part_id;
	this->table = host_table;
	CatalogSchema * schema = host_table->get_tbl_schema();
	tuple_size = schema->get_tuple_size();
#if SIM_FULL_ROW
	data = (char *) alloc_memory.alloc(sizeof(char) * tuple_size);
#else
	data = (char *) alloc_memory.alloc(sizeof(uint64_t) * 1);
#endif
	return RCOK;
}

RC 
RowData::switch_schema(TableSchema * host_table) {
	this->table = host_table;
	return RCOK;
}

void RowData::init_manager(RowData * row) {
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE
  return;
#endif
  DEBUG_M("RowData::init_manager alloc \n");
#if ALGO == OCCTEMPLATE 
    manager = (Row_occ_template *) alloc_memory.align_alloc(sizeof(Row_occ_template));
#endif

#if ALGO != HSTORE && ALGO != HSTORE_SPEC 
	manager->init(this);
#endif
}

TableSchema * RowData::get_table() { 
	return table; 
}

CatalogSchema * RowData::get_tbl_schema() { 
	return get_table()->get_tbl_schema(); 
}

const char * RowData::get_table_name() { 
	return get_table()->get_table_name(); 
};
uint64_t RowData::get_tuple_size() {
	return get_tbl_schema()->get_tuple_size();
}

uint64_t RowData::get_field_cnt() { 
	return get_tbl_schema()->field_cnt;
}

void RowData::set_value(int id, void * ptr) {
	int datasize = get_tbl_schema()->get_field_size(id);
	int pos = get_tbl_schema()->get_field_index_no(id);
  DEBUG("set_value pos %d datasize %d -- %lx\n",pos,datasize,(uint64_t)this);
#if SIM_FULL_ROW
	memcpy( &data[pos], ptr, datasize);
#else
  char d[tuple_size];
	memcpy( &d[pos], ptr, datasize);
#endif
}

void RowData::set_value(int id, void * ptr, int size) {
	int pos = get_tbl_schema()->get_field_index_no(id);
#if SIM_FULL_ROW
	memcpy( &data[pos], ptr, size);
#else
  char d[tuple_size];
	memcpy( &d[pos], ptr, size);
#endif
}

void RowData::set_value(const char * col_name, void * ptr) {
	uint64_t id = get_tbl_schema()->get_field_id(col_name);
	set_value(id, ptr);
}

SET_VALUE(uint64_t);
SET_VALUE(int64_t);
SET_VALUE(double);
SET_VALUE(UInt32);
SET_VALUE(SInt32);

GET_VALUE(uint64_t);
GET_VALUE(int64_t);
GET_VALUE(double);
GET_VALUE(UInt32);
GET_VALUE(SInt32);

char * RowData::get_row_value(int id) {
  int pos __attribute__ ((unused));
	pos = get_tbl_schema()->get_field_index_no(id);
  DEBUG("get_row_value pos %d -- %lx\n",pos,(uint64_t)this);
#if SIM_FULL_ROW
	return &data[pos];
#else
	return data;
#endif
}

char * RowData::get_row_value(char * col_name) {
  uint64_t pos __attribute__ ((unused));
	pos = get_tbl_schema()->get_field_index_no(col_name);
#if SIM_FULL_ROW
	return &data[pos];
#else
	return data;
#endif
}

char * RowData::get_data() { 
  return data; 
}

void RowData::set_data(char * data) { 
	int tuple_size = get_tbl_schema()->get_tuple_size();
#if SIM_FULL_ROW
	memcpy(this->data, data, tuple_size);
#else
  char d[tuple_size];
	memcpy(d, data, tuple_size);
#endif
}
// copy from the src to this
void RowData::copy(RowData * src) {
	assert(src->get_tbl_schema() == this->get_tbl_schema());
#if SIM_FULL_ROW
	set_data(src->get_data());
#else
  char d[tuple_size];
	set_data(d);
#endif
}

void RowData::free_row() {
  DEBUG_M("RowData::free_row free\n");
#if SIM_FULL
	alloc_memory.free(data, sizeof(char) * get_tuple_size());
#else
	alloc_memory.free(data, sizeof(uint64_t) * 1);
#endif
}

RC RowData::get_lock(access_t type, TxnMgr * txn) {
  RC rc = RCOK;
#if ALGO == CALVIN
	lock_t lt = (type == RD || type == SCAN)? LOCK_SH : LOCK_EX;
	rc = this->manager->lock_get(lt, txn);
#endif
  return rc;
}

RC RowData::get_row(access_t type, TxnMgr * txn, RowData *& row) {
    RC rc = RCOK;
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE 
    row = this;
    return rc;
#endif
#if ISOLATION_LEVEL == NOLOCK
    row = this;
    return rc;
#endif

#if ALGO == OCCTEMPLATE

    DEBUG_M("RowData::get_row OCCTEMPLATE alloc \n");
	txn->cur_row = (RowData *) alloc_memory.alloc(sizeof(RowData));
	txn->cur_row->init(get_table(), get_part_id());
    rc = this->manager->access(type,txn);
    txn->cur_row->copy(this);
	row = txn->cur_row;
    assert(rc == RCOK);
	goto end;
#endif
#if ALGO == MVCC
    DEBUG_M("RowData::get_row MVCC alloc \n");
	txn->cur_row = (RowData *) alloc_memory.alloc(sizeof(RowData));
	txn->cur_row->init(get_table(), get_part_id());
    rc = this->manager->access(type,txn,txn->cur_row);
	row = txn->cur_row;
    assert(rc == RCOK);
	goto end;
#endif
#if ALGO == HSTORE || ALGO == HSTORE_SPEC || ALGO == CALVIN
#if ALGO == HSTORE_SPEC
  if(txn_table.spec_mode) {
    DEBUG_M("RowData::get_row HSTORE_SPEC alloc \n");
	  txn->cur_row = (RowData *) alloc_memory.alloc(sizeof(RowData));
	  txn->cur_row->init(get_table(), get_part_id());
	  rc = this->manager->access(txn, R_REQ);
	  row = txn->cur_row;
	  goto end;
  }
#endif
	row = this;
	goto end;
#else
	assert(false);
#endif

end:
  return rc;
}

// Return call for get_row if waiting 
RC RowData::get_row_post_wait(access_t type, TxnMgr * txn, RowData *& row) {

  RC rc = RCOK;

  return rc;
}

// the "row" is the row read out in get_row(). For locking based ALGO, 
// the "row" is the same as "this". For time_stamp based ALGO, the 
// "row" != "this", and the "row" must be freed.
// For MVCC, the row will simply serve as a version. The version will be 
// delete during history cleanup.
// For TIMESTAMP, the row will be explicity deleted at the end of access().
// (c.f. row_ts.cpp)
void RowData::return_row(RC rc, access_t type, TxnMgr * txn, RowData * row) {	
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE
  return;
#endif
#if ISOLATION_LEVEL == NOLOCK
  return;
#endif
  /*
#if ISOLATION_LEVEL == READ_UNCOMMITTED
  if(type == RD) {
    return;
  }
#endif
*/
#if ALGO == CALVIN
	assert (row == NULL || row == this || type == XP);
	if (ALGO != CALVIN && ROLL_BACK && type == XP) {// recover from previous writes. should not happen w/ Calvin
		this->copy(row);
	}
	this->manager->lock_release(txn);
#elif ALGO == OCCTEMPLATE 
	assert (row != NULL);
	if (rc == Abort) {
		manager->abort(type,txn);
	} else {
		manager->commit(type,txn,row);
	}

	row->free_row();
  	DEBUG_M("RowData::return_row Occ_template free \n");
	alloc_memory.free(row, sizeof(RowData));
#elif ALGO == MVCC 
	assert (row != NULL);
	if (rc == Abort) {
		manager->abort(type,txn);
	} else {
		manager->commit(type,txn,row);
	}

	row->free_row();
  	DEBUG_M("RowData::return_row Occ_template free \n");
	alloc_memory.free(row, sizeof(RowData));
#elif ALGO == HSTORE || ALGO == HSTORE_SPEC 
	assert (row != NULL);
	if (ROLL_BACK && type == XP) {// recover from previous writes.
		this->copy(row);
	}
	return;
#else 
	assert(false);
#endif
}

