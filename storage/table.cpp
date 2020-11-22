
#include "global.h"
#include "universal.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "mem_alloc.h"

void TableSchema::init(CatalogSchema * schema) {
	this->table_name = schema->table_name;
	this->table_id = schema->table_id;
	this->schema = schema;
	cur_tab_size = new uint64_t; 
	// isolate cur_tab_size with other parameters.
	// Because cur_tab_size is frequently updated, causing false 
	// sharing problems
	char * ptr = new char[CL_SIZE*2 + sizeof(uint64_t)];
	cur_tab_size = (uint64_t *) &ptr[CL_SIZE];
}

RC TableSchema::get_new_row(RowData *& row) {
	// this function is obsolete. 
	assert(false);
	return RCOK;
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC TableSchema::get_new_row(RowData *& row, uint64_t part_id, uint64_t &row_id) {
	RC rc = RCOK;
  DEBUG_M("TableSchema::get_new_row alloc\n");
	void * ptr = alloc_memory.alloc(sizeof(RowData));
	assert (ptr != NULL);
	
	row = (RowData *) ptr;
	rc = row->init(this, part_id, row_id);
	row->init_manager(row);

	return rc;
}
