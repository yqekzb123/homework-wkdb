
#ifndef _TABLE_H_
#define _TABLE_H_

#include "global.h"

// TODO sequential scan is not supported yet.
// only index access is supported for table. 

class CatalogSchema;
class RowData;

class TableSchema
{
public:
	void init(CatalogSchema * schema);
	// row lookup should be done with index. But index does not have
	// records for new rows. get_new_row returns the pointer to a 
	// new row.	
	RC get_new_row(RowData *& row); // this is equivalent to insert()
	RC get_new_row(RowData *& row, uint64_t part_id, uint64_t &row_id);

	void delete_row(); // TODO delete_row is not supportet yet

	uint64_t get_table_size() { return *cur_tab_size; };
	CatalogSchema * get_tbl_schema() { return schema; };
	const char * get_table_name() { return table_name; };
	uint32_t get_table_id() { return table_id; };

	CatalogSchema * 		schema;
private:
	const char * 	table_name;
  uint32_t table_id;
	uint64_t * 		cur_tab_size;
	char 			pad[CL_SIZE - sizeof(void *)*3 - sizeof(uint32_t)];
};

#endif
