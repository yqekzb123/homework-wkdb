#ifndef _INDEX_BASE_H_
#define _INDEX_BASE_H_

#include "global.h"

class TableSchema;

class index_base {
public:
	virtual RC 			init() { return RCOK; };
	
	virtual RC 			init(uint64_t size) { return RCOK; };

	virtual bool 		index_is_exist(idx_key_t key)=0; // check if the key exist.

	virtual RC 			index_insert(idx_key_t key, 
							itemidData * item, 
							int part_id=-1)=0;

	virtual RC 			index_insert_non_unique(idx_key_t key, 
							itemidData * item, 
							int part_id=-1)=0;

	virtual RC	 		index_read(idx_key_t key, 
							itemidData * &item,
							int part_id=-1)=0;
	
	virtual RC	 		index_read(idx_key_t key, 
							itemidData * &item,
							int part_id=-1, int thd_id=0)=0;

	// TODO implement index_remove
	virtual RC 			index_remove(idx_key_t key) { return RCOK; };
	
	// the index in on "table". The key is the merged key of "fields"
	TableSchema * 			table;
};

#endif
