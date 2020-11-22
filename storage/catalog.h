#ifndef _CATALOG_H_
#define _CATALOG_H_

#include <map>
#include <vector>
#include "global.h"
#include "universal.h"

class Column {
public:
	Column() {
		this->type = new char[80];
		this->name = new char[80];
	}
	Column(uint64_t size, char * type, char * name, 
		uint64_t id, uint64_t index) 
	{
		this->size = size;
		this->id = id;
		this->index = index;
		this->type = new char[80];
		this->name = new char[80];
		strcpy(this->type, type);
		strcpy(this->name, name);
	};

	UInt64 id;
	UInt32 size;
	UInt32 index;
	char * type;
	char * name;
	char pad[CL_SIZE - sizeof(uint64_t)*3 - sizeof(char *)*2];
};

class CatalogSchema {
public:
	// abandoned init function
	// field_size is the size of each each field.
	void init(const char * table_name, uint32_t table_id, int field_cnt);
	void add_col(char * col_name, uint64_t size, char * type);

	UInt32 			field_cnt;
 	const char * 	table_name;
 	uint32_t 	  table_id;
	
	UInt32 			get_tuple_size() { return tuple_size; };
	
	uint64_t 		get_field_cnt() { return field_cnt; };
	uint64_t 		get_field_size(int id) { return _columns[id].size; };
	uint64_t 		get_field_index_no(int id) { return _columns[id].index; };
	char * 			get_field_type(uint64_t id);
	char * 			get_field_name(uint64_t id);
	uint64_t 		get_field_id(const char * name);
	char * 			get_field_type(char * name);
	uint64_t 		get_field_index_no(char * name);

	void 			print_schema();
	Column * 		_columns;
	UInt32 			tuple_size;
private:
	char pad[CL_SIZE - sizeof(uint64_t)*2 - sizeof(int) - sizeof(char *)*2 - sizeof(uint32_t)];
};

#endif
