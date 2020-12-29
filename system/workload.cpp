
#include "global.h"
#include "universal.h"
#include "workload.h"
#include "row.h"
#include "table.h"
#include "IndexHash.h"
#include "IndexBTree.h"
#include "catalog.h"
#include "mem_alloc.h"

RC WLSchema::init() {
	return RCOK;
}

RC WLSchema::init_schema(const char * schema_file) {
    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);	
	string line;
  uint32_t id = 0;
	ifstream fin(schema_file);
    CatalogSchema * schema;
    while (getline(fin, line)) {
		if (line.compare(0, 6, "TABLE=") == 0) {
			string tname(&line[6]);
			void * tmp = new char[CL_SIZE * 2 + sizeof(CatalogSchema)];
            schema = (CatalogSchema *) ((UInt64)tmp + CL_SIZE);
			getline(fin, line);
			int col_count = 0;
			// Read all fields for this table.
			vector<string> lines;
			while (line.length() > 1) {
				lines.push_back(line);
				getline(fin, line);
			}
			schema->init( tname.c_str(), id++, lines.size() );
			for (UInt32 i = 0; i < lines.size(); i++) {
				string line = lines[i];
				vector<string> items;

        char * line_cstr = new char [line.length()+1];
        strcpy(line_cstr,line.c_str());
        int size = atoi(strtok(line_cstr,","));
        char * type = strtok(NULL,",");
        char * name = strtok(NULL,",");

        schema->add_col(name, size, type);
				col_count ++;
			} 
			tmp = new char[CL_SIZE * 2 + sizeof(TableSchema)];
            TableSchema * cur_tab = (TableSchema *) ((UInt64)tmp + CL_SIZE);
			cur_tab->init(schema);
			tables[tname] = cur_tab;
        } else if (!line.compare(0, 6, "INDEX=")) {
			string iname(&line[6]);
			getline(fin, line);

			vector<string> items;
			string token;
			size_t pos;
			while (line.length() != 0) {
				pos = line.find(","); // != std::string::npos) {
				if (pos == string::npos)
					pos = line.length();
	    		token = line.substr(0, pos);
				items.push_back(token);
		    	line.erase(0, pos + 1);
			}
			
			string tname(items[0]);
			int field_cnt = items.size() - 1;
			uint64_t * fields = new uint64_t [field_cnt];
			for (int i = 0; i < field_cnt; i++) 
				fields[i] = atoi(items[i + 1].c_str());
			INDEX * index = new INDEX;
	    int part_cnt __attribute__ ((unused));
			part_cnt = (CENTRAL_INDEX)? 1 : g_cnt_part;

      uint64_t table_size = g_table_size_synth;
#if WORKLOAD == TPCC
      if ( !tname.compare(1, 9, "WAREHOUSE") ) {
        table_size = g_wh_num / g_cnt_part;
        printf("WAREHOUSE size %ld\n",table_size);
      } else if ( !tname.compare(1, 8, "DISTRICT") ) {
        table_size = g_wh_num / g_cnt_part * g_dist_per_wh;
        printf("DISTRICT size %ld\n",table_size);
      } else if ( !tname.compare(1, 8, "CUSTOMER") ) {
        table_size = g_wh_num / g_cnt_part * g_dist_per_wh * g_cust_per_dist;
        printf("CUSTOMER size %ld\n",table_size);
      } else if ( !tname.compare(1, 7, "HISTORY") ) {
        table_size = g_wh_num / g_cnt_part * g_dist_per_wh * g_cust_per_dist;
        printf("HISTORY size %ld\n",table_size);
      } else if ( !tname.compare(1, 5, "ORDER") ) {
        table_size = g_wh_num / g_cnt_part * g_dist_per_wh * g_cust_per_dist;
        printf("ORDER size %ld\n",table_size);
      } else if ( !tname.compare(1, 4, "ITEM") ) {
        table_size = g_max_items;
        printf("ITEM size %ld\n",table_size);
      } else if ( !tname.compare(1, 5, "STOCK") ) {
        table_size = g_wh_num / g_cnt_part * g_max_items;
        printf("STOCK size %ld\n",table_size);
      }
#elif WORKLOAD == PPS
      if ( !tname.compare(1, 5, "PARTS") ) {
        table_size = MAX_PPS_PART_KEY;
      }
      else if ( !tname.compare(1, 8, "PRODUCTS") ) {
        table_size = MAX_PPS_PRODUCT_KEY;
      }
      else if ( !tname.compare(1, 9, "SUPPLIERS") ) {
        table_size = MAX_PPS_SUPPLIER_KEY;
      }
      else if ( !tname.compare(1, 8, "SUPPLIES") ) {
        table_size = MAX_PPS_PRODUCT_KEY;
      }
      else if ( !tname.compare(1, 4, "USES") ) {
        table_size = MAX_PPS_SUPPLIER_KEY;
      }
#elif WORKLOAD == DA
      if (!tname.compare(1, 5, "DAtab")) {
        table_size = MAX_DA_TABLE_SIZE;
      }
#else
      table_size = g_table_size_synth / g_cnt_part;
#endif

#if INDEX_STRUCT == IDX_HASH
			index->init(1024, tables[tname], table_size);
			//index->init(part_cnt*1024, tables[tname], table_size);
#else
			index->init(part_cnt, tables[tname]);
#endif
			indexes[iname] = index;
		}
    }
	fin.close();
	return RCOK;
}


void WLSchema::index_delete_all() {
  #if WORKLOAD ==DA
    for (auto index :indexes) {
      index.second->index_reset();
    }
  #endif
}

void WLSchema::index_insert(string index_name, uint64_t key, RowData * row) {
	assert(false);
	INDEX * index = (INDEX *) indexes[index_name];
	index_insert(index, key, row);
}

void WLSchema::index_insert(INDEX * index, uint64_t key, RowData * row, int64_t part_id) {
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemidData * m_item =
		(itemidData *) alloc_memory.alloc( sizeof(itemidData));
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;

  assert(index);
  assert( index->index_insert(key, m_item, pid) == RCOK );
}

void WLSchema::index_insert_non_unique(INDEX * index, uint64_t key, RowData * row, int64_t part_id) {
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemidData * m_item =
		(itemidData *) alloc_memory.alloc( sizeof(itemidData));
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;

  assert(index);
  assert( index->index_insert_non_unique(key, m_item, pid) == RCOK );
}



