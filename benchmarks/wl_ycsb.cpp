
#include "global.h"
#include "universal.h"
#include "ycsb.h"
#include "workload.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "IndexHash.h"
#include "IndexBTree.h"
#include "catalog.h"
#include "manager.h"
// #include "row_ts.h"
// #include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

int WLYCSB::next_tid;

RC WLYCSB::init() {
	WLSchema::init();
	next_tid = 0;
	char * cpath = getenv("SCHEMA_PATH");
	string path;
	if (cpath == NULL) 
		path = "./benchmarks/schema_YCSB.txt";
	else { 
		path = string(cpath);
		path += "schema_YCSB.txt";

	}
  printf("Initializing schema... ");
  fflush(stdout);
	init_schema( path.c_str() );
  printf("Done\n");
	
  printf("Initializing table... ");
  fflush(stdout);
	init_table_parallel();
  printf("Done\n");
  fflush(stdout);
//	init_table();
	return RCOK;
}

RC WLYCSB::init_schema(const char * schema_file) {
	WLSchema::init_schema(schema_file);
	the_table = tables["MAIN_TABLE"]; 	
	the_index = indexes["MAIN_INDEX"];
	return RCOK;
}
	
int 
WLYCSB::key_to_part(uint64_t key) {
	//uint64_t rows_per_part = g_table_size_synth / g_cnt_part;
	//return key / rows_per_part;
  return key % g_cnt_part;
}

RC WLYCSB::init_table() {
	RC rc;
    uint64_t total_row = 0;
    while (true) {
    	for (UInt32 part_id = 0; part_id < g_cnt_part; part_id ++) {
            if (total_row > g_table_size_synth)
                goto ins_done;
            // Assumes striping of parts_assign to nodes
            if(g_cnt_part % g_cnt_node != g_node_id) {
              total_row++;
              continue;
            }
            RowData * new_row = NULL;
			uint64_t row_id;
            rc = the_table->get_new_row(new_row, part_id, row_id); 
            // insertion of last row may fail after the table_size
            // is updated. So never access the last record in a table
			assert(rc == RCOK);
//			uint64_t value = rand();
			uint64_t primary_key = total_row;
			new_row->set_primary_key(primary_key);
            new_row->set_value(0, &primary_key);
			CatalogSchema * schema = the_table->get_tbl_schema();
			for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
				int field_size = schema->get_field_size(fid);
				char value[field_size];
				for (int i = 0; i < field_size; i++) 
					value[i] = (char)rand() % (1<<8) ;
				new_row->set_value(fid, value);
			}
            itemidData * m_item =
                (itemidData *) alloc_memory.alloc( sizeof(itemidData));
			assert(m_item != NULL);
            m_item->type = DT_row;
            m_item->location = new_row;
            m_item->valid = true;
            uint64_t idx_key = primary_key;
            rc = the_index->index_insert(idx_key, m_item, part_id);
            assert(rc == RCOK);
            total_row ++;
        }
    }
ins_done:
    printf("[YCSB] Table \"MAIN_TABLE\" initialized.\n");
    return RCOK;

}

// init table in parallel
void WLYCSB::init_table_parallel() {
	enable_thread_mem_pool = true;
	pthread_t * p_threads = new pthread_t[g_init_parallelism - 1];
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
		pthread_create(&p_threads[i], NULL, threadInitTable, this);
	}
	threadInitTable(this);

	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_threads[i], NULL);
		//printf("thread %d complete\n", i);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}
	enable_thread_mem_pool = false;
}

void * WLYCSB::init_table_slice() {
	UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
	RC rc;
	assert(g_table_size_synth % g_init_parallelism == 0);
	assert(tid < g_init_parallelism);
  uint64_t key_cnt = 0;
	while ((UInt32)ATOM_FETCH_ADD(next_tid, 0) < g_init_parallelism) {}
	assert((UInt32)ATOM_FETCH_ADD(next_tid, 0) == g_init_parallelism);
	uint64_t slice_size = g_table_size_synth / g_init_parallelism;
	for (uint64_t key = slice_size * tid; 
			key < slice_size * (tid + 1); 
			//key ++
	) {
    if(GET_NODE_ID(key_to_part(key)) != g_node_id) {
      ++key;
      continue;
    }

    ++key_cnt;
    if(key_cnt % 500000 == 0) {
      printf("Thd %d inserted %ld keys %f\n",tid,key_cnt,simulate_man->seconds_from_begin(acquire_ts()));
    }
//		printf("tid=%d. key=%ld\n", tid, key);
		RowData * new_row = NULL;
		uint64_t row_id;
		int part_id = key_to_part(key); // % g_cnt_part;
		rc = the_table->get_new_row(new_row, part_id, row_id); 
		assert(rc == RCOK);
//		uint64_t value = rand();
		uint64_t primary_key = key;
		new_row->set_primary_key(primary_key);
#if SIM_FULL_ROW
		new_row->set_value(0, &primary_key,sizeof(uint64_t));
		
		CatalogSchema * schema = the_table->get_tbl_schema();
		for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
//			int field_size = schema->get_field_size(fid);
//			char value[field_size];
//			for (int i = 0; i < field_size; i++) 
//				value[i] = (char)rand() % (1<<8) ;
			char value[6] = "hello";
			new_row->set_value(fid, value,sizeof(value));
		}
#endif

		itemidData * m_item =
			(itemidData *) alloc_memory.alloc( sizeof(itemidData));
		assert(m_item != NULL);
		m_item->type = DT_row;
		m_item->location = new_row;
		m_item->valid = true;
		uint64_t idx_key = primary_key;
		
		rc = the_index->index_insert(idx_key, m_item, part_id);
		assert(rc == RCOK);
    key += g_cnt_part;
	}
  printf("Thd %d inserted %ld keys\n",tid,key_cnt);
	return NULL;
}

RC WLYCSB::txn_get_manager(TxnMgr *& txn_manager){
  DEBUG_M("WLYCSB::txn_get_manager TxnManYCSB alloc\n");
	txn_manager = (TxnManYCSB *)
		alloc_memory.align_alloc( sizeof(TxnManYCSB));
	new(txn_manager) TxnManYCSB();
	//txn_manager->init(this); 
	return RCOK;
}

