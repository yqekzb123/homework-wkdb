#include "test.h"
#include "table.h"
#include "row.h"
#include "mem_alloc.h"
#include "IndexHash.h"
#include "IndexBTree.h"
#include "thread.h"

RC WLCTEST::init() {
	WLSchema::init();
	string path;
	path = "./benchmarks/schema_TEST.txt";
	init_schema( path.c_str() );

	init_table();
	return RCOK;
}

RC WLCTEST::init_schema(const char * schema_file) {
	WLSchema::init_schema(schema_file);
	the_table = tables["MAIN_TABLE"]; 	
	the_index = indexes["MAIN_INDEX"];
	return RCOK;
}

RC WLCTEST::init_table() {
	RC rc = RCOK;
	for (int rid = 0; rid < 10; rid ++) {
		RowData * new_row = NULL;
		uint64_t row_id;
		int part_id = 0;
        rc = the_table->get_new_row(new_row, part_id, row_id); 
		assert(rc == RCOK);
		uint64_t primary_key = rid;
		new_row->set_primary_key(primary_key);
        new_row->set_value(0, rid);
        new_row->set_value(1, 0);
        new_row->set_value(2, 0);
        itemidData * m_item = (itemidData *) alloc_memory.alloc( sizeof(itemidData));
		assert(m_item != NULL);
		m_item->type = DT_row;
		m_item->location = new_row;
		m_item->valid = true;
		uint64_t idx_key = primary_key;
        rc = the_index->index_insert(idx_key, m_item, 0);
        assert(rc == RCOK);
    }
	return rc;
}

RC WLCTEST::txn_get_manager(TxnMgr *& txn_manager) {
	txn_manager = (TxnManCTEST *) alloc_memory.alloc( sizeof(TxnManCTEST));
	new(txn_manager) TxnManCTEST();
	// txn_manager->init(this);
	return RCOK;
}

void WLCTEST::summarize() {
	uint64_t curr_time = acquire_ts();
	if (g_test_case == CONFLICT) {
		assert(curr_time - time > g_thd_cnt * 1e9);
		// int total_wait_cnt = 0;
		// for (UInt32 tid = 0; tid < g_thd_cnt; tid ++) {
			// total_wait_cnt += stats._stats[tid]->wait_cnt;
		// }
		printf("CONFLICT TEST. PASSED.\n");
	}
}
