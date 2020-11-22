#include "test.h"
#include "row.h"

void TxnManCTEST::init(uint64_t thd_id, WLSchema * h_wl) {
	TxnMgr::init(thd_id, h_wl);
	_wl = (WLCTEST *) h_wl;
}

RC TxnManCTEST::run_txn(int type, int access_num) {
	switch(type) {
	case READ_WRITE :
		return testReadwrite(access_num);
	case CONFLICT:
		return testConflict(access_num);
	default:
		assert(false);
	}
}

RC TxnManCTEST::testReadwrite(int access_num) {
	RC rc = RCOK;
	itemidData * m_item;

	m_item = index_read(_wl->the_index, 0, 0);
	RowData * row = ((RowData *)m_item->location);
	RowData * row_local;
	get_row(row, WR, row_local);
	if (access_num == 0) {			
		char str[] = "hello";
		row_local->set_value(0, 1234);
		row_local->set_value(1, 1234.5);
		row_local->set_value(2, 8589934592UL);
		row_local->set_value(3, str);
	} else {
		int v1;
    	double v2;
    	uint64_t v3;
	    char * v4;
    	
		row_local->get_row_value(0, v1);
	    row_local->get_row_value(1, v2);
    	row_local->get_row_value(2, v3);
	    v4 = row_local->get_row_value(3);

    	assert(v1 == 1234);
	    assert(v2 == 1234.5);
    	assert(v3 == 8589934592UL);
	    assert(strcmp(v4, "hello") == 0);
	}
	if(rc == RCOK) 
      rc = start_commit();
    else if(rc == Abort)
      rc = start_abort();
	if (access_num == 0)
		return RCOK;
	else 
		return FINISH;
}

RC 
TxnManCTEST::testConflict(int access_num)
{
	RC rc = RCOK;
	itemidData * m_item;

	idx_key_t key;
	for (key = 0; key < 1; key ++) {
		m_item = index_read(_wl->the_index, key, 0);
		RowData * row = ((RowData *)m_item->location);
		RowData * row_local; 
		get_row(row, WR, row_local);
		if (row_local) {
			char str[] = "hello";
			row_local->set_value(0, 1234);
			row_local->set_value(1, 1234.5);
			row_local->set_value(2, 8589934592UL);
			row_local->set_value(3, str);
			sleep(1);
		} else {
			rc = Abort;
			break;
		}
	}
	if(rc == RCOK) 
      rc = start_commit();
    else if(rc == Abort)
	{
		rc = start_abort();
	}
	return rc;
}
