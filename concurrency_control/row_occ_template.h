
#ifndef ROW_OCC_TEMPLATE_H
#define ROW_OCC_TEMPLATE_H

class Row_occ_template {
public:
	void init(RowData * row);
  RC access(access_t type, TxnMgr * txn);
  RC read_and_prewrite(TxnMgr * txn);
  RC read(TxnMgr * txn);
  RC prewrite(TxnMgr * txn);
  RC abort(access_t type, TxnMgr * txn);
  RC commit(access_t type, TxnMgr * txn, RowData * data);
  void write(RowData * data);
	
private:
  volatile bool occ_template_avail;
	
	RowData * _row;
	
  std::set<uint64_t> * uc_reads;
  std::set<uint64_t> * uc_writes;
  uint64_t read_ts;
  uint64_t write_ts;
};

#endif
