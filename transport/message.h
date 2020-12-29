
#ifndef _MESSAGE_H_
#define _MESSAGE_H_

#include "global.h"
#include "universal.h"
#include "logMan.h"
#include "array.h"

class rqst_ycsb;
class RecordLog;
struct Item_no;

class Msg {
public:
  virtual ~Msg(){}
  static Msg * create_message(char * buf); 
  static Msg * create_message(BaseQry * query, RemReqType rtype); 
  static Msg * create_message(TxnMgr * txn, RemReqType rtype); 
  static Msg * create_message(uint64_t txn_id, RemReqType rtype); 
  static Msg * create_message(uint64_t txn_id,uint64_t batch_id, RemReqType rtype); 
  static Msg * create_message(RecordLog * record, RemReqType rtype); 
  static Msg * create_message(RemReqType rtype); 
  static std::vector<Msg*> * create_messages(char * buf); 
  static void release_message(Msg * message); 
  RemReqType rtype;
  uint64_t txn_id;
  uint64_t batch_id;
  uint64_t return_node_id;

  uint64_t wq_time;
  uint64_t mq_time;
  uint64_t ntwk_time;

  // Collect other stats
  double lat_task_queue_time;
  double lat_msg_queue_time;
  double lat_cc_block_time;
  double lat_cc_time;
  double lat_process_time;
  double lat_network_time;
  double lat_other_time;

  uint64_t mget_size();
  uint64_t read_txn_id() {return txn_id;}
  uint64_t get_batch_id() {return batch_id;}
  uint64_t get_return_id() {return return_node_id;}
  void mcopy_from_buf(char * buf);
  void mcopy_to_buf(char * buf);
  void mcopy_from_txn(TxnMgr * txn);
  void mcopy_to_txn(TxnMgr * txn);
  RemReqType get_rtype() {return rtype;}

  virtual uint64_t get_size() = 0;
  virtual void copy_from_buf(char * buf) = 0;
  virtual void copy_to_buf(char * buf) = 0;
  virtual void copy_to_txn(TxnMgr * txn) = 0;
  virtual void copy_from_txn(TxnMgr * txn) = 0;
  virtual void init() = 0;
  virtual void release() = 0;
};

// Msg types
class InitDoneMessage : public Msg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init() {}
  void release() {}
};

class FinishMessage : public Msg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init() {}
  void release() {}
  bool is_abort() { return rc == Abort;}

  uint64_t pid;
  RC rc;
  //uint64_t txn_id;
  //uint64_t batch_id;
  bool readonly;
#if ALGO == OCCTEMPLATE
  uint64_t commit_timestamp;
#endif
};

class LogMessage : public Msg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init() {}
  void release(); 
  void copy_from_record(RecordLog * record);

  //Array<RecordLog*> log_records;
  RecordLog record;
};

class RspLogMessage : public Msg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init() {}
  void release() {}
};

class FlushedLogMessage : public Msg {
public:
  void copy_from_buf(char * buf) {}
  void copy_to_buf(char * buf) {}
  void copy_from_txn(TxnMgr * txn) {}
  void copy_to_txn(TxnMgr * txn) {}
  uint64_t get_size() {return sizeof(FlushedLogMessage);}
  void init() {}
  void release() {}

};


class QueryResponseMessage : public Msg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  RC rc;
  uint64_t pid;

};

class AckMessage : public Msg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  RC rc;
#if ALGO == OCCTEMPLATE
  uint64_t lower;
  uint64_t upper;
#endif

  // For Calvin PPS: part keys from secondary lookup for sequencer response
  Array<uint64_t> part_keys;
};

class PrepareMessage : public Msg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  uint64_t pid;
  RC rc;
  uint64_t txn_id;
};

class ForwardMessage : public Msg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  RC rc;
#if WORKLOAD == TPCC
	uint64_t o_id;
#endif
};


class DoneMessage : public Msg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init() {}
  void release() {}
  uint64_t batch_id;
};

class ClientRspMessage : public Msg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  RC rc;
  uint64_t client_start_ts;
};

class ClientQryMsg : public Msg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_query(BaseQry * query); 
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init();
  void release();

  uint64_t pid;
  uint64_t ts;
#if ALGO == CALVIN
  uint64_t batch_id;
  uint64_t txn_id;
#endif
  uint64_t client_start_ts;
  uint64_t first_startts;
  Array<uint64_t> parts_assign;
};

class QueryMsgYCSBcl : public ClientQryMsg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_query(BaseQry * query);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init(); 
  void release(); 

  Array<rqst_ycsb*> requests;

};

class QueryMsgClientTPCC : public ClientQryMsg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_query(BaseQry * query);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init(); 
  void release(); 

  uint64_t txn_type;
	// common txn input for both payment & new-order
  uint64_t w_id;
  uint64_t d_id;
  uint64_t c_id;

  // payment
  uint64_t d_w_id;
  uint64_t c_w_id;
  uint64_t c_d_id;
	char c_last[LASTNAME_LEN];
  uint64_t h_amount;
  bool by_last_name;

  // new order
  Array<Item_no*> items;
  bool rbk;
  bool remote;
  uint64_t ol_cnt;
  uint64_t o_entry_d;

};

class PPSClientQueryMessage : public ClientQryMsg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_query(BaseQry * query);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init(); 
  void release(); 

  uint64_t txn_type;

  // getparts 
  uint64_t part_key;
  // getproducts / getpartbyproduct
  uint64_t product_key;
  // getsuppliers / getpartbysupplier
  uint64_t supplier_key;

  // part keys from secondary lookup
  Array<uint64_t> part_keys;

  bool recon;

};

class DAClientQueryMessage : public ClientQryMsg {
 public:
  void copy_from_buf(char* buf);//ok
  void copy_to_buf(char* buf);//ok
  void copy_from_query(BaseQry* query);//ok
  void copy_from_txn(TxnMgr* txn);//ok
  void copy_to_txn(TxnMgr* txn);
  uint64_t get_size();
  void init();
  void release();

  DATxnType txn_type;
	uint64_t trans_id;
	uint64_t item_id;
	uint64_t seq_id;
	uint64_t write_version;
	uint64_t state;
	uint64_t next_state;
	uint64_t last_state;
};

class QueryMessage : public Msg {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  uint64_t pid;

#if MODE==QRY_ONLY_MODE
  uint64_t max_access;
#endif
};

class QueryMsgYCSB : public QueryMessage {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init();
  void release(); 

 Array<rqst_ycsb*> requests;

};

class QueryMsgTPCC : public QueryMessage {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init();
  void release(); 

  uint64_t txn_type;
  uint64_t state; 

	// common txn input for both payment & new-order
  uint64_t w_id;
  uint64_t d_id;
  uint64_t c_id;

  // payment
  uint64_t d_w_id;
  uint64_t c_w_id;
  uint64_t c_d_id;
	char c_last[LASTNAME_LEN];
  uint64_t h_amount;
  bool by_last_name;

  // new order
  Array<Item_no*> items;
	bool rbk;
  bool remote;
  uint64_t ol_cnt;
  uint64_t o_entry_d;

};

class PPSQueryMessage : public QueryMessage {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnMgr * txn);
  void copy_to_txn(TxnMgr * txn);
  uint64_t get_size();
  void init();
  void release(); 

  uint64_t txn_type;
  uint64_t state; 

  // getparts 
  uint64_t part_key;
  // getproducts / getpartbyproduct
  uint64_t product_key;
  // getsuppliers / getpartbysupplier
  uint64_t supplier_key;

  // part keys from secondary lookup
  Array<uint64_t> part_keys;
};

class DAQueryMessage : public QueryMessage {
 public:
  void copy_from_buf(char* buf);
  void copy_to_buf(char* buf);
  void copy_from_txn(TxnMgr* txn);
  void copy_to_txn(TxnMgr* txn);
  uint64_t get_size();
  void init();
  void release();

  DATxnType txn_type;
	uint64_t trans_id;
	uint64_t item_id;
	uint64_t seq_id;
	uint64_t write_version;
	uint64_t state;
	uint64_t next_state;
	uint64_t last_state;
};

#endif
