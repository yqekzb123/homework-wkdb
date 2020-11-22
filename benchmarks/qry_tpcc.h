
#ifndef _QryTPCC_H_
#define _QryTPCC_H_

#include "global.h"
#include "universal.h"
#include "query.h"

class WLSchema;
class Msg;
class QueryMsgTPCC;
class QueryMsgClientTPCC;

// items of new order transaction
struct Item_no {
	uint64_t ol_i_id;
	uint64_t ol_supply_w_id;
	uint64_t ol_quantity;
  void copy(Item_no * item) {
    ol_i_id = item->ol_i_id;
    ol_supply_w_id = item->ol_supply_w_id;
    ol_quantity = item->ol_quantity;
  }
};


class QueryGenTPCC : public QryGenerator {
public:
  BaseQry * create_query(WLSchema * h_wl, uint64_t home_partition_id);

private:
	BaseQry * gen_requests(uint64_t home_partition_id, WLSchema * h_wl);
  BaseQry * gen_payment(uint64_t home_partition); 
  BaseQry * gen_new_order(uint64_t home_partition); 
	myrand * mrand;
};

class QryTPCC : public BaseQry {
public:
	void init(uint64_t thd_id, WLSchema * h_wl);
  void init();
  void reset();
  void release();
  void release_items();
  void print();
  static std::set<uint64_t> participants(Msg * message, WLSchema * wl); 
  uint64_t participants(bool *& pps,WLSchema * wl); 
  uint64_t get_participants(WLSchema * wl); 
  bool readonly();

	TPCCTxnType txn_type;
	// common txn input for both payment & new-order
	uint64_t w_id;
	uint64_t d_id;
	uint64_t c_id;
	// txn input for payment
	uint64_t d_w_id;
	uint64_t c_w_id;
	uint64_t c_d_id;
	char c_last[LASTNAME_LEN];
	double h_amount;
	bool by_last_name;
	// txn input for new-order
	//Item_no * items;
  Array<Item_no*> items;
	bool rbk;
	bool remote;
	uint64_t ol_cnt;
	uint64_t o_entry_d;
	// Input for delivery
	uint64_t o_carrier_id;
	uint64_t ol_delivery_d;
	// for order-status

	// Other
	uint64_t ol_i_id;
	uint64_t ol_supply_w_id;
	uint64_t ol_quantity;
	uint64_t ol_number;
	uint64_t ol_amount;
	uint64_t o_id;
  uint64_t rqry_req_cnt;

};

#endif
