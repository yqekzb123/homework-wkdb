
#ifndef _TPCC_H_
#define _TPCC_H_

#include "workload.h"
#include "txn.h"
#include "query.h"
#include "row.h"

class QryTPCC;
class QueryMsgTPCC;
struct Item_no;

class TableSchema;
class INDEX;
class QryTPCC;
enum TPCCRemTxnType {
  TPCC_PAYMENT_S=0,
  TPCC_PAYMENT0,
  TPCC_PAYMENT1,
  TPCC_PAYMENT2,
  TPCC_PAYMENT3,
  TPCC_PAYMENT4,
  TPCC_PAYMENT5,
  TPCC_NEWORDER_S,
  TPCC_NEWORDER0,
  TPCC_NEWORDER1,
  TPCC_NEWORDER2,
  TPCC_NEWORDER3,
  TPCC_NEWORDER4,
  TPCC_NEWORDER5,
  TPCC_NEWORDER6,
  TPCC_NEWORDER7,
  TPCC_NEWORDER8,
  TPCC_NEWORDER9,
  TPCC_FIN,
  TPCC_RDONE};


class WLTPCC : public WLSchema {
public:
	RC init();
	RC init_table();
	RC init_schema(const char * schema_file);
	RC txn_get_manager(TxnMgr *& txn_manager);
	TableSchema * 		t_warehouse;
	TableSchema * 		t_district;
	TableSchema * 		t_customer;
	TableSchema *		t_history;
	TableSchema *		t_neworder;
	TableSchema *		t_order;
	TableSchema *		t_orderline;
	TableSchema *		t_item;
	TableSchema *		t_stock;

	INDEX * 	i_item;
	INDEX * 	i_warehouse;
	INDEX * 	i_district;
	INDEX * 	i_customer_id;
	INDEX * 	i_customer_last;
	INDEX * 	i_stock;
	INDEX * 	i_order; // key = (w_id, d_id, o_id)
//	INDEX * 	i_order_wdo; // key = (w_id, d_id, o_id)
//	INDEX * 	i_order_wdc; // key = (w_id, d_id, c_id)
	INDEX * 	i_orderline; // key = (w_id, d_id, o_id)
	INDEX * 	i_orderline_wd; // key = (w_id, d_id). 
	
	// XXX HACK
	// For delivary. Only one txn can be delivering a warehouse at a time.
	// *_delivering[warehouse_id] -> the warehouse is delivering.
	bool ** delivering;
//	bool volatile ** delivering;

private:
	uint64_t num_wh;
	void init_tab_item(int id);
	void init_tab_wh();
	void init_tab_dist(uint64_t w_id);
	void init_tab_stock(int id,uint64_t w_id);
	// init_tab_cust initializes both tab_cust and tab_hist.
	void init_tab_cust(int id, uint64_t d_id, uint64_t w_id);
	void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id);
	void init_tab_order(int id,uint64_t d_id, uint64_t w_id);
	
	UInt32 perm_count;
	uint64_t * perm_c_id;
	void init_permutation();
	uint64_t get_permutation();

	static void * threadInitItem(void * This);
	static void * threadInitWh(void * This);
	static void * threadInitDist(void * This);
	static void * threadInitStock(void * This);
	static void * threadInitCust(void * This);
	static void * threadInitHist(void * This);
	static void * threadInitOrder(void * This);
};

  struct thr_args{
    WLTPCC * wl;
    UInt32 id;
    UInt32 tot;
  };

class TxnManTPCC : public TxnMgr
{
public:
	void init(uint64_t thd_id, WLSchema * h_wl);
  void reset();
  RC acquire_locks(); 
	RC run_txn();
	RC run_txn_post_wait();
	RC run_calvin_txn(); 
  RC run_tpcc_phase2(); 
  RC run_tpcc_phase5(); 
	TPCCRemTxnType state;
  void copy_remote_items(QueryMsgTPCC * message); 
private:
	WLTPCC * _wl;
	volatile RC _rc;
  RowData * row;

  uint64_t next_item_id;

void next_tpcc_state();
RC run_txn_state();
  bool is_done();
  bool is_local_item(uint64_t idx);


	RC run_payment_0(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, RowData *& r_wh_local);
	RC run_payment_1(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, RowData * r_wh_local);
	RC run_payment_2(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, RowData *& r_dist_local);
	RC run_payment_3(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, RowData * r_dist_local);
	RC run_payment_4(uint64_t w_id, uint64_t d_id,uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, char * c_last, double h_amount, bool by_last_name, RowData *& r_cust_local); 
	RC run_payment_5(uint64_t w_id, uint64_t d_id,uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, char * c_last, double h_amount, bool by_last_name, RowData * r_cust_local); 
	RC new_order_0(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, RowData *& r_wh_local);
	RC new_order_1(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, RowData * r_wh_local);
	RC new_order_2(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, RowData *& r_cust_local);
	RC new_order_3(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, RowData * r_cust_local);
	RC new_order_4(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, RowData *& r_dist_local);
	RC new_order_5(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, RowData * r_dist_local);
	RC new_order_6(uint64_t ol_i_id, RowData *& r_item_local);
	RC new_order_7(uint64_t ol_i_id, RowData * r_item_local);
	RC new_order_8(uint64_t w_id,uint64_t  d_id,bool remote, uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,uint64_t  ol_number,uint64_t  o_id, RowData *& r_stock_local);
	RC new_order_9(uint64_t w_id,uint64_t  d_id,bool remote, uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,uint64_t  ol_number,uint64_t ol_amount, uint64_t  o_id, RowData * r_stock_local);
	RC run_order_status(QryTPCC * query);
	RC run_delivery(QryTPCC * query);
	RC run_stock_level(QryTPCC * query);
};

#endif
