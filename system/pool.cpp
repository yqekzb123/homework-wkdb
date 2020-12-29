
#include "pool.h"
#include "global.h"
#include "universal.h"
#include "txn.h"
#include "mem_alloc.h"
#include "workload.h"
#include "qry_ycsb.h"
#include "ycsb.h"
#include "qry_tpcc.h"
#include "da.h"
#include "da_query.h"
#include "query.h"
#include "msg_queue.h"
#include "row.h"

#define TRY_LIMIT 10

void TxnManPool::init(WLSchema * wl, uint64_t size) {
  _wl = wl;
#if ALGO == CALVIN
  pool = new boost::lockfree::queue<TxnMgr* > (size);
#else
  pool = new boost::lockfree::queue<TxnMgr* > * [g_thread_total_cnt];
#endif
  TxnMgr * txn;
  for(uint64_t thd_id = 0; thd_id < g_thread_total_cnt; thd_id++) {
#if ALGO != CALVIN
	pool[thd_id] = new boost::lockfree::queue<TxnMgr* > (size);
#endif
	for(uint64_t i = 0; i < size; i++) {
	//put(items[i]);
	  _wl->txn_get_manager(txn);
	  txn->init(thd_id,_wl);
	  put(thd_id, txn);
	}
  }
}

void TxnManPool::get(uint64_t thd_id, TxnMgr *& item) {
#if ALGO == CALVIN
  bool r = pool->pop(item);
#else
  bool r = pool[thd_id]->pop(item);
#endif
  if(!r) {
	_wl->txn_get_manager(item);
  }
  item->init(thd_id,_wl);
}

void TxnManPool::put(uint64_t thd_id, TxnMgr * item) {
  item->release();
  int try_times = 0;
#if ALGO == CALVIN
  while(!pool->push(item) && try_times++ < TRY_LIMIT) { }
#else
  while(!pool[thd_id]->push(item) && try_times++ < TRY_LIMIT) { }
#endif
  if(try_times >= TRY_LIMIT) {
	alloc_memory.free(item,sizeof(TxnMgr));
  }
}

void TxnManPool::free_pool() {
  TxnMgr * item;
  for(uint64_t thd_id = 0; thd_id < g_thread_total_cnt; thd_id++) {
#if ALGO == CALVIN
  while(pool->pop(item)) {
#else
  while(pool[thd_id]->pop(item)) {
#endif
	alloc_memory.free(item,sizeof(TxnMgr));
  }
  }
}

void TxnPool::init(WLSchema * wl, uint64_t size) {
  _wl = wl;
#if ALGO == CALVIN
  pool = new boost::lockfree::queue<Txn*  > (size);
#else
  pool = new boost::lockfree::queue<Txn* > * [g_thread_total_cnt];
#endif
  Txn * txn;
  for(uint64_t thd_id = 0; thd_id < g_thread_total_cnt; thd_id++) {
#if ALGO != CALVIN
	pool[thd_id] = new boost::lockfree::queue<Txn*  > (size);
#endif
	for(uint64_t i = 0; i < size; i++) {
	//put(items[i]);
	txn = (Txn*) alloc_memory.alloc(sizeof(Txn));
	txn->init();
	put(thd_id,txn);
	}
  }
}

void TxnPool::get(uint64_t thd_id, Txn *& item) {
#if ALGO == CALVIN
  bool r = pool->pop(item);
#else
  bool r = pool[thd_id]->pop(item);
#endif
  if(!r) {
	item = (Txn*) alloc_memory.alloc(sizeof(Txn));
	item->init();
  }
}

void TxnPool::put(uint64_t thd_id,Txn * item) {
  //item->release();
  item->reset(thd_id);
  int try_times = 0;
#if ALGO == CALVIN
  while(!pool->push(item) && try_times++ < TRY_LIMIT) { }
#else
  while(!pool[thd_id]->push(item) && try_times++ < TRY_LIMIT) { }
#endif
  if(try_times >= TRY_LIMIT) {
	item->release(thd_id);
	alloc_memory.free(item,sizeof(Txn));
  }
}

void TxnPool::free_pool() {
  TxnMgr * item;
	for(uint64_t thd_id = 0; thd_id < g_thread_total_cnt; thd_id++) {
#if ALGO == CALVIN
  while(pool->pop(item)) {
#else
  while(pool[thd_id]->pop(item)) {
#endif
	alloc_memory.free(item,sizeof(item));

  }
	}
}

void QryPool::init(WLSchema * wl, uint64_t size) {
  	_wl = wl;
#if ALGO == CALVIN
  	pool = new boost::lockfree::queue<BaseQry* > (size);
#else
  	pool = new boost::lockfree::queue<BaseQry*> * [g_thread_total_cnt];
#endif
	BaseQry * qry=NULL;
	DEBUG_M("QryPool alloc init\n");
	for(uint64_t thd_id = 0; thd_id < g_thread_total_cnt; thd_id++) {
#if ALGO != CALVIN
	pool[thd_id] = new boost::lockfree::queue<BaseQry* > (size);
#endif
	for(uint64_t i = 0; i < size; i++) {
	//put(items[i]);
#if WORKLOAD==TPCC
	QryTPCC * m_qry = (QryTPCC *) alloc_memory.alloc(sizeof(QryTPCC));
	m_qry = new QryTPCC();
#elif WORKLOAD==YCSB
	QryYCSB * m_qry = (QryYCSB *) alloc_memory.alloc(sizeof(QryYCSB));
	m_qry = new QryYCSB();
#elif WORKLOAD==TEST
	QryTPCC * m_qry = (QryTPCC *) alloc_memory.alloc(sizeof(QryTPCC));
	m_qry = new QryTPCC();
#elif WORKLOAD==DA
	DAQuery * m_qry = (DAQuery *) alloc_memory.alloc(sizeof(DAQuery));
	m_qry = new DAQuery();
#endif
	m_qry->init();
	qry = m_qry;
	put(thd_id,qry);
	}
  }
}

void QryPool::get(uint64_t thd_id, BaseQry *& item) {
#if ALGO == CALVIN
  	bool r = pool->pop(item);
#else
  	bool r = pool[thd_id]->pop(item);
#endif
	if(!r) {
		DEBUG_M("query_pool alloc\n");
#if WORKLOAD==TPCC
		QryTPCC * qry = (QryTPCC *) alloc_memory.alloc(sizeof(QryTPCC));
		qry = new QryTPCC();
#elif WORKLOAD==PPS
		PPSQry * qry = (PPSQry *) alloc_memory.alloc(sizeof(PPSQry));
		qry = new PPSQry();
#elif WORKLOAD==YCSB
		QryYCSB * qry = NULL;
		qry = (QryYCSB *) alloc_memory.alloc(sizeof(QryYCSB));
		qry = new QryYCSB();
#elif WORKLOAD==TEST
		QryTPCC * qry = (QryTPCC *) alloc_memory.alloc(sizeof(QryTPCC));
		qry = new QryTPCC();
#elif WORKLOAD==DA
		DAQuery * qry = NULL;
		qry = (DAQuery *) alloc_memory.alloc(sizeof(DAQuery));
		qry = new DAQuery();
#endif
		qry->init();
		item = (BaseQry*)qry;
	}
	DEBUG_R("get 0x%lx\n",(uint64_t)item);
}

void QryPool::put(uint64_t thd_id, BaseQry * item) {
  assert(item);
#if WORKLOAD == YCSB
  ((QryYCSB*)item)->reset();
#elif WORKLOAD == TPCC
  ((QryTPCC*)item)->reset();
#elif WORKLOAD == PPS
  ((PPSQry*)item)->reset();
#elif WORKLOAD == TEST
  ((QryTPCC*)item)->reset();
#endif
  //DEBUG_M("put 0x%lx\n",(uint64_t)item);
  DEBUG_R("put 0x%lx\n",(uint64_t)item);
  //alloc_memory.free(item,sizeof(item));
  int try_times = 0;
#if ALGO == CALVIN
  while(!pool->push(item) && try_times++ < TRY_LIMIT) { }
#else
  while(!pool[thd_id]->push(item) && try_times++ < TRY_LIMIT) { }
#endif
  if(try_times >= TRY_LIMIT) {
#if WORKLOAD == YCSB
  ((QryYCSB*)item)->release();
#elif WORKLOAD == TPCC
  ((QryTPCC*)item)->release();
#elif WORKLOAD == PPS
  ((PPSQry*)item)->release();
#elif WORKLOAD == TEST
  ((QryTPCC*)item)->release();
#endif
	alloc_memory.free(item,sizeof(BaseQry));
  }
}

void QryPool::free_pool() {
  BaseQry * item;
  DEBUG_M("query_pool free\n");
	for(uint64_t thd_id = 0; thd_id < g_thread_total_cnt; thd_id++) {
#if ALGO == CALVIN
  while(pool->pop(item)) {
#else
  while(pool[thd_id]->pop(item)) {
#endif
	alloc_memory.free(item,sizeof(item));
  }
	}
}


void AccessPool::init(WLSchema * wl, uint64_t size) {
  _wl = wl;
  pool = new boost::lockfree::queue<Access* > * [g_thread_total_cnt];
  DEBUG_M("AccessPool alloc init\n");
  for(uint64_t thd_id = 0; thd_id < g_thread_total_cnt; thd_id++) {
	pool[thd_id] = new boost::lockfree::queue<Access* > (size);
	for(uint64_t i = 0; i < size; i++) {
	Access * item = (Access*)alloc_memory.alloc(sizeof(Access));
	put(thd_id,item);
	}
  }
}

void AccessPool::get(uint64_t thd_id, Access *& item) {
  //bool r = pool->pop(item);
  bool r = pool[thd_id]->pop(item);
  if(!r) {
	DEBUG_M("acc_pool alloc\n");
	item = (Access*)alloc_memory.alloc(sizeof(Access));
  }
}

void AccessPool::put(uint64_t thd_id, Access * item) {
  pool[thd_id]->push(item);
  /*
  int try_times = 0;
  while(!pool->push(item) && try_times++ < TRY_LIMIT) { }
  if(try_times >= TRY_LIMIT) {
	alloc_memory.free(item,sizeof(Access));
  }
  */
}

void AccessPool::free_pool() {
  Access * item;
  DEBUG_M("acc_pool free\n");
  //while(pool->pop(item)) {
	for(uint64_t thd_id = 0; thd_id < g_thread_total_cnt; thd_id++) {
  while(pool[thd_id]->pop(item)) {
	alloc_memory.free(item,sizeof(item));
  }
  }
}

void TxnTablePool::init(WLSchema * wl, uint64_t size) {
  _wl = wl;
  pool = new boost::lockfree::queue<txn_node* > * [g_thread_total_cnt];
  DEBUG_M("TxnTablePool alloc init\n");
  for(uint64_t thd_id = 0; thd_id < g_thread_total_cnt; thd_id++) {
	pool[thd_id] = new boost::lockfree::queue<txn_node* > (size);
	for(uint64_t i = 0; i < size; i++) {
	  txn_node * t_node = (txn_node *) alloc_memory.align_alloc(sizeof(struct txn_node));
	  //put(new txn_node());
	  put(thd_id,t_node);
	}
  }
}

void TxnTablePool::get(uint64_t thd_id, txn_node *& item) {
  bool r = pool[thd_id]->pop(item);
  if(!r) {
	DEBUG_M("tbl_txn_pool alloc\n");
	item = (txn_node *) alloc_memory.align_alloc(sizeof(struct txn_node));
  }
}

void TxnTablePool::put(uint64_t thd_id, txn_node * item) {
  int try_times = 0;
  while(!pool[thd_id]->push(item) && try_times++ < TRY_LIMIT) { }
  if(try_times >= TRY_LIMIT) {
	alloc_memory.free(item,sizeof(txn_node));
  }
}

void TxnTablePool::free_pool() {
  txn_node * item;
  DEBUG_M("tbl_txn_pool free\n");
  for(uint64_t thd_id = 0; thd_id < g_thread_total_cnt; thd_id++) {
	while(pool[thd_id]->pop(item)) {
	  alloc_memory.free(item,sizeof(item));
	}
  }
}
void MsgPool::init(WLSchema * wl, uint64_t size) {
  _wl = wl;
  pool = new boost::lockfree::queue<entry_message* > (size);
  entry_message* entry;
  DEBUG_M("MsgPool alloc init\n");
  for(uint64_t i = 0; i < size; i++) {
	entry = (entry_message*) alloc_memory.alloc(sizeof(struct entry_message));
	put(entry);
  }
}

void MsgPool::get(entry_message* & item) {
  bool r = pool->pop(item);
  if(!r) {
	DEBUG_M("message_pool alloc\n");
	item = (entry_message*) alloc_memory.alloc(sizeof(struct entry_message));
  }
}

void MsgPool::put(entry_message* item) {
  item->message = NULL;
  item->dest = UINT64_MAX;
  item->begintime = UINT64_MAX;
  int try_times = 0;
  while(!pool->push(item) && try_times++ < TRY_LIMIT) { }
  if(try_times >= TRY_LIMIT) {
	alloc_memory.free(item,sizeof(entry_message));
  }
}

void MsgPool::free_pool() {
  entry_message * item;
  DEBUG_M("free message_pool\n");
  while(pool->pop(item)) {
	alloc_memory.free(item,sizeof(item));
  }
}

void RowPool::init(WLSchema * wl, uint64_t size) {
  _wl = wl;
  pool = new boost::lockfree::queue<RowData*> * [g_thread_total_cnt];
  RowData* entry;
  DEBUG_M("init RowPool alloc\n");
  for(uint64_t thd_id = 0; thd_id < g_thread_total_cnt; thd_id++) {
	pool[thd_id] = new boost::lockfree::queue<RowData* > (size);
	for(uint64_t i = 0; i < size; i++) {
	entry = (RowData*) alloc_memory.alloc(sizeof(struct RowData));
	put(thd_id,entry);
	}
  }
}

void RowPool::get(uint64_t thd_id, RowData* & item) {
  bool r = pool[thd_id]->pop(item);
  if(!r) {
	DEBUG_M("alloc message_pool\n");
	item = (RowData*) alloc_memory.alloc(sizeof(struct RowData));
  }
}

void RowPool::put(uint64_t thd_id, RowData* item) {
  int try_times = 0;
  while(!pool[thd_id]->push(item) && try_times++ < TRY_LIMIT) { }
  if(try_times >= TRY_LIMIT) {
	alloc_memory.free(item,sizeof(RowData));
  }
}

void RowPool::free_pool() {
  RowData * item;
  for(uint64_t thd_id = 0; thd_id < g_thread_total_cnt; thd_id++) {
  while(pool[thd_id]->pop(item)) {
	DEBUG_M("free row_pool\n");
	alloc_memory.free(item,sizeof(RowData));
  }
  }
}

