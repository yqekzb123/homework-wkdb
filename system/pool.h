
#ifndef _TXN_POOL_H_
#define _TXN_POOL_H_


#include "global.h"
#include "universal.h"
#include <boost/lockfree/queue.hpp>
#include "concurrentqueue.h"

class TxnMgr;
class BaseQry;
class WLSchema;
struct entry_message;
struct txn_node;
class Access;
class Txn;
class RowData;


class TxnManPool {
public:
  void init(WLSchema * wl, uint64_t size);
  void get(uint64_t thd_id, TxnMgr *& item);
  void put(uint64_t thd_id, TxnMgr * items);
  void free_pool();

private:
#if ALGO == CALVIN
  boost::lockfree::queue<TxnMgr*> * pool;
#else
  boost::lockfree::queue<TxnMgr*> ** pool;
#endif
  WLSchema * _wl;

};


class TxnPool {
public:
  void init(WLSchema * wl, uint64_t size);
  void get(uint64_t thd_id, Txn *& item);
  void put(uint64_t thd_id,Txn * items);
  void free_pool();

private:
#if ALGO == CALVIN
  boost::lockfree::queue<Txn*> * pool;
#else
  boost::lockfree::queue<Txn*> ** pool;
#endif
  WLSchema * _wl;

};

class QryPool {
public:
  void init(WLSchema * wl, uint64_t size);
  void get(uint64_t thd_id, BaseQry *& item);
  void put(uint64_t thd_id, BaseQry * items);
  void free_pool();

private:
#if ALGO == CALVIN
  boost::lockfree::queue<BaseQry* > * pool;
#else
  boost::lockfree::queue<BaseQry* > ** pool;
#endif
  WLSchema * _wl;

};


class AccessPool {
public:
  void init(WLSchema * wl, uint64_t size);
  void get(uint64_t thd_id, Access *& item);
  void put(uint64_t thd_id, Access * items);
  void free_pool();

private:
  boost::lockfree::queue<Access* > ** pool;
  WLSchema * _wl;

};


class TxnTablePool {
public:
  void init(WLSchema * wl, uint64_t size);
  void get(uint64_t thd_id, txn_node *& item);
  void put(uint64_t thd_id, txn_node * items);
  void free_pool();

private:
  boost::lockfree::queue<txn_node*> ** pool;
  WLSchema * _wl;

};
class MsgPool {
public:
  void init(WLSchema * wl, uint64_t size);
  void get(entry_message *& item);
  void put(entry_message * items);
  void free_pool();

private:
  boost::lockfree::queue<entry_message* > * pool;
  WLSchema * _wl;

};

class RowPool {
public:
  void init(WLSchema * wl, uint64_t size);
  void get(uint64_t thd_id, RowData *& item);
  void put(uint64_t thd_id, RowData * items);
  void free_pool();

private:
  boost::lockfree::queue<RowData* > ** pool;
  WLSchema * _wl;

};


#endif
