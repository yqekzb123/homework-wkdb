
#ifndef _OCCTEMPLATE_H_
#define _OCCTEMPLATE_H_

#include "row.h"
#include "semaphore.h"


class TxnMgr;

enum OCCTEMPLATEState { OCCTEMPLATE_RUNNING=0,OCCTEMPLATE_VALIDATED,OCCTEMPLATE_COMMITTED,OCCTEMPLATE_ABORTED};

class Occ_template {
public:
  void init();
  RC validate(TxnMgr * txn);
  RC find_bound(TxnMgr * txn); 
private:
 	sem_t 	_semaphore;
};

struct TimestampBoundsEntry{
  uint64_t lower;
  uint64_t upper;
  uint64_t key;
  OCCTEMPLATEState state;
  TimestampBoundsEntry * next;
  TimestampBoundsEntry * prev;
  void init(uint64_t key) {
    lower = 0;
    upper = UINT64_MAX;
    this->key = key;
    state = OCCTEMPLATE_RUNNING;
    next = NULL;
    prev = NULL;
  }
};

struct TimestampBoundsNode {
  TimestampBoundsEntry * head;
  TimestampBoundsEntry * tail;
  pthread_mutex_t mtx;
  void init() {
    head = NULL;
    tail = NULL;
    pthread_mutex_init(&mtx,NULL);
  }
};

class TimestampBounds {
public:
	void init();
	void init(uint64_t thd_id, uint64_t key);
	void release(uint64_t thd_id, uint64_t key);
  uint64_t get_lower(uint64_t thd_id, uint64_t key);
  uint64_t get_upper(uint64_t thd_id, uint64_t key);
  void set_lower(uint64_t thd_id, uint64_t key, uint64_t value);
  void set_upper(uint64_t thd_id, uint64_t key, uint64_t value);
  OCCTEMPLATEState get_state(uint64_t thd_id, uint64_t key);
  void set_state(uint64_t thd_id, uint64_t key, OCCTEMPLATEState value);
private:
  // hash table
  uint64_t hash(uint64_t key);
  uint64_t table_size;
  TimestampBoundsNode* table;
  TimestampBoundsEntry* find(uint64_t key);

  TimestampBoundsEntry * find_entry(uint64_t id);
	
 	sem_t 	_semaphore;
};

#endif
