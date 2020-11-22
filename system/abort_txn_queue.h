
#ifndef _ABORTTXN_QUEUE_H_
#define _ABORTTXN_QUEUE_H_

#include "global.h"
#include "universal.h"

struct abort_txn_entry {
  uint64_t penalty_end;
  uint64_t txn_id;
  abort_txn_entry() {
  }
  abort_txn_entry(uint64_t penalty_end, uint64_t txn_id) {
    this->penalty_end = penalty_end;
    this->txn_id = txn_id;
  }
};


struct CompareAbortTxnEntry {
  bool operator()(const abort_txn_entry* lhs, const abort_txn_entry* rhs) {
    return lhs->penalty_end > rhs->penalty_end;
    //return lhs->penalty_end < rhs->penalty_end;
  }
};

class AbortTxnQueue {
public:
  void init();
  uint64_t enqueue(uint64_t thd_id, uint64_t txn_id, uint64_t abort_cnt);
  void process(uint64_t thd_id);
private:
  std::priority_queue<abort_txn_entry*,std::vector<abort_txn_entry*>,CompareAbortTxnEntry> queue;
  pthread_mutex_t mtx;
};



#endif
