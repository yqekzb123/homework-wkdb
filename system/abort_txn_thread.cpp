
#include "global.h"
#include "universal.h"
#include "thread.h"
#include "abort_txn_thread.h"
#include "abort_txn_queue.h"

void AbortTxnThread::setup() {
}

RC AbortTxnThread::run() {
  tsetup();
  printf("Running AbortTxnThread %ld\n",_thd_id);
	while (!simulate_man->is_done()) {
    heartbeat();
    abort_queue.process(read_thd_id());
  }
  return FINISH;
 
}


