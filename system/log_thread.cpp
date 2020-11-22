
#include "global.h"
#include "universal.h"
#include "thread.h"
#include "log_thread.h"
#include "logMan.h"

void LogThread::setup() {
}

RC LogThread::run() {
  tsetup();
	while (!simulate_man->is_done()) {
    logger.processRecord(read_thd_id());
    //logger.flushBufferCheck();
  }
  return FINISH;
 
}


