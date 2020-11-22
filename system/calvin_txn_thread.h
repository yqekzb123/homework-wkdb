
#ifndef _CALVINTHREAD_H_
#define _CALVINTHREAD_H_

#include "global.h"

class WLSchema;

/*
class CalvinThread : public Thread {
public:
	RC 			run();
  void setup();
  uint64_t txn_starttime;
private:
	TxnMgr * m_txn;
};
*/

class CalvinscheduleThread : public Thread {
public:
    RC run();
    void setup();
private:
    TxnMgr * m_txn;
};

class CalvinSequenceThread : public Thread {
public:
    RC run();
    void setup();
private:
    bool is_batch_ready();
	uint64_t last_batchtime;
};

#endif
