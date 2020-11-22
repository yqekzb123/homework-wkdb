
#ifndef _AbortTxnThread_H_
#define _AbortTxnThread_H_

#include "global.h"

class WLSchema;

class AbortTxnThread : public Thread {
public:
	RC 			run();
  void setup();
};

#endif
