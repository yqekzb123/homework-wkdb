
#ifndef _LOGTHREAD_H_
#define _LOGTHREAD_H_

#include "global.h"

class WLSchema;

class LogThread : public Thread {
public:
	RC 			run();
  void setup();
};

#endif
