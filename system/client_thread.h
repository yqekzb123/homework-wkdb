
#ifndef _CLIENT_THREAD_H_
#define _CLIENT_THREAD_H_

#include "global.h"

class WLSchema;

class ClientThread : public Thread {
public:
	RC 			run();
  void setup();
private:
  uint64_t last_send_time;
  uint64_t send_interval;
};

#endif
