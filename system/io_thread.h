
#ifndef _IOTHREAD_H_
#define _IOTHREAD_H_

#include "global.h"

class WLSchema;
class MsgThread;

class InputThread : public Thread {
public:
	RC 			run();
  RC  client_recv_loop();
  RC  server_recv_loop();
  void  check_for_init_done();
  void setup();
};

class OutputThread : public Thread {
public:
	RC 			run();
  void setup();
  MsgThread * messager;
};

#endif
