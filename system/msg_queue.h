
#ifndef _MSG_QUEUE_H_
#define _MSG_QUEUE_H_

#include "global.h"
#include "universal.h"
#include "concurrentqueue.h"
#include "lock_free_queue.h"
#include <boost/lockfree/queue.hpp>

class BaseQry;
class Msg;

struct entry_message {
  Msg * message;
  uint64_t dest;
  uint64_t begintime;
};

typedef entry_message * msg_entry_t;

class MsgQueue {
public:
  void init();
  void enqueue(uint64_t thd_id, Msg * message, uint64_t dest);
  uint64_t dequeue(uint64_t thd_id, Msg *& message);
private:
 //LockfreeQueue m_queue;
// This is close to max capacity for boost
#if NETWORK_DELAY_TEST
  boost::lockfree::queue<entry_message*> ** cl_m_queue;
#endif
  boost::lockfree::queue<entry_message*> ** m_queue;
  std::vector<entry_message*> sthd_cache;
  uint64_t ** ctr;

};

#endif
