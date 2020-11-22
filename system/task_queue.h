
#ifndef _TASK_QUEUE_H_
#define _TASK_QUEUE_H_


#include "global.h"
#include "universal.h"
#include <queue>
#include <boost/lockfree/queue.hpp>
//#include "message.h"

class BaseQry;
class WLSchema;
class Msg;

struct task_quequ_start {
  Msg * message;
  uint64_t batch_id;
  uint64_t txn_id;
  RemReqType rtype;
  uint64_t begintime;

};


struct CompSchedEntry { //CompareSchedEntry
  bool operator()(const task_quequ_start* lhs, const task_quequ_start* rhs) {
    if(lhs->batch_id == rhs->batch_id)
      return lhs->begintime > rhs->begintime;
    return lhs->batch_id < rhs->batch_id;
  }
};
struct CompareWQEntry {
#if PRIORITY == PRIORITY_FCFS
  bool operator()(const task_quequ_start* lhs, const task_quequ_start* rhs) {
    return lhs->begintime < rhs->begintime;
  }
#elif PRIORITY == PRIORITY_ACTIVE
  bool operator()(const task_quequ_start* lhs, const task_quequ_start* rhs) {
    if(lhs->rtype == WKCQRY && rhs->rtype != WKCQRY)
      return true;
    if(rhs->rtype == WKCQRY && lhs->rtype != WKCQRY)
      return false;
    return lhs->begintime < rhs->begintime;
  }
#elif PRIORITY == PRIORITY_HOME
  bool operator()(const task_quequ_start* lhs, const task_quequ_start* rhs) {
    if(ISLOCAL(lhs->txn_id) && !ISLOCAL(rhs->txn_id))
      return true;
    if(ISLOCAL(rhs->txn_id) && !ISLOCAL(lhs->txn_id))
      return false;
    return lhs->begintime < rhs->begintime;
  }
#endif

};

class TaskQueue {
public:
  void init();
  void enqueue(uint64_t thd_id,Msg * message,bool busy); 
  Msg * dequeue(uint64_t thd_id);
  void sched_enqueue(uint64_t thd_id, Msg * message); 
  Msg * sched_dequeue(uint64_t thd_id); 
  void sequencer_enqueue(uint64_t thd_id, Msg * message); 
  Msg * sequencer_dequeue(uint64_t thd_id); 

  uint64_t get_cnt() {return get_wq_cnt() + get_rem_wq_cnt() + get_new_wq_cnt();}
  uint64_t get_wq_cnt() {return 0;}
  //uint64_t get_wq_cnt() {return task_queue.size();}
  uint64_t get_sched_wq_cnt() {return 0;}
  uint64_t get_rem_wq_cnt() {return 0;} 
  uint64_t get_new_wq_cnt() {return 0;}
  //uint64_t get_rem_wq_cnt() {return remote_op_queue.size();}
  //uint64_t get_new_wq_cnt() {return new_query_queue.size();}

private:
  boost::lockfree::queue<task_quequ_start* > * task_queue;
  boost::lockfree::queue<task_quequ_start* > * new_txn_queue;
  boost::lockfree::queue<task_quequ_start* > * seq_queue;
  boost::lockfree::queue<task_quequ_start* > ** sched_queue;
  uint64_t sched_ptr;
  BaseQry * last_sched_dq;
  uint64_t curr_epoch;

};


#endif
