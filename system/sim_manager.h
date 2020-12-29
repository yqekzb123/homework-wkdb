
#ifndef _SIMMAN_H_
#define _SIMMAN_H_

#include "global.h"

class Simulator
{
public:
	volatile bool sim_init_done;
	volatile bool warmup;
  volatile uint64_t warmup_endtime;
	bool start_set;
	volatile bool sim_done;
  uint64_t run_begintime;
  uint64_t response_cnt;
  uint64_t seq_epoch;
  uint64_t worker_epoch;
  uint64_t last_worker_epoch_time;
  uint64_t last_seq_epoch_time;
  int64_t epoch_txn_cnt;
  uint64_t txn_cnt;
  uint64_t inflight_cnt;
  uint64_t last_da_query_time;
  void init();
  bool is_setup_done();
  bool is_done();
  bool is_warmup_done();
  void set_setup_done();
  void set_done();
  bool timeout();
  void set_begintime(uint64_t begintime);
  void process_setup_msg();
  void inc_txn_cnt(); 
  void inc_inflight_cnt(); 
  void dec_inflight_cnt(); 
  uint64_t get_worker_epoch(); 
  void next_worker_epoch(); 
  uint64_t get_seq_epoch(); 
  void advance_seq_epoch(); 
  void inc_epoch_txn_cnt(); 
  void decr_epoch_txn_cnt(); 
  double seconds_from_begin(uint64_t time);
};

#endif
