
#include "global.h"
#include "universal.h"
#include "sim_manager.h"

void Simulator::init() {
	sim_done = false;
  warmup = false;
  warmup_endtime = 0;
	start_set = false;
	sim_init_done = false;
  txn_cnt = 0;
  inflight_cnt = 0;
  epoch_txn_cnt = 0;
  worker_epoch = 1;
  seq_epoch = 0;
  response_cnt = g_node_total_cnt - 1;

#if TIME_ENABLE
  run_begintime = acquire_ts();
  last_da_query_time = acquire_ts();
#else
  run_begintime = get_clock_wall();
#endif
  last_worker_epoch_time = run_begintime;
  last_seq_epoch_time = get_clock_wall();
}


void Simulator::set_begintime(uint64_t begintime) {
    if(ATOM_CAS(start_set, false, true)) {
      run_begintime = begintime;
      last_worker_epoch_time = begintime;
      last_da_query_time = begintime;
      sim_done = false;
      printf("Starttime set to %ld\n",run_begintime);
    } 
}

bool Simulator::timeout() {
#if TIME_ENABLE
	#if WORKLOAD == DA
		uint64_t t=last_da_query_time;
		uint64_t now=acquire_ts();
		if(now<t)
		{
			now=t;
		}
		bool res =  ((acquire_ts() - run_begintime) >= (g_timer_done + g_warmup_timer)/12)
		&&((now - t) >= (g_timer_done + g_warmup_timer)/6);
		if (res) {
			printf("123\n");
		}
		return res;
	#else
  return (acquire_ts() - run_begintime) >= g_timer_done + g_warmup_timer;
  #endif
#else
  return (get_clock_wall() - run_begintime) >= g_timer_done + g_warmup_timer;
#endif
}

bool Simulator::is_done() {
  bool done = sim_done || timeout();
  if(done && !sim_done) {
    set_done();
  }
  return done;
}

bool Simulator::is_warmup_done() {
	#if WORKLOAD == DA
		return true;
	#endif
  if(warmup)
    return true;
  bool done = ((acquire_ts() - run_begintime) >= g_warmup_timer);
  if(done) {
    ATOM_CAS(warmup_endtime,0,acquire_ts());
    ATOM_CAS(warmup,false,true);
  }
  return done;
}
bool Simulator::is_setup_done() {
  return sim_init_done;
}

void Simulator::set_setup_done() {
    ATOM_CAS(sim_init_done, false, true);
}

void Simulator::set_done() {
    if(ATOM_CAS(sim_done, false, true)) {
      if(warmup_endtime == 0)
        warmup_endtime = run_begintime;
      SET_STATS(0, total_runtime, acquire_ts() - warmup_endtime); 
    }
}

void Simulator::process_setup_msg() {
  uint64_t rsp_left = ATOM_SUB_FETCH(response_cnt,1);
  if(rsp_left == 0) {
    set_setup_done();
  }
}

void Simulator::inc_txn_cnt() {
  ATOM_ADD(txn_cnt,1);
}

void Simulator::inc_inflight_cnt() {
  ATOM_ADD(inflight_cnt,1);
}

void Simulator::dec_inflight_cnt() {
  ATOM_SUB(inflight_cnt,1);
}

void Simulator::inc_epoch_txn_cnt() {
  ATOM_ADD(epoch_txn_cnt,1);
}

void Simulator::decr_epoch_txn_cnt() {
  ATOM_SUB(epoch_txn_cnt,1);
}

uint64_t Simulator::get_seq_epoch() {
  return seq_epoch;
}

void Simulator::advance_seq_epoch() {
  ATOM_ADD(seq_epoch,1);
  last_seq_epoch_time += g_limit_seq_batch_time;
}

uint64_t Simulator::get_worker_epoch() {
  return worker_epoch;
}

void Simulator::next_worker_epoch() {
  last_worker_epoch_time = acquire_ts();
  ATOM_ADD(worker_epoch,1);
}

double Simulator::seconds_from_begin(uint64_t time) {
  return (double)(time - run_begintime) / BILLION;
}
