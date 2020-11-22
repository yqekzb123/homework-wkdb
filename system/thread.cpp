
#include "global.h"
#include "manager.h"
#include "thread.h"
#include "txn.h"
#include "workload.h"
#include "query.h"
#include "math.h"
#include "universal.h"
#include "msg_queue.h"
#include "message.h"

void Thread::heartbeat() {
  /*
#if TIME_ENABLE
  uint64_t now_time = acquire_ts();
#else
  uint64_t now_time = get_clock_wall();
#endif
  if (now_time - heartbeat_time >= g_prog_timer) {
    printf("Heartbeat %ld %f\n",_thd_id,simulate_man->seconds_from_begin(now_time));
    heartbeat_time = now_time;
  }
  */

}

void Thread::send_init_done_to_nodes() {
		for(uint64_t i = 0; i < g_node_total_cnt; i++) {
			if(i != g_node_id) {
        printf("Send WKINIT_DONE to %ld\n",i);
        msg_queue.enqueue(read_thd_id(),Msg::create_message(WKINIT_DONE),i);
			}
		}
}

void Thread::init(uint64_t thd_id, uint64_t node_id, WLSchema * workload) {
	_thd_id = thd_id;
	_node_id = node_id;
	_wl = workload;
	rdm.init(_thd_id);
}

uint64_t Thread::read_thd_id() { return _thd_id; }
uint64_t Thread::get_node_id() { return _node_id; }

void Thread::tsetup() {
	printf("Setup %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	pthread_barrier_wait( &warmup_bar );

  setup();

	printf("Running %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	pthread_barrier_wait( &warmup_bar );

#if TIME_ENABLE
  run_begintime = acquire_ts();
#else
  run_begintime = get_clock_wall();
#endif
  simulate_man->set_begintime(run_begintime);
  prog_time = run_begintime;
  heartbeat_time = run_begintime;
	pthread_barrier_wait( &warmup_bar );


}

void Thread::progress_stats() {
		if(read_thd_id() == 0) {
#if TIME_ENABLE
      uint64_t now_time = acquire_ts();
#else
      uint64_t now_time = get_clock_wall();
#endif
      if (now_time - prog_time >= g_prog_timer) {
        prog_time = now_time;
        SET_STATS(read_thd_id(), total_runtime, prog_time - simulate_man->run_begintime); 

        if(ISCLIENT) {
          stats.print_client(true);
        } else {
          stats.print(true);
        }
      }
		}

}
