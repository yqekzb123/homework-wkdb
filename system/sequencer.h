
#ifndef _SEQUENCER_H_
#define _SEQUENCER_H_

#include "global.h"
#include "query.h"
#include <boost/lockfree/queue.hpp>

class WLSchema;
class BaseQry;
class Msg;

typedef struct qlite_entry {
  BaseQry * qry;
	uint32_t client_id;
	uint64_t client_start_ts;
	uint64_t seq_start_ts;
	uint64_t seq_start_ts_first;
	uint64_t skew_start_ts;
	uint64_t total_batch_time;
	uint32_t server_ack_cnt;
	uint32_t abort_cnt;
  Msg * message;
} qlite;

typedef struct qlite_ll_entry {
  qlite * list;
	uint64_t size;
	uint32_t max_size;
	uint32_t left_txns;
	uint64_t epoch;
	uint64_t batch_time_send;
  qlite_ll_entry * next;
  qlite_ll_entry * prev;
} qlite_ll;


class Sequencer {
 public:
	void init(WLSchema * wl);	
	void process_ack(Msg * message, uint64_t thd_id);
	void process_txn(Msg * message,uint64_t thd_id, uint64_t early_start, uint64_t last_start, uint64_t wait_time, uint32_t abort_cnt);
	void send_batch_next(uint64_t thd_id);

 private:
	void reset_participater_nodes(bool * part_nodes);

  boost::lockfree::queue<Msg*, boost::lockfree::capacity<65526> > * fill_queue;
#if WORKLOAD == YCSB
	QryYCSB* node_queries;
#elif WORKLOAD == TPCC
	QryTPCC* node_queries;
#elif WORKLOAD == PPS
	PPSQry* node_queries;
#endif
	volatile uint64_t total_finished_txns;
	volatile uint64_t total_received_txns;
	volatile uint32_t response_cnt;
  uint64_t last_batch;
	qlite_ll * workload_head;		// list of txns in batch being executed
	qlite_ll * workload_tail;		// list of txns in batch being executed
	volatile uint32_t next_txn_id;
	WLSchema * _wl;
};

class Seq_thread_t {
public:
	uint64_t _thd_id;
	uint64_t _node_id;
	WLSchema * _wl;

	uint64_t 	read_thd_id();
	uint64_t 	get_node_id();


	void 		init(uint64_t thd_id, uint64_t node_id, WLSchema * workload);
	// the following function must be in the form void* (*)(void*)
	// to run with pthread.
	// conversion is done within the function.
	RC 			run_remote();
	RC 			run_recv();
	RC 			run_send();
};
#endif
