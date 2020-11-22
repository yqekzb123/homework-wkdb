
#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "test.h"
#include "thread.h"
#include "task_thread.h"
#include "calvin_txn_thread.h"
#include "abort_txn_thread.h"
#include "io_thread.h"
#include "log_thread.h"
#include "manager.h"
#include "math.h"
#include "query.h"
// #include "occ.h"
#include "transport.h"
#include "msg_queue.h"
#include "qry_ycsb.h"
#include "sequencer.h"
#include "logMan.h"
#include "sim_manager.h"
#include "abort_txn_queue.h"
#include "task_queue.h"
#include "occ_template.h"
#include "client_query.h"

void network_test();
void network_recv();
void * work_thread(void *);


TaskThread * work_thds;
InputThread * input_thds;
OutputThread * output_thds;
AbortTxnThread * abort_thds;
LogThread * logger_thds;
#if ALGO == CALVIN
CalvinscheduleThread * calvin_sched_thds;
CalvinSequenceThread * calvin_seq_thds;
#endif

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
	// 0. initialize global data structure
	parser(argc, argv);
#if SEED != 0
  uint64_t rand_seed = SEED + g_node_id;
#else
	uint64_t rand_seed = acquire_ts();
#endif
	srand(rand_seed);
	printf("Random rand_seed: %ld\n",rand_seed);

	int64_t begintime;
	int64_t endtime;
  int64_t exectime;
  begintime = get_serverdb_clock();
  printf("Initializing stats... ");
  fflush(stdout);
	stats.init(g_thread_total_cnt);
  printf("Done\n");
  printf("Initializing glob manager... "); //global manager
  fflush(stdout);
	global_manager.init();
  printf("Done\n");
  printf("Initializing tport manager... "); //transport manager
  fflush(stdout);
	tport_manager.init();
  printf("Done\n");
  fflush(stdout);
  printf("Initializing simulate_man... ");
  fflush(stdout);
  simulate_man = new Simulator;
  simulate_man->init();
  printf("Done\n");
  fflush(stdout);
	WLSchema * m_workload;
	switch (WORKLOAD) {
		case YCSB :
			m_workload = new WLYCSB; break;
		case TPCC :
			m_workload = new WLTPCC; break;
    case TEST :
			m_workload = new WLCTEST; break;
		default:
			assert(false);
	}
	m_workload->init();
	printf("WLSchema initialized!\n");
  fflush(stdout);
#if NETWORK_TEST
	tport_manager.init(g_node_id,m_workload);
	sleep(3);
	if(g_node_id == 0)
		network_test();
	else if(g_node_id == 1)
		network_recv();

	return 0;
#endif
  fflush(stdout);
  task_queue.init();
  printf("Task queue Initialized... ");
  abort_queue.init();
  printf("Abort txn queue Initialized... ");
  fflush(stdout);
  msg_queue.init();
  printf("Msg queue Initialized... ");
  fflush(stdout);
  txn_man_pool.init(m_workload,0);
  printf("Txn manager pool Initialized... ");
  fflush(stdout);
  txn_pool.init(m_workload,0);
  printf("Txn pool Initialized... ");
  fflush(stdout);
  row_pool.init(m_workload,0);
  printf("Row pool Initialized... ");
  fflush(stdout);
  acc_pool.init(m_workload,0);
  printf("Access pool Initialized... ");
  fflush(stdout);
  tbl_txn_pool.init(m_workload,0);
  printf("Txn node table pool Initialized... ");
  fflush(stdout);
  query_pool.init(m_workload,0);
  printf("Query pool Initialized... ");
  fflush(stdout);
  message_pool.init(m_workload,0);
  printf("Msg pool Initialized... ");
  fflush(stdout);
  txn_table.init();
  printf("Txn table Initialized... ");
  fflush(stdout);
#if ALGO == CALVIN
  seq_man.init(m_workload);
  printf("Sequencer Initialized... ");
  fflush(stdout);
#endif
#if ALGO == OCCTEMPLATE
  txn_timestamp_bounds.init();
  printf("Time Table Initialized... ");
  fflush(stdout);
	occ_template_man.init();
  printf("occ template manager Initialized... ");
  fflush(stdout);
#endif
#if LOGGING
  logger.init("logfile.log");
  printf("Log manager Initialized... ");
  fflush(stdout);
#endif

#if SERVER_GENERATE_QUERIES
  qry_queue_client.init(m_workload);
  printf("Client query queue Initialized... ");
  fflush(stdout);
#endif

	// 2. spawn multiple threads
	uint64_t thd_cnt = g_thd_cnt;
	uint64_t w_thd_cnt = thd_cnt;
	uint64_t r_thd_cnt = g_rem_thd_cnt;
	uint64_t s_thd_cnt = g_send_thd_cnt;
  uint64_t thd_cnt_all = thd_cnt + r_thd_cnt + s_thd_cnt + g_abort_thread_cnt;
#if LOGGING
  thd_cnt_all += 1; // logger thread
#endif
#if ALGO == CALVIN
  thd_cnt_all += 2; // sequencer + scheduler thread
#endif
    assert(thd_cnt_all == g_this_total_thread_cnt);
	
    pthread_t * p_threads =
    (pthread_t *) malloc(sizeof(pthread_t) * (thd_cnt_all));
    pthread_attr_t attrib;
    pthread_attr_init(&attrib);

    work_thds = new TaskThread[w_thd_cnt];
    input_thds = new InputThread[r_thd_cnt];
    output_thds = new OutputThread[s_thd_cnt];
    abort_thds = new AbortTxnThread[1];
    logger_thds = new LogThread[1];
#if ALGO == CALVIN
    calvin_sched_thds = new CalvinscheduleThread[1];
    calvin_seq_thds = new CalvinSequenceThread[1];
#endif

    endtime = get_serverdb_clock();
    exectime = endtime - begintime;
    printf("Initialization Time = %ld\n", exectime);
    fflush(stdout);
    warmup_done = true;
    pthread_barrier_init( &warmup_bar, NULL, thd_cnt_all);

#if SET_AFFINITY
  uint64_t cpus_cnt = 0;
  cpu_set_t cpus;
#endif
  // spawn and run txns again.
  begintime = get_serverdb_clock();
  simulate_man->run_begintime = begintime;

  uint64_t id = 0;
  for (uint64_t i = 0; i < w_thd_cnt; i++) {
#if SET_AFFINITY
      CPU_ZERO(&cpus);
      CPU_SET(cpus_cnt, &cpus);
      pthread_attr_setaffinity_np(&attrib, sizeof(cpu_set_t), &cpus);
      cpus_cnt++;
#endif
      assert(id >= 0 && id < w_thd_cnt);
      work_thds[i].init(id,g_node_id,m_workload);
      pthread_create(&p_threads[id++], &attrib, work_thread, (void *)&work_thds[i]);
	}
	for (uint64_t j = 0; j < r_thd_cnt ; j++) {
	    assert(id >= w_thd_cnt && id < w_thd_cnt + r_thd_cnt);
	    input_thds[j].init(id,g_node_id,m_workload);
	    pthread_create(&p_threads[id++], NULL, work_thread, (void *)&input_thds[j]);
	}


	for (uint64_t j = 0; j < s_thd_cnt; j++) {
	    assert(id >= w_thd_cnt + r_thd_cnt && id < w_thd_cnt + r_thd_cnt + s_thd_cnt);
	    output_thds[j].init(id,g_node_id,m_workload);
	    pthread_create(&p_threads[id++], NULL, work_thread, (void *)&output_thds[j]);
	  }
#if LOGGING
    logger_thds[0].init(id,g_node_id,m_workload);
    pthread_create(&p_threads[id++], NULL, work_thread, (void *)&logger_thds[0]);
#endif

#if ALGO != CALVIN
  abort_thds[0].init(id,g_node_id,m_workload);
  pthread_create(&p_threads[id++], NULL, work_thread, (void *)&abort_thds[0]);
#endif

#if ALGO == CALVIN
#if SET_AFFINITY
		CPU_ZERO(&cpus);
    CPU_SET(cpus_cnt, &cpus);
    pthread_attr_setaffinity_np(&attrib, sizeof(cpu_set_t), &cpus);
		cpus_cnt++;
#endif

  calvin_sched_thds[0].init(id,g_node_id,m_workload);
  pthread_create(&p_threads[id++], &attrib, work_thread, (void *)&calvin_sched_thds[0]);
#if SET_AFFINITY
		CPU_ZERO(&cpus);
    CPU_SET(cpus_cnt, &cpus);
    pthread_attr_setaffinity_np(&attrib, sizeof(cpu_set_t), &cpus);
		cpus_cnt++;
#endif

  calvin_seq_thds[0].init(id,g_node_id,m_workload);
  pthread_create(&p_threads[id++], &attrib, work_thread, (void *)&calvin_seq_thds[0]);
#endif


	for (uint64_t i = 0; i < thd_cnt_all ; i++) 
		pthread_join(p_threads[i], NULL);

	endtime = get_serverdb_clock();
	
  fflush(stdout);
  printf("PASS! SimTime = %f\n", (float)(endtime - begintime) / BILLION);
  if (STATS_ENABLE)
    stats.print(false);
  //malloc_stats_print(NULL, NULL, NULL);
  printf("\n");
  fflush(stdout);
  // Free things
	//tport_manager.shutdown();
  m_workload->index_delete_all();

	return 0;
}

void * work_thread(void * id) {
    Thread * thd = (Thread *) id;
	thd->run();
	return NULL;
}

void network_test() {
}

void network_recv() {

}
