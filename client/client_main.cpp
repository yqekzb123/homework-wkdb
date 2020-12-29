#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "da.h"
#include "test.h"
#include "thread.h"
#include "io_thread.h"
#include "client_thread.h"
#include "client_query.h"
#include "transport.h"
#include "client_txn.h"
#include "msg_queue.h"
#include "task_queue.h"
//#include <jemallloc.h>

void * f(void *);
void * g(void *);
void * worker(void *);
void * nn_worker(void *);
void * send_worker(void *);
void network_test();
void network_recv();
void * work_thread(void *);

ClientThread * client_thds;
InputThread * input_thds;
OutputThread * output_thds;

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
    printf("Running client...\n\n");
	// 0. initialize global data structure
	parser(argc, argv);
    assert(g_node_id >= g_cnt_node);
    //assert(g_cl_node_cnt <= g_cnt_node);

	uint64_t rand_seed = acquire_ts();
	srand(rand_seed);
	printf("Random rand_seed: %ld\n",rand_seed);


	int64_t begintime;
	int64_t endtime;
    begintime = get_serverdb_clock();
	// per-partition malloc
  printf("Initializing stats... ");
  fflush(stdout);
	stats.init(g_total_client_thread_cnt);
  printf("Done\n");
  printf("Initializing tport manager... ");
  fflush(stdout);
	tport_manager.init(); //transport manager
  printf("Done\n");
  printf("Initializing client manager... ");
	WLSchema * m_workload;
	switch (WORKLOAD) {
		case YCSB :
			m_workload = new WLYCSB; break;
		case TPCC :
			m_workload = new WLTPCC; break;
		case TEST :
			m_workload = new WLCTEST; break;
		case DA :
			m_workload = new DAWorkload; break;
		default:
			assert(false);
	}
	m_workload->WLSchema::init();
	printf("workload initialized!\n");

  printf("Initializing simulate_man... ");
  fflush(stdout);
  simulate_man = new Simulator;
  simulate_man->init();
  printf("Done\n");
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
  client_man.init();
  printf("Done\n");
  printf("Initializing work queue... ");
  fflush(stdout);
  task_queue.init();
  printf("Done\n");
  printf("Initializing message pool... ");
  fflush(stdout);
  message_pool.init(m_workload,g_max_inflight);
  printf("Done\n");
  fflush(stdout);

	// 2. spawn multiple threads
	uint64_t thd_cnt = g_cl_thd_cnt;
	uint64_t cthd_cnt = thd_cnt; 
	uint64_t r_thd_cnt = g_cl_rem_thd_cnt;
	uint64_t s_thd_cnt = g_cl_send_thd_cnt;
  uint64_t thd_cnt_all = thd_cnt + r_thd_cnt + s_thd_cnt;
  assert(thd_cnt_all == g_this_total_thread_cnt);

	pthread_t * p_threads = 
		(pthread_t *) malloc(sizeof(pthread_t) * (thd_cnt_all));
	pthread_attr_t attrib;
	pthread_attr_init(&attrib);

  client_thds = new ClientThread[cthd_cnt];
  input_thds = new InputThread[r_thd_cnt];
  output_thds = new OutputThread[s_thd_cnt];
	//// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
  printf("Initializing message queue... ");
  msg_queue.init();
  printf("Done\n");
  printf("Initializing query queue of client... ");
  fflush(stdout);
  qry_queue_client.init(m_workload);
  printf("Done\n");
  fflush(stdout);

#if CREATE_TXN_FILE
  return(0);
#endif

  endtime = get_serverdb_clock();
  printf("Initialization Time = %ld\n", endtime - begintime);
  fflush(stdout);
	warmup_done = true;
	pthread_barrier_init( &warmup_bar, NULL, thd_cnt_all);

	uint64_t cpus_cnt = 0;
	cpu_set_t cpus;
	// spawn and run txns again.
	begintime = get_serverdb_clock();
  	simulate_man->run_begintime = begintime;
	simulate_man->last_da_query_time = begintime;
  uint64_t id = 0;
	for (uint64_t i = 0; i < thd_cnt; i++) {
		CPU_ZERO(&cpus);
#if TPORT_TYPE_IPC
        CPU_SET(g_node_id * thd_cnt + cpus_cnt, &cpus);
#elif !SET_AFFINITY
        CPU_SET(g_node_id * thd_cnt + cpus_cnt, &cpus);
#else
        CPU_SET(cpus_cnt, &cpus);
#endif
		cpus_cnt = (cpus_cnt + 1) % g_servers_per_client;
    pthread_attr_setaffinity_np(&attrib, sizeof(cpu_set_t), &cpus);
    client_thds[i].init(id,g_node_id,m_workload);
		pthread_create(&p_threads[id++], &attrib, work_thread, (void *)&client_thds[i]);
    }

	for (uint64_t j = 0; j < r_thd_cnt ; j++) {
    input_thds[j].init(id,g_node_id,m_workload);
		pthread_create(&p_threads[id++], NULL, work_thread, (void *)&input_thds[j]);
  }

	for (uint64_t i = 0; i < s_thd_cnt; i++) {
    output_thds[i].init(id,g_node_id,m_workload);
		pthread_create(&p_threads[id++], NULL, work_thread, (void *)&output_thds[i]);
  }
	for (uint64_t i = 0; i < thd_cnt_all; i++) 
		pthread_join(p_threads[i], NULL);

	endtime = get_serverdb_clock();
	
  fflush(stdout);
  printf("CLIENT PASS! SimTime = %ld\n", endtime - begintime);
  if (STATS_ENABLE)
    stats.print_client(false);
  fflush(stdout);
	return 0;
}

void * work_thread(void * id) {
  Thread * thd = (Thread *) id;
	thd->run();
	return NULL;
}

void network_test() {
  /*

	ts_t start;
	ts_t end;
	double time;
	int bytes;
	for(int i=4; i < 257; i+=4) {
		time = 0;
		for(int j=0;j < 1000; j++) {
			start = acquire_ts();
			tport_manager.simple_send_msg(i);
			while((bytes = tport_manager.simple_recv_msg()) == 0) {}
			end = acquire_ts();
			assert(bytes == i);
			time += end-start;
		}
		time = time/1000;
		printf("Network Bytes: %d, s: %f\n",i,time/BILLION);
        fflush(stdout);
	}
  */
}

void network_recv() {
  /*
	int bytes;
	while(1) {
		if( (bytes = tport_manager.simple_recv_msg()) > 0)
			tport_manager.simple_send_msg(bytes);
	}
  */
}
