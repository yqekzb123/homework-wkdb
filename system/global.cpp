
#include "global.h"
#include "mem_alloc.h"
#include "stats.h"
#include "sim_manager.h"
#include "manager.h"
#include "query.h"
#include "client_query.h"
// #include "occ.h"
#include "transport.h"
#include "task_queue.h"
#include "abort_txn_queue.h"
#include "msg_queue.h"
#include "pool.h"
#include "txn_table.h"
#include "client_txn.h"
#include "sequencer.h"
#include "logMan.h"
#include "occ_template.h"

mem_alloc alloc_memory;
Stats stats;
Simulator * simulate_man;
Manager global_manager;
Qry_queue query_queue;
Qry_queue_client qry_queue_client;
// OptCC occ_man;
Occ_template occ_template_man;
Transport tport_manager;
TxnManPool txn_man_pool;
TxnPool txn_pool;
AccessPool acc_pool;
TxnTablePool tbl_txn_pool;
MsgPool message_pool;
RowPool row_pool;
QryPool query_pool;
TxnTable txn_table;
TaskQueue task_queue;
AbortTxnQueue abort_queue;
MsgQueue msg_queue;
Client_txn client_man;
Sequencer seq_man;
LogMan logger;
TimestampBounds txn_timestamp_bounds;

bool volatile warmup_done = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t warmup_bar;

ts_t g_penalty_abort = ABORT_PENALTY;
ts_t g_abort_penalty_max = ABORT_PENALTY_MAX;
bool g_central_man = CENTRAL_MAN;
UInt32 g_alloc_ts = TS_ALLOC;
bool g_key_order = KEY_ORDER;
bool g_alloc_ts_batch = TS_BATCH_ALLOC;
UInt32 g_ts_batch_num = TS_BATCH_NUM;
int32_t g_max_inflight = MAX_TXN_IN_FLIGHT;
//int32_t g_max_inflight = MAX_TXN_IN_FLIGHT/NODE_CNT;
uint64_t g_msg_size = MSG_SIZE_MAX;
int32_t g_per_server_load = LOAD_PER_SERVER;

bool g_hw_migrate = HW_MIGRATE;

volatile UInt64 g_row_id = 0;
bool g_alloc_part = PART_ALLOC;
bool g_pad_mem = MEM_PAD;
UInt32 g_cc_alg = ALGO;
ts_t g_intvl_query = QUERY_INTVL;
UInt32 g_per_txn_part = PART_PER_TXN;
double g_multi_part_perc = PERC_MULTI_PART;
double g_read_txn_perc = 1.0 - TXN_WRITE_PERC;
double g_write_txn_perc = TXN_WRITE_PERC;
double g_read_tup_perc = 1.0 - TUP_WRITE_PERC;
double g_write_tup_perc = TUP_WRITE_PERC;
double g_theta_zipf = ZIPF_THETA;
double g_perc_data = DATA_PERC;
double g_perc_access = ACCESS_PERC;
bool g_prt_distr_lat = PRT_LAT_DISTR;
UInt32 g_node_id = 0;
UInt32 g_cnt_node = NODE_CNT;
UInt32 g_cnt_part = PART_CNT;
UInt32 g_virtual_part_cnt = VIRTUAL_PART_CNT;
UInt32 g_core_cnt = CORE_CNT;

#if ALGO == HSTORE || ALGO == HSTORE_SPEC
UInt32 g_thd_cnt = PART_CNT/NODE_CNT;
#else
UInt32 g_thd_cnt = THREAD_CNT;
#endif
UInt32 g_rem_thd_cnt = REM_THREAD_CNT;
UInt32 g_abort_thread_cnt = 1;
#if LOGGING
UInt32 g_logger_thread_cnt = 1;
#else
UInt32 g_logger_thread_cnt = 0;
#endif
UInt32 g_send_thd_cnt = SEND_THREAD_CNT;
#if ALGO == CALVIN
// sequencer + scheduler thread
UInt32 g_thread_total_cnt = g_thd_cnt + g_rem_thd_cnt + g_send_thd_cnt + g_abort_thread_cnt + g_logger_thread_cnt + 2;
#else
UInt32 g_thread_total_cnt = g_thd_cnt + g_rem_thd_cnt + g_send_thd_cnt + g_abort_thread_cnt + g_logger_thread_cnt;
#endif
UInt32 g_total_client_thread_cnt = g_cl_thd_cnt + g_cl_rem_thd_cnt + g_cl_send_thd_cnt;
UInt32 g_node_total_cnt = g_cnt_node + g_cl_node_cnt + g_cnt_repl*g_cnt_node;
UInt64 g_table_size_synth = SYNTH_TABLE_SIZE;
UInt32 g_per_qry_req = REQ_PER_QUERY;
bool g_ppt_strict = STRICT_PPT == 1;
UInt32 g_per_tuple_field = FIELD_PER_TUPLE;
UInt32 g_init_parallelism = INIT_PARALLELISM;
UInt32 g_cl_node_cnt = CLIENT_NODE_CNT;
UInt32 g_cl_thd_cnt = CLIENT_THREAD_CNT;
UInt32 g_cl_rem_thd_cnt = CLIENT_REM_THREAD_CNT;
UInt32 g_cl_send_thd_cnt = CLIENT_SEND_THREAD_CNT;
UInt32 g_servers_per_client = 0;
UInt32 g_clients_per_server = 0;
UInt32 g_server_start_node = 0;

UInt32 g_this_thread_cnt = ISCLIENT ? g_cl_thd_cnt : g_thd_cnt;
UInt32 g_this_rem_thread_cnt = ISCLIENT ? g_cl_rem_thd_cnt : g_rem_thd_cnt;
UInt32 g_send_thd_cnt_this = ISCLIENT ? g_cl_send_thd_cnt : g_send_thd_cnt;
UInt32 g_this_total_thread_cnt = ISCLIENT ? g_total_client_thread_cnt : g_thread_total_cnt;

UInt32 g_per_part_max_txn = MAX_TXN_PER_PART;
UInt32 g_delay_net = NETWORK_DELAY;
UInt64 g_timer_done = DONE_TIMER;
UInt64 g_limit_batch_time = BATCH_TIMER;
UInt64 g_limit_seq_batch_time = SEQ_BATCH_TIMER;
UInt64 g_prog_timer = PROG_TIMER;
UInt64 g_warmup_timer = WARMUP_TIMER;
UInt64 g_msg_time_limit = MSG_TIME_LIMIT;

UInt64 g_log_buf_max = LOG_BUF_MAX;
UInt64 g_log_flush_timeout = LOG_BUF_TIMEOUT;

// CALVIN
UInt32 g_seq_thread_cnt = SEQ_THREAD_CNT;

double g_mpr = MPR;
double g_mpitem = MPIR;

// TPCC
UInt32 g_wh_num = NUM_WH;
double g_payment_perc = PERC_PAYMENT;
bool g_update_wh = WH_UPDATE;
char * output_file = NULL;
char * input_file = NULL;
char * txn_file = NULL;

#if SMALL_TPCC
UInt32 g_max_items = MAX_ITEMS_SMALL;
UInt32 g_cust_per_dist = CUST_PER_DIST_SMALL;
#else 
UInt32 g_max_items = MAX_ITEMS_NORM;
UInt32 g_cust_per_dist = CUST_PER_DIST_NORM;
#endif
UInt32 g_max_items_per_txn = MAX_ITEMS_PER_TXN;
UInt32 g_dist_per_wh = DIST_PER_WH;

UInt32 g_type_repl = REPL_TYPE;
UInt32 g_cnt_repl = REPLICA_CNT;
