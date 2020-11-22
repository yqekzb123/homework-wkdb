
#ifndef _GLOBAL_H_
#define _GLOBAL_H_

#define __STDC_LIMIT_MACROS
#include <stdint.h>
#include <unistd.h>
#include <cstddef>
#include <cstdlib>
#include <cassert>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string.h>
#include <typeinfo>
#include <list>
#include <map>
#include <set>
#include <queue>
#include <string>
#include <vector>
#include <sstream>
#include <time.h> 
#include <sys/time.h>
#include <math.h>

#include "pthread.h"
#include "config.h"
#include "stats.h"
//#include "task_queue.h"
#include "pool.h"
#include "txn_table.h"
#include "logMan.h"
#include "sim_manager.h"
//#include "occ_template.h"

using namespace std;

class mem_alloc;
class Stats;
class Simulator;
class Manager;
class Qry_queue;
// class OptCC;
class Occ_template;
class Transport;
class Remote_query;
class TxnManPool;
class TxnPool;
class AccessPool;
class TxnTablePool;
class MsgPool;
class RowPool;
class QryPool;
class TxnTable;
class TaskQueue;
class AbortTxnQueue;
class MsgQueue;
class Qry_queue_client;
class Client_txn;
class Sequencer;
class LogMan;
class TimestampBounds;

typedef uint32_t UInt32;
typedef int32_t SInt32;
typedef uint64_t UInt64;
typedef int64_t SInt64;

typedef uint64_t ts_t; // time stamp type

/******************************************/
// Global Data Structure 
/******************************************/
extern mem_alloc alloc_memory;
extern Stats stats;
extern Simulator * simulate_man;
extern Manager global_manager;
extern Qry_queue query_queue;
extern Qry_queue_client qry_queue_client;
// extern OptCC occ_man;
extern Occ_template occ_template_man;
extern Transport tport_manager;
extern TxnManPool txn_man_pool;
extern TxnPool txn_pool;
extern AccessPool acc_pool;
extern TxnTablePool tbl_txn_pool;
extern MsgPool message_pool;
extern RowPool row_pool;
extern QryPool query_pool;
extern TxnTable txn_table;
extern TaskQueue task_queue;
extern AbortTxnQueue abort_queue;
extern MsgQueue msg_queue;
extern Client_txn client_man;
extern Sequencer seq_man;
extern LogMan logger;
extern TimestampBounds txn_timestamp_bounds;

extern bool volatile warmup_done;
extern bool volatile enable_thread_mem_pool;
extern pthread_barrier_t warmup_bar;

/******************************************/
// Client Global Params 
/******************************************/
extern UInt32 g_cl_thd_cnt;
extern UInt32 g_cl_rem_thd_cnt;
extern UInt32 g_cl_send_thd_cnt;
extern UInt32 g_cl_node_cnt;
extern UInt32 g_servers_per_client;
extern UInt32 g_clients_per_server;
extern UInt32 g_server_start_node;

/******************************************/
// Global Parameter
/******************************************/
extern volatile UInt64 g_row_id;
extern bool g_alloc_part;
extern bool g_pad_mem;
extern bool g_prt_distr_lat;
extern UInt32 g_node_id;
extern UInt32 g_cnt_node;
extern UInt32 g_cnt_part;
extern UInt32 g_virtual_part_cnt;
extern UInt32 g_core_cnt;
extern UInt32 g_node_total_cnt;
extern UInt32 g_thread_total_cnt;
extern UInt32 g_total_client_thread_cnt;
extern UInt32 g_this_thread_cnt;
extern UInt32 g_this_rem_thread_cnt;
extern UInt32 g_send_thd_cnt_this;
extern UInt32 g_this_total_thread_cnt;
extern UInt32 g_thd_cnt;
extern UInt32 g_abort_thread_cnt;
extern UInt32 g_logger_thread_cnt;
extern UInt32 g_send_thd_cnt;
extern UInt32 g_rem_thd_cnt;
extern ts_t g_penalty_abort; 
extern ts_t g_abort_penalty_max; 
extern bool g_central_man;
extern UInt32 g_alloc_ts;
extern bool g_key_order;
extern bool g_alloc_ts_batch;
extern UInt32 g_ts_batch_num;
extern int32_t g_max_inflight;
extern uint64_t g_msg_size;
extern uint64_t g_log_buf_max;
extern uint64_t g_log_flush_timeout;

extern UInt32 g_per_part_max_txn;
extern int32_t g_per_server_load;

extern bool g_hw_migrate;
extern UInt32 g_delay_net;
extern UInt64 g_timer_done;
extern UInt64 g_limit_batch_time;
extern UInt64 g_limit_seq_batch_time;
extern UInt64 g_prog_timer;
extern UInt64 g_warmup_timer;
extern UInt64 g_msg_time_limit;

// YCSB
extern UInt32 g_cc_alg;
extern ts_t g_intvl_query;
extern UInt32 g_per_txn_part;
extern double g_multi_part_perc;
extern double g_read_txn_perc;
extern double g_write_txn_perc;
extern double g_read_tup_perc;
extern double g_write_tup_perc;
extern double g_theta_zipf;
extern double g_perc_data;
extern double g_perc_access;
extern UInt64 g_table_size_synth;
extern UInt32 g_per_qry_req;
extern bool g_ppt_strict;
extern UInt32 g_per_tuple_field;
extern UInt32 g_init_parallelism;
extern double g_mpr;
extern double g_mpitem;

// TPCC
extern UInt32 g_wh_num;
extern double g_payment_perc;
extern bool g_update_wh;
extern char * output_file;
extern char * input_file;
extern char * txn_file;
extern UInt32 g_max_items;
extern UInt32 g_dist_per_wh;
extern UInt32 g_cust_per_dist;
extern UInt32 g_max_items_per_txn;

// CALVIN
extern UInt32 g_seq_thread_cnt;

// Replication
extern UInt32 g_type_repl;
extern UInt32 g_cnt_repl;

enum RC { RCOK=0, Commit, Abort, WAIT, WAIT_REM, ERROR, FINISH, NONE };
enum RemReqType {WKINIT_DONE=0,
    WKCQRY,
    WKRQRY,
    WKRQRY_CONT,
    WKRFIN,
    WKRQRY_RSP,
    WKRACK_PREP,
    WKRACK_FIN,
    WKRTXN,
    WKRTXN_CONT,
    WKRPREPARE,
    WKRFWD,
    WKRDONE,
    WKCL_RSP,
    WKLOG_MSG,
    WKLOG_MSG_RSP,
    WKLOG_FLUSHED,
    WKACK_CALVIN,
    WKNO_MSG};

// Calvin
enum CALVIN_PHASE {CALVIN_RW_ANALYSIS=0,CALVIN_LOC_RD,CALVIN_SERVE_RD,CALVIN_COLLECT_RD,CALVIN_EXEC_WR,CALVIN_DONE};

/* Thread */
typedef uint64_t txnid_t;

/* Txn */
typedef uint64_t txn_t;

/* Table and Row */
typedef uint64_t rid_t; // row id
typedef uint64_t pgid_t; // page id



/* INDEX */
enum latch_t {LATCH_EX, LATCH_SH, LATCH_NONE};
// accessing type determines the latch type on nodes
enum idx_acc_t {INDEX_INSERT, INDEX_READ, INDEX_NONE};
typedef uint64_t idx_key_t; // key id for index
typedef uint64_t (*func_ptr)(idx_key_t);	// part_id func_ptr(index_key);

/* general concurrency control */
enum access_t {RD, WR, XP, SCAN};
/* LOCK */
enum lock_t {LOCK_EX = 0, LOCK_SH, LOCK_NONE };
/* TIMESTAMP */
enum TsType {R_REQ = 0, W_REQ, P_REQ, XP_REQ}; 

#define GET_THREAD_ID(id)	(id % g_thd_cnt)
#define GET_NODE_ID(id)	(id % g_cnt_node)
#define GET_PART_ID(t,n)	(n) 
#define GET_PART_ID_FROM_IDX(idx)	(g_node_id + idx * g_cnt_node) 
#define GET_PART_ID_IDX(p)	(p / g_cnt_node) 
#define ISSERVER (g_node_id < g_cnt_node)
#define ISSERVERN(id) (id < g_cnt_node)
#define ISCLIENT (g_node_id >= g_cnt_node && g_node_id < g_cnt_node + g_cl_node_cnt)
#define ISREPLICA (g_node_id >= g_cnt_node + g_cl_node_cnt && g_node_id < g_cnt_node + g_cl_node_cnt + g_cnt_repl * g_cnt_node)
#define ISREPLICAN(id) (id >= g_cnt_node + g_cl_node_cnt && id < g_cnt_node + g_cl_node_cnt + g_cnt_repl * g_cnt_node)
#define ISCLIENTN(id) (id >= g_cnt_node && id < g_cnt_node + g_cl_node_cnt)
#define IS_LOCAL(tid) (tid % g_cnt_node == g_node_id || ALGO == CALVIN)
#define IS_REMOTE(tid) (tid % g_cnt_node != g_node_id || ALGO == CALVIN)
#define IS_LOCAL_KEY(key) (key % g_cnt_node == g_node_id)

/*
#define GET_THREAD_ID(id)	(id % g_thd_cnt)
#define GET_NODE_ID(id)	(id / g_thd_cnt)
#define GET_PART_ID(t,n)	(n*g_thd_cnt + t) 
*/

#define MSG(str, args...) { \
	printf("[%s : %d] " str, __FILE__, __LINE__, args); } \
//	printf(args); }

// principal index structure. The workload may decide to use a different 
// index structure for specific purposes. (e.g. non-primary key access should use hash)
#if (INDEX_STRUCT == IDX_BTREE)
#define INDEX		IndexBTree
#else  // IDX_HASH
#define INDEX		IndexHash
#endif

/************************************************/
// constants
/************************************************/
#ifndef UINT64_MAX
#define UINT64_MAX 		18446744073709551615UL
#endif // UINT64_MAX

#endif
