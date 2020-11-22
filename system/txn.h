
#ifndef _TXN_H_
#define _TXN_H_

#include "global.h"
#include "universal.h"
#include "semaphore.h"
#include "array.h"
//#include "workload.h"

class WLSchema;
class Thread;
class RowData;
class TableSchema;
class BaseQry;
class INDEX;
class TxnQEntry; 
class QryYCSB;
class QryTPCC;
//class r_query;

enum TxnState {START,INIT,EXEC,PREP,FIN,DONE};

class Access {
public:
	access_t 	type;
	RowData * 	orig_row;
	RowData * 	data;
	RowData * 	orig_data;
	void cleanup();
};

class Txn {
public:
    void init();
    void reset(uint64_t thd_id);
    void release_access(uint64_t thd_id);
    void release_insert(uint64_t thd_id);
    void release(uint64_t thd_id);
    //vector<Access*> access;
    Array<Access*> access;
    uint64_t time_stamp;

    uint64_t write_cnt;
    uint64_t row_cnt;
    // Internal state
    TxnState twopc_state;
    Array<RowData*> insert_rows;
    txnid_t         txn_id;
    uint64_t batch_id;
    RC rc;
};

class TxnStats {
public:
    void init();
    void clear_short();
    void reset();
    void stats_abort(uint64_t thd_id);
    void stats_commit(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id, uint64_t timespan_long, uint64_t timespan_short);
    uint64_t begintime;
    uint64_t restart_begintime;
    uint64_t wait_begintime;
    uint64_t write_cnt;
    uint64_t abort_cnt;
    double total_process_time;
    double process_time;
    double total_local_wait_time;
    double local_wait_time;
    double total_remote_wait_time; // time waiting for a remote response, to help calculate network time
    double remote_wait_time;
    double total_twopc_time;
    double two_pc_time;
    double total_abort_time; // time spent in aborted query land
    double total_msg_queue_time; // time spent on outgoing queue
    double msg_queue_time;
    double total_task_queue_time; // time spent on work queue
    double task_queue_time;
    double total_cc_block_time; // time spent blocking on a cc resource
    double cc_block_time;
    double total_cc_time; // time spent actively doing cc
    double cc_time;
    uint64_t total_task_queue_cnt;
    uint64_t task_queue_cnt;

    // short stats
    double task_queue_time_short;
    double cc_block_time_short;
    double cc_time_short;
    double msg_queue_time_short;
    double process_time_short;
    double network_time_short;

    double lat_network_time_start;
    double lat_other_time_start;
};

/*
   Execution of transactions
   Manipulates/manages Txn (contains txn-specific data)
   Maintains BaseQry (contains input args, info about query)
   */
class TxnMgr
{
public:
    virtual ~TxnMgr() {}
    virtual void init(uint64_t thd_id,WLSchema * h_wl);
    virtual void reset();
    void clear();
    void reset_query();
    void release();
    Thread * h_thd;
    WLSchema * h_wl;

    virtual RC      run_txn() = 0;
    virtual RC      run_txn_post_wait() = 0;
    virtual RC      run_calvin_txn() = 0;
    virtual RC      acquire_locks() = 0;
    void            register_thread(Thread * h_thd);
    uint64_t        read_thd_id();
    WLSchema *      get_wl();
    void            set_txn_id(txnid_t txn_id);
    txnid_t         read_txn_id();
    void            set_query(BaseQry * qry);
    BaseQry *     get_query();
    bool            is_done();
    void            stats_commit();
    bool            is_multi_part();

    void            set_timestamp(ts_t time_stamp);
    ts_t            get_timestamp();

    uint64_t        get_rsp_cnt() {return response_cnt;}
    uint64_t        incr_rsp(int i);
    uint64_t        decr_rsp(int i);
    uint64_t        incr_lr();
    uint64_t        decr_lr();

    RC commit();
    RC start_commit();
    RC start_abort();
    RC abort();

    void release_locks(RC rc);
    bool isRecon() { assert(ALGO == CALVIN || !recon); return recon;};
    bool recon;

    RowData * volatile cur_row;
    // [DL_DETECT, NO_WAIT, WAIT_DIE]
    int volatile   lock_ready;

    // [HSTORE, HSTORE_SPEC]
    int volatile    ready_part;
    int volatile    ready_ulk;
    bool aborted;
    uint64_t return_id;
    RC        validate();
    void            cleanup(RC rc);
    void            cleanup_row(RC rc,uint64_t rid);
    void release_last_lock();
    RC send_reads_remote();
    // void set_end_timestamp(ui/get_end_timestamp() {return txn->end_timestamp;}
    uint64_t access_get_cnt() {return txn->row_cnt;}
    uint64_t sizeof_write_set() {return txn->write_cnt;}
    uint64_t sizeof_read_set() {return txn->row_cnt - txn->write_cnt;}
    access_t access_get_type(uint64_t access_id) {return txn->access[access_id]->type;}
    RowData * access_get_original_row(uint64_t access_id) {return txn->access[access_id]->orig_row;}
    void swap_accesses(uint64_t a, uint64_t b) {
      txn->access.swap(a,b);
    }
    uint64_t get_batch_id() {return txn->batch_id;}
    void set_batch_id(uint64_t batch_id) {txn->batch_id = batch_id;}

    // For OCC_TEMPLATE
    uint64_t commit_timestamp;
    uint64_t get_commit_timestamp() {return commit_timestamp;}
    void set_commit_timestamp(uint64_t time_stamp) {commit_timestamp = time_stamp;}
    uint64_t latest_wts;
    uint64_t latest_rts;
    std::set<uint64_t> * uc_reads;
    std::set<uint64_t> * uc_writes;
    std::set<uint64_t> * uc_writes_y;

    uint64_t twopl_wait_start;

	////////////////////////////////
	// LOGGING
	////////////////////////////////
//	void 			gen_log_entry(int &length, void * log);
    bool log_flushed;
    bool repl_finished;
    Txn * txn;
    BaseQry * query;
    uint64_t client_start_ts;
    uint64_t client_id;
    uint64_t get_abort_cnt() {return abort_cnt;}
    uint64_t abort_cnt;
    int received_rps(RC rc);
    bool waiting_for_rps();
    RC get_rc() {return txn->rc;}
    void set_rc(RC rc) {txn->rc = rc;}
    //void send_rfin_messages(RC rc) {assert(false);}
    void send_finish_msg();
    void send_prepare_msg();

    TxnStats txn_stats;

    bool set_ready() {return ATOM_CAS(txn_ready,0,1);}
    bool unset_ready() {return ATOM_CAS(txn_ready,1,0);}
    bool is_ready() {return txn_ready == true;}
    volatile int txn_ready;
    // Calvin
    uint32_t lock_ready_cnt;
    uint32_t calvin_expected_rsp_cnt;
    bool locking_done;
    CALVIN_PHASE phase;
    Array<RowData*> locked_rows_calvin;
    bool calvin_exec_phase_done();
    bool calvin_collect_phase_done();

protected:	

    int response_cnt;
    void            insert_row(RowData * row, TableSchema * table);

    itemidData *      index_read(INDEX * index, idx_key_t key, int part_id);
    itemidData *      index_read(INDEX * index, idx_key_t key, int part_id, int count);
    RC get_lock(RowData * row, access_t type);
    RC get_row(RowData * row, access_t type, RowData *& row_rtn);
    RC get_row_post_wait(RowData *& row_rtn);

    // For Waiting
    RowData * last_row;
    RowData * last_row_rtn;
    access_t last_type;

    sem_t rsp_mutex;
};

#endif

