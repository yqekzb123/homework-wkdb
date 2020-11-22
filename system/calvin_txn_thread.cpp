
#include "global.h"
#include "manager.h"
#include "thread.h"
#include "calvin_txn_thread.h"
#include "txn.h"
#include "workload.h"
#include "query.h"
#include "qry_ycsb.h"
#include "qry_tpcc.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "universal.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "sequencer.h"
#include "logMan.h"
#include "message.h"
#include "task_queue.h"

void CalvinscheduleThread::setup() {
}

RC CalvinscheduleThread::run() {
    tsetup();

    RC rc = RCOK;
    TxnMgr * txn_man;
    uint64_t prof_starttime = acquire_ts();
    uint64_t idle_starttime = 0;

    while(!simulate_man->is_done()) {
        txn_man = NULL;

        Msg * message = task_queue.sched_dequeue(_thd_id);

        if(!message) {
            if(idle_starttime == 0)
                idle_starttime = acquire_ts();
            continue;
        }
        if(idle_starttime > 0) {
            INC_STATS(_thd_id,sched_idle_time,acquire_ts() - idle_starttime);
            idle_starttime = 0;
        }

        prof_starttime = acquire_ts();
        assert(message->get_rtype() == WKCQRY);
        assert(message->read_txn_id() != UINT64_MAX);

        txn_man = txn_table.get_transaction_manager(read_thd_id(),message->read_txn_id(),message->get_batch_id());
        while(!txn_man->unset_ready()) { }
        assert(ISSERVERN(message->get_return_id()));
        txn_man->txn_stats.begintime = acquire_ts();

        txn_man->txn_stats.lat_network_time_start = message->lat_network_time;
        txn_man->txn_stats.lat_other_time_start = message->lat_other_time;

        message->copy_to_txn(txn_man);
        txn_man->register_thread(this);
        assert(ISSERVERN(txn_man->return_id));

        INC_STATS(read_thd_id(),sched_txn_table_time,acquire_ts() - prof_starttime);
        prof_starttime = acquire_ts();

        rc = RCOK;
        // Acquire locks
        if (!txn_man->isRecon()) {
            rc = txn_man->acquire_locks();
        }

        if(rc == RCOK) {
            task_queue.enqueue(_thd_id,message,false);
        }
        txn_man->set_ready();

        INC_STATS(_thd_id,mtx[33],acquire_ts() - prof_starttime);
        prof_starttime = acquire_ts();

    }
    printf("FINISH %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;
}

void CalvinSequenceThread::setup() {
}

bool CalvinSequenceThread::is_batch_ready() {
    bool ready = get_clock_wall() - simulate_man->last_seq_epoch_time >= g_limit_seq_batch_time;
    return ready;
}

RC CalvinSequenceThread::run() {
    tsetup();

    Msg * message;
    uint64_t idle_starttime = 0;
    uint64_t prof_starttime = 0;

    while(!simulate_man->is_done()) {

        prof_starttime = acquire_ts();

        if(is_batch_ready()) {
          simulate_man->advance_seq_epoch();
          //last_batchtime = get_clock_wall();
          seq_man.send_batch_next(_thd_id);
        }

        INC_STATS(_thd_id,mtx[30],acquire_ts() - prof_starttime);
        prof_starttime = acquire_ts();

        message = task_queue.sequencer_dequeue(_thd_id);

        INC_STATS(_thd_id,mtx[31],acquire_ts() - prof_starttime);
        prof_starttime = acquire_ts();

        if(!message) {
            if(idle_starttime == 0)
                idle_starttime = acquire_ts();
            continue;
        }
        if(idle_starttime > 0) {
          INC_STATS(_thd_id,seq_idle_time,acquire_ts() - idle_starttime);
          idle_starttime = 0;
        }

        switch (message->get_rtype()) {
          case WKCQRY:
            // Query from client
            DEBUG("SEQ process_txn\n");
            seq_man.process_txn(message,read_thd_id(),0,0,0,0);
            // Don't free message yet
            break;
          case WKACK_CALVIN:
            // Ack from server
            DEBUG("SEQ process_ack (%ld,%ld) from %ld\n",message->read_txn_id(),message->get_batch_id(),message->get_return_id());
            seq_man.process_ack(message,read_thd_id());
            // Free message here
            message->release();
            break;
          default:
            assert(false);
        }

        INC_STATS(_thd_id,mtx[32],acquire_ts() - prof_starttime);
        prof_starttime = acquire_ts();
    }
    printf("FINISH %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;

}
