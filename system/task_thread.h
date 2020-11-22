
#ifndef _WORKERTHREAD_H_
#define _WORKERTHREAD_H_

#include "global.h"

class WLSchema;
class Msg;

class TaskThread : public Thread {
public:
    RC run();
    void setup();
    void process(Msg * message);
    RC runTest(TxnMgr * txn);
    void check_if_done(RC rc);
    void release_txn_manager();
    void commit();
    void abort();
    TxnMgr * get_transaction_manager(Msg * message);
    void calvin_wrapup();
    RC rfin_process(Msg * message);
    RC rfwd_process(Msg * message);
    RC rack_rfin_process(Msg * message);
    RC rack_prep_process(Msg * message);
    RC rqry_rsp_process(Msg * message);
    RC rqry_process(Msg * message);
    RC rqry_cont_process(Msg * message);
    RC rinit_process(Msg * message);
    RC rprepare_process(Msg * message);
    RC rpass_process(Msg * message);
    RC rtxn_process(Msg * message);
    RC calvin_rtxn_process(Msg * message);
    RC rtxn_cont_process(Msg * message);
    RC log_msg_process(Msg * message);
    RC log_msg_rsp_process(Msg * message);
    RC log_flushed_process(Msg * message);
    RC init_phase();
    uint64_t get_next_txn_id();
    bool is_cc_new_timestamp();

private:
    uint64_t _thd_txn_id;
    ts_t        _curr_ts;
    ts_t        get_next_ts();
    TxnMgr * txn_man;


};

#endif
