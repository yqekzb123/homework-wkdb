#include <unistd.h>

#include "config.h"
#include "da.h"
#include "da_const.h"
#include "da_query.h"
#include "IndexBTree.h"
#include "IndexHash.h"
#include "message.h"
#include "msg_queue.h"
#include "query.h"
#include "row.h"
#include "table.h"
#include "thread.h"
#include "transport.h"
#include "workload.h"

void DATxnManager::init(uint64_t thd_id, WLSchema *h_wl) {
  TxnMgr::init(thd_id, h_wl);
  _wl = (DAWorkload *)h_wl;
  reset();
}
RC DATxnManager::run_txn_post_wait() {
  get_row_post_wait(row);
  return RCOK;
}
RC DATxnManager::acquire_locks(){return RCOK;}
RC DATxnManager::run_calvin_txn(){return RCOK;}
void DATxnManager::reset(){TxnMgr::reset();}
RC DATxnManager::run_txn() {
#if MODE == SETUP_MODE
  return RCOK;
#endif
  RC rc = RCOK;
  //uint64_t starttime = acquire_ts();
  if(IS_LOCAL(txn->txn_id)) {
    DEBUG("Running txn %ld\n",txn->txn_id);
#if DISTR_DEBUG
    query->print();
#endif
    query->parts_touched.add_unique(GET_PART_ID(0,g_node_id));
  }

  DAQuery *da_query = (DAQuery *)query;
  uint64_t trans_id = da_query->trans_id;
  uint64_t item_id = da_query->item_id;  // item_id from 0 to 2 represent X,Y,Z
  //uint64_t seq_id = da_query->seq_id;
  uint64_t state = da_query->state;
  uint64_t version = da_query->write_version;
  //uint64_t next_state = da_query->next_state;
  //uint64_t last_state = da_query->last_state;
  DATxnType txn_type = da_query->txn_type;
  bool jump=false;

  #if WORKLOAD ==DA
    printf("thd_id:%lu check: state:%lu nextstate:%lu \n",h_thd->_thd_id, state, _wl->nextstate);
    fflush(stdout);
  #endif
  if(_wl->nextstate!=0)
  {
    while (state != _wl->nextstate&&!simulate_man->is_done());
  }

  if(already_abort_tab.count(trans_id)>0)
  {
    if(txn_type==DA_WRITE || txn_type==DA_READ||txn_type==DA_COMMIT||txn_type==DA_ABORT)
    {
      jump=true;
      if(txn_type==DA_ABORT)
        INC_STATS(read_thd_id(), positive_txn_abort_cnt, 1);
    }
    //else if(txn_type==DA_COMMIT)
      //txn_type=DA_ABORT;
  }

  if(!jump)
  {
    //enum RC { RCOK = 0, Commit, Abort, WAIT, WAIT_REM, ERROR, FINISH, NONE };
      itemidData *item;
      INDEX *index = _wl->i_datab;
      uint64_t value[3];
      RC rc2 = RCOK;
      item = index_read(index, item_id, 0);
      assert(item != NULL);
      RowData *TempRow = ((RowData *)item->location);

      switch (txn_type) {
        case DA_WRITE: {
          rc = get_row(TempRow, WR, row);
          if(rc == RCOK)
            row->set_value(VALUE, version);
          else
          {
            rc2 = rc;
            rc = start_abort();
            already_abort_tab.insert(trans_id);
          }
          break;
        }
        case DA_READ: {
          rc = get_row(TempRow, RD, row);
          if(rc == RCOK)
            row->get_row_value(VALUE, value[0]);
          else
          {
            rc2 = rc;
            rc = start_abort();
            already_abort_tab.insert(trans_id);
          }
          break;
        }
        case DA_COMMIT: {
          rc=start_commit();
          break;
        }
        case DA_ABORT: {
          INC_STATS(read_thd_id(), positive_txn_abort_cnt, 1);
          rc = start_abort();
          break;
        }
        case DA_SCAN: {
          RowData *TempRow;
          for (int i = 0; i < ITEM_CNT; i++) {
            item = index_read(index, item_id, 0);
            assert(item != NULL);
            TempRow = ((RowData *)item->location);
            rc = get_row(TempRow, WR, row);
            row->get_row_value(VALUE, value[i]);
          }
          break;
        }
      }
      if (rc == RCOK) {
        switch (txn_type)
        {
          case DA_WRITE:
            DA_history_mem.push_back('W');
            break;
          case DA_READ:
            DA_history_mem.push_back('R');
            break;
          case DA_COMMIT:
            DA_history_mem.push_back('C');
            break;
          case DA_ABORT:
            DA_history_mem.push_back('A');
            break;
          case DA_SCAN:
            DA_history_mem.push_back('S');
            break;
        }
        DA_history_mem.push_back(static_cast<char>('0'+trans_id));//trans_id
        if(txn_type==DA_WRITE || txn_type==DA_READ) {
          DA_history_mem.push_back(static_cast<char>('a'+item_id));//item_id
          DA_history_mem.push_back(static_cast<char>('='));//item_id
          if (txn_type==DA_WRITE)
            DA_history_mem.push_back(static_cast<char>('0'+version));//item_id
          else if (txn_type==DA_READ)
            DA_history_mem.push_back(static_cast<char>('0'+value[0]));//item_id
        } 
        DA_history_mem.push_back(' ');
      } else if (rc == Commit) {
        DA_history_mem.push_back('C');
        DA_history_mem.push_back(static_cast<char>('0'+trans_id));//trans_id
        DA_history_mem.push_back(' ');
      } else {
        if (rc2 == WAIT) {
          DA_history_mem.push_back('W');
          DA_history_mem.push_back('a');
          DA_history_mem.push_back('i');
          DA_history_mem.push_back('t');
        }
        else DA_history_mem.push_back('A');
        DA_history_mem.push_back(static_cast<char>('0'+trans_id));//trans_id
        DA_history_mem.push_back(' ');
      }
  }

  _wl->nextstate = da_query->next_state;
  if(_wl->nextstate==0)
  {
    if(abort_history)
      abort_file<<DA_history_mem<<endl;
    else
      commit_file<<DA_history_mem<<endl;

    string().swap(DA_history_mem);
    abort_history=false;
    da_start_stamp_tab.clear();
    _wl->reset_tab_idx();
    already_abort_tab.clear();
    da_start_trans_tab.clear();
  }
  return rc;
}
