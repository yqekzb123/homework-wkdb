
#include "global.h"
#include "universal.h"
#include "txn.h"
#include "occ_template.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_occ_template.h"

void Occ_template::init() {
  sem_init(&_semaphore, 0, 1);
}

RC Occ_template::validate(TxnMgr * txn) {
  uint64_t start_time = acquire_ts();
  uint64_t timespan;
  sem_wait(&_semaphore);

  timespan = acquire_ts() - start_time;
  txn->txn_stats.cc_block_time += timespan;
  txn->txn_stats.cc_block_time_short += timespan;
  INC_STATS(txn->read_thd_id(), occ_template_cs_wait_time, timespan);
  start_time = acquire_ts();
  RC rc = RCOK;
  uint64_t lower = txn_timestamp_bounds.get_lower(txn->read_thd_id(), txn->read_txn_id());
  uint64_t upper = txn_timestamp_bounds.get_upper(txn->read_thd_id(), txn->read_txn_id());
  DEBUG("OCCTEMPLATE Validate Start %ld: [%lu,%lu]\n", txn->read_txn_id(), lower, upper);
  std::set<uint64_t> after;
  std::set<uint64_t> before;
  // lower bound of txn greater than write time_stamp
  if(lower <= txn->latest_wts) {
    lower = txn->latest_wts + 1;
    INC_STATS(txn->read_thd_id(), occ_template_case1_cnt, 1);
  }
  // lower bound of uncommitted writes greater than upper bound of txn
  for(auto it = txn->uc_writes->begin(); it != txn->uc_writes->end(); it++) {
    uint64_t it_lower = txn_timestamp_bounds.get_lower(txn->read_thd_id(), *it);
    if(upper >= it_lower) {
      OCCTEMPLATEState state = txn_timestamp_bounds.get_state(txn->read_thd_id(), *it);
      if(state == OCCTEMPLATE_VALIDATED || state == OCCTEMPLATE_COMMITTED) {
        INC_STATS(txn->read_thd_id(),occ_template_case2_cnt,1);
        if(it_lower > 0) {
          upper = it_lower - 1;
        } else {
          upper = it_lower;
        }
      }
      if(state == OCCTEMPLATE_RUNNING) {
        after.insert(*it);
      }
    }
  }
  // lower bound of txn greater than read time_stamp
  if(lower <= txn->latest_rts) {
    lower = txn->latest_rts + 1;
    INC_STATS(txn->read_thd_id(),occ_template_case3_cnt,1);
  }
  // upper bound of uncommitted reads less than lower bound of txn
  for(auto it = txn->uc_reads->begin(); it != txn->uc_reads->end(); it++) {
    uint64_t it_upper = txn_timestamp_bounds.get_upper(txn->read_thd_id(), *it);
    if(lower <= it_upper) {
      OCCTEMPLATEState state = txn_timestamp_bounds.get_state(txn->read_thd_id(), *it);
      if(state == OCCTEMPLATE_VALIDATED || state == OCCTEMPLATE_COMMITTED) {
        INC_STATS(txn->read_thd_id(),occ_template_case4_cnt,1);
        if(it_upper < UINT64_MAX) {
          lower = it_upper + 1;
        } else {
          lower = it_upper;
        }
      }
      if(state == OCCTEMPLATE_RUNNING) {
        before.insert(*it);
      }
    }
  }
  // upper bound of uncommitted write writes less than lower bound of txn
  for(auto it = txn->uc_writes_y->begin(); it != txn->uc_writes_y->end(); it++) {
      OCCTEMPLATEState state = txn_timestamp_bounds.get_state(txn->read_thd_id(), *it);
    uint64_t it_upper = txn_timestamp_bounds.get_upper(txn->read_thd_id(), *it);
      if(state == OCCTEMPLATE_ABORTED) {
        continue;
      }
      if(state == OCCTEMPLATE_VALIDATED || state == OCCTEMPLATE_COMMITTED) {
        if(lower <= it_upper) {
          INC_STATS(txn->read_thd_id(),occ_template_case5_cnt,1);
          if(it_upper < UINT64_MAX) {
            lower = it_upper + 1;
          } else {
            lower = it_upper;
          }
        }
      }
      if(state == OCCTEMPLATE_RUNNING) {
        after.insert(*it);
      }
  }
  if(lower >= upper) {
    // Abort
    txn_timestamp_bounds.set_state(txn->read_thd_id(),txn->read_txn_id(),OCCTEMPLATE_ABORTED);
    rc = Abort;
  } else {
    // Validated
    txn_timestamp_bounds.set_state(txn->read_thd_id(),txn->read_txn_id(),OCCTEMPLATE_VALIDATED);
    rc = RCOK;

    for(auto it = before.begin(); it != before.end(); it++) {
      uint64_t it_upper = txn_timestamp_bounds.get_upper(txn->read_thd_id(), *it);
      if(it_upper > lower && it_upper < upper-1) {
        lower = it_upper + 1;
      }
    }
    for(auto it = before.begin(); it != before.end(); it++) {
      uint64_t it_upper = txn_timestamp_bounds.get_upper(txn->read_thd_id(), *it);
      if(it_upper >= lower) {
        if(lower > 0) {
          txn_timestamp_bounds.set_upper(txn->read_thd_id(),*it,lower-1);
        } else {
          txn_timestamp_bounds.set_upper(txn->read_thd_id(),*it,lower);
        }
      }
    }
    for(auto it = after.begin(); it != after.end();it++) {
      uint64_t it_lower = txn_timestamp_bounds.get_lower(txn->read_thd_id(),*it);
      uint64_t it_upper = txn_timestamp_bounds.get_upper(txn->read_thd_id(),*it);
      if(it_upper != UINT64_MAX && it_upper > lower + 2 && it_upper < upper ) {
        upper = it_upper - 2;
      } 
      if((it_lower < upper && it_lower > lower+1)) {
        upper = it_lower - 1;
      } 
    }
    // set all upper and lower bounds to meet inequality
    for(auto it = after.begin(); it != after.end();it++) {
      uint64_t it_lower = txn_timestamp_bounds.get_lower(txn->read_thd_id(),*it);
      if(it_lower <= upper) {
        if(upper < UINT64_MAX) {
          txn_timestamp_bounds.set_lower(txn->read_thd_id(),*it,upper+1);
        } else {
          txn_timestamp_bounds.set_lower(txn->read_thd_id(),*it,upper);
        }
      }
    }

    assert(lower < upper);
    INC_STATS(txn->read_thd_id(),occ_template_range,upper-lower);
    INC_STATS(txn->read_thd_id(),occ_template_commit_cnt,1);
  }
  txn_timestamp_bounds.set_lower(txn->read_thd_id(),txn->read_txn_id(),lower);
  txn_timestamp_bounds.set_upper(txn->read_thd_id(),txn->read_txn_id(),upper);
  INC_STATS(txn->read_thd_id(),occ_template_validate_cnt,1);
  timespan = acquire_ts() - start_time;
  INC_STATS(txn->read_thd_id(),occ_template_validate_time,timespan);
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;
  DEBUG("OCCTEMPLATE Validate End %ld: %d [%lu,%lu]\n",txn->read_txn_id(),rc==RCOK,lower,upper);
  sem_post(&_semaphore);
  return rc;

}

RC Occ_template::find_bound(TxnMgr * txn) {
  RC rc = RCOK;
  uint64_t lower = txn_timestamp_bounds.get_lower(txn->read_thd_id(),txn->read_txn_id());
  uint64_t upper = txn_timestamp_bounds.get_upper(txn->read_thd_id(),txn->read_txn_id());
  if(lower >= upper) {
    txn_timestamp_bounds.set_state(txn->read_thd_id(),txn->read_txn_id(),OCCTEMPLATE_VALIDATED);
    rc = Abort;
  } else {
    txn_timestamp_bounds.set_state(txn->read_thd_id(),txn->read_txn_id(),OCCTEMPLATE_COMMITTED);
    // TODO: can commit_time be selected in a smarter way?
    txn->commit_timestamp = lower; 
  }
  DEBUG("OCCTEMPLATE Bound %ld: %d [%lu,%lu] %lu\n",txn->read_txn_id(),rc,lower,upper,txn->commit_timestamp);
  return rc;
}

void TimestampBounds::init() {
  //table_size = g_max_inflight * g_cnt_node * 2 + 1;
  table_size = g_max_inflight + 1;
  DEBUG_M("TimestampBounds::init table alloc\n");
  table = (TimestampBoundsNode*) alloc_memory.alloc(sizeof(TimestampBoundsNode) * table_size);
  for(uint64_t i = 0; i < table_size;i++) {
    table[i].init();
  }
}

uint64_t TimestampBounds::hash(uint64_t key) {
  return key % table_size;
}

TimestampBoundsEntry* TimestampBounds::find(uint64_t key) {
  TimestampBoundsEntry * entry = table[hash(key)].head;
  while(entry) {
    if(entry->key == key) 
      break;
    entry = entry->next;
  }
  return entry;

}

void TimestampBounds::init(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  uint64_t mtx_start_write_time = acquire_ts();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[34],acquire_ts() - mtx_start_write_time);
  TimestampBoundsEntry* entry = find(key);
  if(!entry) {
    DEBUG_M("TimestampBounds::init entry alloc\n");
    entry = (TimestampBoundsEntry*) alloc_memory.alloc(sizeof(TimestampBoundsEntry));
    entry->init(key);
    LIST_PUT_TAIL(table[idx].head,table[idx].tail,entry);
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

void TimestampBounds::release(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  uint64_t mtx_start_write_time = acquire_ts();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[35],acquire_ts() - mtx_start_write_time);
  TimestampBoundsEntry* entry = find(key);
  if(entry) {
    LIST_REMOVE_HT(entry,table[idx].head,table[idx].tail);
    DEBUG_M("TimestampBounds::release entry free\n");
    alloc_memory.free(entry,sizeof(TimestampBoundsEntry));
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

uint64_t TimestampBounds::get_lower(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  uint64_t value = 0;
  uint64_t mtx_start_write_time = acquire_ts();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[36],acquire_ts() - mtx_start_write_time);
  TimestampBoundsEntry* entry = find(key);
  if(entry) {
    value = entry->lower;
  }
  pthread_mutex_unlock(&table[idx].mtx);
  return value;
}

uint64_t TimestampBounds::get_upper(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  uint64_t value = UINT64_MAX;
  uint64_t mtx_start_write_time = acquire_ts();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[37],acquire_ts() - mtx_start_write_time);
  TimestampBoundsEntry* entry = find(key);
  if(entry) {
    value = entry->upper;
  }
  pthread_mutex_unlock(&table[idx].mtx);
  return value;
}


void TimestampBounds::set_lower(uint64_t thd_id, uint64_t key, uint64_t value) {
  uint64_t idx = hash(key);
  uint64_t mtx_start_write_time = acquire_ts();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[38],acquire_ts() - mtx_start_write_time);
  TimestampBoundsEntry* entry = find(key);
  if(entry) {
    entry->lower = value;
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

void TimestampBounds::set_upper(uint64_t thd_id, uint64_t key, uint64_t value) {
  uint64_t idx = hash(key);
  uint64_t mtx_start_write_time = acquire_ts();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[39],acquire_ts() - mtx_start_write_time);
  TimestampBoundsEntry* entry = find(key);
  if(entry) {
    entry->upper = value;
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

OCCTEMPLATEState TimestampBounds::get_state(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  OCCTEMPLATEState state = OCCTEMPLATE_ABORTED;
  uint64_t mtx_start_write_time = acquire_ts();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[40],acquire_ts() - mtx_start_write_time);
  TimestampBoundsEntry* entry = find(key);
  if(entry) {
    state = entry->state;
  }
  pthread_mutex_unlock(&table[idx].mtx);
  return state;
}

void TimestampBounds::set_state(uint64_t thd_id, uint64_t key, OCCTEMPLATEState value) {
  uint64_t idx = hash(key);
  uint64_t mtx_start_write_time = acquire_ts();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id,mtx[41],acquire_ts() - mtx_start_write_time);
  TimestampBoundsEntry* entry = find(key);
  if(entry) {
    entry->state = value;
  }
  pthread_mutex_unlock(&table[idx].mtx);
}
