
#include "row.h"
#include "txn.h"
#include "row_occ_template.h"
#include "mem_alloc.h"
#include "manager.h"
#include "universal.h"
#include "occ_template.h"

void Row_occ_template::init(RowData * row) {
	_row = row;

  read_ts = 0;
  write_ts = 0;
  occ_template_avail = true;
  uc_writes = new std::set<uint64_t>();
  uc_reads = new std::set<uint64_t>();
  assert(uc_writes->begin() == uc_writes->end());
  assert(uc_writes->size() == 0);
	
}

RC Row_occ_template::access(access_t type, TxnMgr * txn) {
    uint64_t begintime = acquire_ts();
#if WORKLOAD == TPCC
  read_and_prewrite(txn);
#else
  if(type == RD)
    read(txn);
  if(type == WR)
    prewrite(txn);
#endif
  uint64_t timespan = acquire_ts() - begintime;
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;
  return RCOK;
}

RC Row_occ_template::read_and_prewrite(TxnMgr * txn) {
	assert (ALGO == OCCTEMPLATE);
	RC rc = RCOK;

  uint64_t mtx_start_write_time = acquire_ts();
  while(!ATOM_CAS(occ_template_avail,true,false)) { }
  INC_STATS(txn->read_thd_id(),mtx[30],acquire_ts() - mtx_start_write_time);
  DEBUG("READ + PREWRITE %ld -- %ld: lw %ld\n",txn->read_txn_id(),_row->get_primary_key(),write_ts);

  // Copy uncommitted writes
  for(auto it = uc_writes->begin(); it != uc_writes->end(); it++) {
    uint64_t txn_id = *it;
    txn->uc_writes->insert(txn_id);
    txn->uc_writes_y->insert(txn_id);
    DEBUG("    UW %ld -- %ld: %ld\n",txn->read_txn_id(),_row->get_primary_key(),txn_id);
  }

  // Copy uncommitted reads 
  for(auto it = uc_reads->begin(); it != uc_reads->end(); it++) {
    uint64_t txn_id = *it;
    txn->uc_reads->insert(txn_id);
    DEBUG("    UR %ld -- %ld: %ld\n",txn->read_txn_id(),_row->get_primary_key(),txn_id);
  }

  // Copy read time_stamp
  if(txn->latest_rts < read_ts)
    txn->latest_rts = read_ts;


  // Copy write time_stamp
  if(txn->latest_wts < write_ts)
    txn->latest_wts = write_ts;

  //Add to uncommitted reads (soft lock)
  uc_reads->insert(txn->read_txn_id());

  //Add to uncommitted writes (soft lock)
  uc_writes->insert(txn->read_txn_id());

  ATOM_CAS(occ_template_avail,false,true);

	return rc;
}


RC Row_occ_template::read(TxnMgr * txn) {
	assert (ALGO == OCCTEMPLATE);
	RC rc = RCOK;

  uint64_t mtx_start_write_time = acquire_ts();
  while(!ATOM_CAS(occ_template_avail,true,false)) { }
  INC_STATS(txn->read_thd_id(),mtx[30],acquire_ts() - mtx_start_write_time);
  DEBUG("READ %ld -- %ld: lw %ld\n",txn->read_txn_id(),_row->get_primary_key(),write_ts);

  // Copy uncommitted writes
  for(auto it = uc_writes->begin(); it != uc_writes->end(); it++) {
    uint64_t txn_id = *it;
    txn->uc_writes->insert(txn_id);
    DEBUG("    UW %ld -- %ld: %ld\n",txn->read_txn_id(),_row->get_primary_key(),txn_id);
  }

  // Copy write time_stamp
  if(txn->latest_wts < write_ts)
    txn->latest_wts = write_ts;

  //Add to uncommitted reads (soft lock)
  uc_reads->insert(txn->read_txn_id());

  ATOM_CAS(occ_template_avail,false,true);

	return rc;
}

RC Row_occ_template::prewrite(TxnMgr * txn) {
	assert (ALGO == OCCTEMPLATE);
	RC rc = RCOK;

  uint64_t mtx_start_write_time = acquire_ts();
  while(!ATOM_CAS(occ_template_avail,true,false)) { }
  INC_STATS(txn->read_thd_id(),mtx[31],acquire_ts() - mtx_start_write_time);
  DEBUG("PREWRITE %ld -- %ld: lw %ld, lr %ld\n",txn->read_txn_id(),_row->get_primary_key(),write_ts,read_ts);

  // Copy uncommitted reads 
  for(auto it = uc_reads->begin(); it != uc_reads->end(); it++) {
    uint64_t txn_id = *it;
    txn->uc_reads->insert(txn_id);
    DEBUG("    UR %ld -- %ld: %ld\n",txn->read_txn_id(),_row->get_primary_key(),txn_id);
  }

  // Copy uncommitted writes 
  for(auto it = uc_writes->begin(); it != uc_writes->end(); it++) {
    uint64_t txn_id = *it;
    txn->uc_writes_y->insert(txn_id);
    DEBUG("    UW %ld -- %ld: %ld\n",txn->read_txn_id(),_row->get_primary_key(),txn_id);
  }

  // Copy read time_stamp
  if(txn->latest_rts < read_ts)
    txn->latest_rts = read_ts;

  // Copy write time_stamp
  if(txn->latest_wts < write_ts)
    txn->latest_wts = write_ts;

  //Add to uncommitted writes (soft lock)
  uc_writes->insert(txn->read_txn_id());

  ATOM_CAS(occ_template_avail,false,true);

	return rc;
}


RC Row_occ_template::abort(access_t type, TxnMgr * txn) {	
  uint64_t mtx_start_write_time = acquire_ts();
  while(!ATOM_CAS(occ_template_avail,true,false)) { }
  INC_STATS(txn->read_thd_id(),mtx[32],acquire_ts() - mtx_start_write_time);
  DEBUG("Occ_template Abort %ld: %d -- %ld\n",txn->read_txn_id(),type,_row->get_primary_key());
#if WORKLOAD == TPCC
    uc_reads->erase(txn->read_txn_id());
    uc_writes->erase(txn->read_txn_id());
#else
  if(type == RD) {
    uc_reads->erase(txn->read_txn_id());
  }

  if(type == WR) {
    uc_writes->erase(txn->read_txn_id());
  }
#endif

  ATOM_CAS(occ_template_avail,false,true);
  return Abort;
}

RC Row_occ_template::commit(access_t type, TxnMgr * txn, RowData * data) {	
  uint64_t mtx_start_write_time = acquire_ts();
  while(!ATOM_CAS(occ_template_avail,true,false)) { }
  INC_STATS(txn->read_thd_id(),mtx[33],acquire_ts() - mtx_start_write_time);
  DEBUG("Occ_template Commit %ld: %d,%lu -- %ld\n",txn->read_txn_id(),type,txn->get_commit_timestamp(),_row->get_primary_key());

#if WORKLOAD == TPCC
    if(txn->get_commit_timestamp() >  read_ts)
      read_ts = txn->get_commit_timestamp();
    uc_reads->erase(txn->read_txn_id());
    if(txn->get_commit_timestamp() >  write_ts)
      write_ts = txn->get_commit_timestamp();
    uc_writes->erase(txn->read_txn_id());
    // Apply write to DB
    write(data);

  uint64_t txn_commit_ts = txn->get_commit_timestamp();
  // Forward validation
  // Check uncommitted writes against this txn's 
    for(auto it = uc_writes->begin(); it != uc_writes->end();it++) {
      if(txn->uc_writes->count(*it) == 0) {
        // apply timestamps
        // these write txns need to come AFTER this txn
        uint64_t it_lower = txn_timestamp_bounds.get_lower(txn->read_thd_id(),*it);
        if(it_lower <= txn_commit_ts) {
          txn_timestamp_bounds.set_lower(txn->read_thd_id(),*it,txn_commit_ts+1);
          DEBUG("OCCTEMPLATE forward val set lower %ld: %lu\n",*it,txn_commit_ts+1);
        }
      }
    }

    uint64_t lower =  txn_timestamp_bounds.get_lower(txn->read_thd_id(),txn->read_txn_id());
    for(auto it = uc_writes->begin(); it != uc_writes->end();it++) {
      if(txn->uc_writes_y->count(*it) == 0) {
        // apply timestamps
        // these write txns need to come BEFORE this txn
        uint64_t it_upper = txn_timestamp_bounds.get_upper(txn->read_thd_id(),*it);
        if(it_upper >= txn_commit_ts) {
          txn_timestamp_bounds.set_upper(txn->read_thd_id(),*it,txn_commit_ts-1);
          DEBUG("OCCTEMPLATE forward val set upper %ld: %lu\n",*it,txn_commit_ts-1);
        }
      }
    }

    for(auto it = uc_reads->begin(); it != uc_reads->end();it++) {
      if(txn->uc_reads->count(*it) == 0) {
        // apply timestamps
        // these write txns need to come BEFORE this txn
        uint64_t it_upper = txn_timestamp_bounds.get_upper(txn->read_thd_id(),*it);
        if(it_upper >= lower) {
          txn_timestamp_bounds.set_upper(txn->read_thd_id(),*it,lower-1);
          DEBUG("OCCTEMPLATE forward val set upper %ld: %lu\n",*it,lower-1);
        }
      }
    }



#else
  uint64_t txn_commit_ts = txn->get_commit_timestamp();
  if(type == RD) {
    if(txn_commit_ts >  read_ts)
      read_ts = txn_commit_ts;
    uc_reads->erase(txn->read_txn_id());

  // Forward validation
  // Check uncommitted writes against this txn's 
    for(auto it = uc_writes->begin(); it != uc_writes->end();it++) {
      if(txn->uc_writes->count(*it) == 0) {
        // apply timestamps
        // these write txns need to come AFTER this txn
        uint64_t it_lower = txn_timestamp_bounds.get_lower(txn->read_thd_id(),*it);
        if(it_lower <= txn_commit_ts) {
          txn_timestamp_bounds.set_lower(txn->read_thd_id(),*it,txn_commit_ts+1);
          DEBUG("OCCTEMPLATE forward val set lower %ld: %lu\n",*it,txn_commit_ts+1);
        }
      }
    }

  }
  /*
#if WORKLOAD == TPCC
    if(txn_commit_ts >  read_ts)
      read_ts = txn_commit_ts;
#endif
*/

  if(type == WR) {
    if(txn_commit_ts >  write_ts)
      write_ts = txn_commit_ts;
    uc_writes->erase(txn->read_txn_id());
    // Apply write to DB
    write(data);
    uint64_t lower =  txn_timestamp_bounds.get_lower(txn->read_thd_id(),txn->read_txn_id());
    for(auto it = uc_writes->begin(); it != uc_writes->end();it++) {
      if(txn->uc_writes_y->count(*it) == 0) {
        // apply timestamps
        // these write txns need to come BEFORE this txn
        uint64_t it_upper = txn_timestamp_bounds.get_upper(txn->read_thd_id(),*it);
        if(it_upper >= txn_commit_ts) {
          txn_timestamp_bounds.set_upper(txn->read_thd_id(),*it,txn_commit_ts-1);
          DEBUG("OCCTEMPLATE forward val set upper %ld: %lu\n",*it,txn_commit_ts-1);
        }
      }
    }

    for(auto it = uc_reads->begin(); it != uc_reads->end();it++) {
      if(txn->uc_reads->count(*it) == 0) {
        // apply timestamps
        // these write txns need to come BEFORE this txn
        uint64_t it_upper = txn_timestamp_bounds.get_upper(txn->read_thd_id(),*it);
        if(it_upper >= lower) {
          txn_timestamp_bounds.set_upper(txn->read_thd_id(),*it,lower-1);
          DEBUG("OCCTEMPLATE forward val set upper %ld: %lu\n",*it,lower-1);
        }
      }
    }

  }
#endif



  ATOM_CAS(occ_template_avail,false,true);
	return RCOK;
}

void
Row_occ_template::write(RowData * data) {
	_row->copy(data);
}


