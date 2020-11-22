#include "logMan.h"
#include "task_queue.h"
#include "message.h"
#include "mem_alloc.h"
#include <fstream>


void LogMan::init(const char * log_file_name) {
  this->log_file_name = log_file_name;
  log_file.open(log_file_name, ios::out | ios::app | ios::binary);
  assert(log_file.is_open());
  pthread_mutex_init(&mtx,NULL);

}

void LogMan::release() {
  log_file.close(); 
}

RecordLog * LogMan::createRecord( 
    uint64_t txn_id,
    LogIUD iud,
    uint64_t table_id,
    uint64_t key
    ) {
  RecordLog * record = (RecordLog*)alloc_memory.alloc(sizeof(RecordLog));
  record->rcd.init();
  record->rcd.lsn = ATOM_FETCH_ADD(lsn,1);
  record->rcd.iud = iud;
  record->rcd.txn_id = txn_id;
  record->rcd.table_id = table_id;
  record->rcd.key = key;
  return record;
}

RecordLog * LogMan::createRecord( 
    RecordLog * record
    ) {
  RecordLog * my_record = (RecordLog*)alloc_memory.alloc(sizeof(RecordLog));
  my_record->rcd.init();
  my_record->copyRecord(record);
  return my_record;
}


void RecordLog::copyRecord( 
    RecordLog * record
    ) {
  rcd.init();
  rcd.lsn = record->rcd.lsn;
  rcd.iud = record->rcd.iud;
  rcd.type = record->rcd.type;
  rcd.txn_id = record->rcd.txn_id;
  rcd.table_id = record->rcd.table_id;
  rcd.key = record->rcd.key;
}


void LogMan::enqueueRecord(RecordLog* record) {
  DEBUG("Enqueue Log Record %ld\n",record->rcd.txn_id);
  pthread_mutex_lock(&mtx);
  log_queue.push(record); 
  pthread_mutex_unlock(&mtx);
}

void LogMan::processRecord(uint64_t thd_id) {
  if(log_queue.empty())
    return;
  RecordLog * record = NULL;
  pthread_mutex_lock(&mtx);
  if(!log_queue.empty()) {
    record = log_queue.front(); 
    log_queue.pop();
  }
  pthread_mutex_unlock(&mtx);

  if(record) {
    uint64_t begintime = acquire_ts();
    DEBUG("Dequeue Log Record %ld\n",record->rcd.txn_id);
    if(record->rcd.iud == L_NOTIFY) {
      flushBuffer(thd_id);
      task_queue.enqueue(thd_id,Msg::create_message(record->rcd.txn_id,WKLOG_FLUSHED),false);

    }
    writeToBuffer(thd_id,record);
    //writeToBuffer((char*)(&record->rcd),sizeof(record->rcd));
    log_buf_cnt++;
    alloc_memory.free(record,sizeof(RecordLog));
    INC_STATS(thd_id,log_process_time,acquire_ts() - begintime);
  }
  
}

uint64_t LogMan::reserveBuffer(uint64_t size) {
  return ATOM_FETCH_ADD(aries_write_offset,size);
}


//void LogMan::writeToBuffer(char * data, uint64_t offset, uint64_t size) {
void LogMan::writeToBuffer(uint64_t thd_id, char * data, uint64_t size) {
  //memcpy(aries_log_buffer + offset, data, size);
  //aries_write_offset += size;
  uint64_t begintime = acquire_ts();
  log_file.write(data,size);
  INC_STATS(thd_id,log_write_time,acquire_ts() - begintime);
  INC_STATS(thd_id,log_write_cnt,1);

}

void LogMan::notify_on_sync(uint64_t txn_id) {
  RecordLog * record = (RecordLog*)alloc_memory.alloc(sizeof(RecordLog));
  record->rcd.init();
  record->rcd.txn_id = txn_id;
  record->rcd.iud = L_NOTIFY;
  enqueueRecord(record);
}

void LogMan::writeToBuffer(uint64_t thd_id, RecordLog * record) {
  DEBUG("Buffer Write\n");
  //memcpy(aries_log_buffer + offset, data, size);
  //aries_write_offset += size;
  uint64_t begintime = acquire_ts();
#if LOG_COMMAND

  WRITE_VAL(log_file,record->rcd.checksum);
  WRITE_VAL(log_file,record->rcd.lsn);
  WRITE_VAL(log_file,record->rcd.type);
  WRITE_VAL(log_file,record->rcd.txn_id);
  //WRITE_VAL(log_file,record->rcd.partid);
#if WORKLOAD == TPCC
  WRITE_VAL(log_file,record->rcd.txntype);
#endif
  WRITE_VAL_SIZE(log_file,record->rcd.params,record->rcd.params_size);

#else

  WRITE_VAL(log_file,record->rcd.checksum);
  WRITE_VAL(log_file,record->rcd.lsn);
  WRITE_VAL(log_file,record->rcd.type);
  WRITE_VAL(log_file,record->rcd.iud);
  WRITE_VAL(log_file,record->rcd.txn_id);
  //WRITE_VAL(log_file,record->rcd.partid);
  WRITE_VAL(log_file,record->rcd.table_id);
  WRITE_VAL(log_file,record->rcd.key);
  /*
  WRITE_VAL(log_file,record->rcd.n_cols);
  WRITE_VAL(log_file,record->rcd.cols);
  WRITE_VAL_SIZE(log_file,record->rcd.before_image,record->rcd.before_image_size);
  WRITE_VAL_SIZE(log_file,record->rcd.after_image,record->rcd.after_image_size);
  */

#endif
  INC_STATS(thd_id,log_write_time,acquire_ts() - begintime);

}

void LogMan::flushBufferCheck(uint64_t thd_id) {
  if(log_buf_cnt >= g_log_buf_max || acquire_ts() - last_flush > g_log_flush_timeout) {
    flushBuffer(thd_id);
  }
}

void LogMan::flushBuffer(uint64_t thd_id) {
  DEBUG("Flush Buffer\n");
  uint64_t begintime = acquire_ts();
  log_file.flush();
  INC_STATS(thd_id,log_flush_time,acquire_ts() - begintime);
  INC_STATS(thd_id,log_flush_cnt,1);

  last_flush = acquire_ts();
  log_buf_cnt = 0;
}
