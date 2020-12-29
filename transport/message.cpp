
#include "mem_alloc.h"
#include "query.h"
#include "qry_ycsb.h"
#include "ycsb.h"
#include "qry_tpcc.h"
#include "tpcc.h"
#include "global.h"
#include "message.h"
#include "occ_template.h"
#include "da.h"
#include "da_query.h"

std::vector<Msg*> * Msg::create_messages(char * buf) {
  std::vector<Msg*> * all_msgs = new std::vector<Msg*>;
  char * data = buf;
	uint64_t ptr = 0;
  uint32_t dest_id;
  uint32_t return_id;
  uint32_t txn_cnt;
  COPY_VAL(dest_id,data,ptr);
  COPY_VAL(return_id,data,ptr);
  COPY_VAL(txn_cnt,data,ptr);
  assert(dest_id == g_node_id);
  assert(return_id != g_node_id);
  assert(ISCLIENTN(return_id) || ISSERVERN(return_id) || ISREPLICAN(return_id));
  while(txn_cnt > 0) {
    Msg * message = create_message(&data[ptr]);
    message->return_node_id = return_id;
    ptr += message->get_size();
    all_msgs->push_back(message);
    --txn_cnt;
  }
  return all_msgs;
}

Msg * Msg::create_message(char * buf) {
 RemReqType rtype = WKNO_MSG;
 uint64_t ptr = 0;
 COPY_VAL(rtype,buf,ptr);
 Msg * message = create_message(rtype);
 message->copy_from_buf(buf);
 return message;
}

Msg * Msg::create_message(TxnMgr * txn, RemReqType rtype) {
 Msg * message = create_message(rtype);
 message->mcopy_from_txn(txn);
 message->copy_from_txn(txn);

 // copy latency here
 message->lat_task_queue_time = txn->txn_stats.task_queue_time_short;
 message->lat_msg_queue_time = txn->txn_stats.msg_queue_time_short;
 message->lat_cc_block_time = txn->txn_stats.cc_block_time_short;
 message->lat_cc_time = txn->txn_stats.cc_time_short;
 message->lat_process_time = txn->txn_stats.process_time_short;
 message->lat_network_time = txn->txn_stats.lat_network_time_start;
 message->lat_other_time = txn->txn_stats.lat_other_time_start;

 return message;
}

Msg * Msg::create_message(RecordLog * record, RemReqType rtype) {
 Msg * message = create_message(rtype);
 ((LogMessage*)message)->copy_from_record(record);
 message->txn_id = record->rcd.txn_id;
 return message;
}


Msg * Msg::create_message(BaseQry * query, RemReqType rtype) {
 assert(rtype == WKRQRY || rtype == WKCQRY);
 Msg * message = create_message(rtype);
#if WORKLOAD == YCSB
 ((QueryMsgYCSBcl*)message)->copy_from_query(query);
#elif WORKLOAD == TPCC 
 ((QueryMsgClientTPCC*)message)->copy_from_query(query);
#elif WORKLOAD == PPS 
 ((PPSClientQueryMessage*)message)->copy_from_query(query);
#elif  WORKLOAD == DA
  ((DAClientQueryMessage*)message)->copy_from_query(query);
#endif
 return message;
}

Msg * Msg::create_message(uint64_t txn_id, RemReqType rtype) {
 Msg * message = create_message(rtype);
 message->txn_id = txn_id;
 return message;
}

Msg * Msg::create_message(uint64_t txn_id, uint64_t batch_id, RemReqType rtype) {
 Msg * message = create_message(rtype);
 message->txn_id = txn_id;
 message->batch_id = batch_id;
 return message;
}

Msg * Msg::create_message(RemReqType rtype) {
  Msg * message;
  switch(rtype) {
    case WKINIT_DONE:
      message = new InitDoneMessage;
      break;
    case WKRQRY:
    case WKRQRY_CONT:
#if WORKLOAD == YCSB
      message = new QueryMsgYCSB;
#elif WORKLOAD == TPCC 
      message = new QueryMsgTPCC;
#elif WORKLOAD == PPS 
      message = new PPSQueryMessage;
#elif WORKLOAD == TEST
      message = new QueryMsgTPCC;
#elif WORKLOAD == DA
      message = new DAQueryMessage;
#endif
      message->init();
      break;
    case WKRFIN:
      message = new FinishMessage;
      break;
    case WKRQRY_RSP:
      message = new QueryResponseMessage;
      break;
    case WKLOG_MSG:
      message = new LogMessage;
      break;
    case WKLOG_MSG_RSP:
      message = new RspLogMessage;
      break;
    case WKLOG_FLUSHED:
      message = new FlushedLogMessage;
      break;
    case WKACK_CALVIN:
    case WKRACK_PREP:
    case WKRACK_FIN:
      message = new AckMessage;
      break;
    case WKCQRY:
    case WKRTXN:
    case WKRTXN_CONT:
#if WORKLOAD == YCSB
      message = new QueryMsgYCSBcl;
#elif WORKLOAD == TPCC 
      message = new QueryMsgClientTPCC;
#elif WORKLOAD == PPS 
      message = new PPSClientQueryMessage;
#elif WORKLOAD == TEST
      message = new QueryMsgClientTPCC;
#elif WORKLOAD == DA
      message = new DAClientQueryMessage;
#endif
      message->init();
      break;
    case WKRPREPARE:
      message = new PrepareMessage;
      break;
    case WKRFWD:
      message = new ForwardMessage;
      break;
    case WKRDONE:
      message = new DoneMessage;
      break;
    case WKCL_RSP:
      message = new ClientRspMessage;
      break;
    default: assert(false);
  }
  assert(message);
  message->rtype = rtype;
  message->txn_id = UINT64_MAX;
  message->batch_id = UINT64_MAX;
  message->return_node_id = g_node_id;
  message->wq_time = 0;
  message->mq_time = 0;
  message->ntwk_time = 0;

  message->lat_task_queue_time = 0;
  message->lat_msg_queue_time = 0;
  message->lat_cc_block_time = 0;
  message->lat_cc_time = 0;
  message->lat_process_time = 0;
  message->lat_network_time = 0;
  message->lat_other_time = 0;


  return message;
}

uint64_t Msg::mget_size() {
  uint64_t size = 0;
  size += sizeof(RemReqType);
  size += sizeof(uint64_t);
#if ALGO == CALVIN
  size += sizeof(uint64_t);
#endif
  // for stats, send message queue time
  size += sizeof(uint64_t);

  // for stats, latency
  size += sizeof(uint64_t) * 7;
  return size;
}

void Msg::mcopy_from_txn(TxnMgr * txn) {
  //rtype = query->rtype;
  txn_id = txn->read_txn_id();
#if ALGO == CALVIN
  batch_id = txn->get_batch_id();
#endif
}

void Msg::mcopy_to_txn(TxnMgr * txn) {
  txn->return_id = return_node_id;
}


void Msg::mcopy_from_buf(char * buf) {
  uint64_t ptr = 0;
  COPY_VAL(rtype,buf,ptr);
  COPY_VAL(txn_id,buf,ptr);
#if ALGO == CALVIN
  COPY_VAL(batch_id,buf,ptr);
#endif
  COPY_VAL(mq_time,buf,ptr);

  COPY_VAL(lat_task_queue_time,buf,ptr);
  COPY_VAL(lat_msg_queue_time,buf,ptr);
  COPY_VAL(lat_cc_block_time,buf,ptr);
  COPY_VAL(lat_cc_time,buf,ptr);
  COPY_VAL(lat_process_time,buf,ptr);
  COPY_VAL(lat_network_time,buf,ptr);
  COPY_VAL(lat_other_time,buf,ptr);
  if ((ALGO == CALVIN && rtype == WKACK_CALVIN && txn_id % g_cnt_node == g_node_id) || (ALGO != CALVIN && IS_LOCAL(txn_id))) {
    lat_network_time = (acquire_ts() - lat_network_time) - lat_other_time;
  } else {
    lat_other_time = acquire_ts();
  }
  //printf("buftot %ld: %f, %f\n",txn_id,lat_network_time,lat_other_time);
}

void Msg::mcopy_to_buf(char * buf) {
  uint64_t ptr = 0;
  COPY_BUF(buf,rtype,ptr);
  COPY_BUF(buf,txn_id,ptr);
#if ALGO == CALVIN
  COPY_BUF(buf,batch_id,ptr);
#endif
  COPY_BUF(buf,mq_time,ptr);

  COPY_BUF(buf,lat_task_queue_time,ptr);
  COPY_BUF(buf,lat_msg_queue_time,ptr);
  COPY_BUF(buf,lat_cc_block_time,ptr);
  COPY_BUF(buf,lat_cc_time,ptr);
  COPY_BUF(buf,lat_process_time,ptr);
  if ((ALGO == CALVIN && rtype == WKCQRY && txn_id % g_cnt_node == g_node_id) || (ALGO != CALVIN && IS_LOCAL(txn_id))) {
    lat_network_time = acquire_ts();
  } else {
    lat_other_time = acquire_ts() - lat_other_time;
  }
  //printf("mtobuf %ld: %f, %f\n",txn_id,lat_network_time,lat_other_time);
  COPY_BUF(buf,lat_network_time,ptr);
  COPY_BUF(buf,lat_other_time,ptr);
}

void Msg::release_message(Msg * message) {
  switch(message->rtype) {
    case WKINIT_DONE: {
      InitDoneMessage * m_msg = (InitDoneMessage*)message;
      m_msg->release();
      delete m_msg;
      break;
                    }
    case WKRQRY:
    case WKRQRY_CONT: {
#if WORKLOAD == YCSB
      QueryMsgYCSB * m_msg = (QueryMsgYCSB*)message;
#elif WORKLOAD == TPCC 
      QueryMsgTPCC * m_msg = (QueryMsgTPCC*)message;
#elif WORKLOAD == TEST
      QueryMsgTPCC * m_msg = (QueryMsgTPCC*)message;
#elif WORKLOAD == DA
      DAQueryMessage* m_msg = (DAQueryMessage*)message;
#endif
      m_msg->release();
      delete m_msg;
      break;
                    }
    case WKRFIN: {
      FinishMessage * m_msg = (FinishMessage*)message;
      m_msg->release();
      delete m_msg;
      break;
               }
    case WKRQRY_RSP: {
      QueryResponseMessage * m_msg = (QueryResponseMessage*)message;
      m_msg->release();
      delete m_msg;
      break;
                   }
    case WKLOG_MSG: {
      LogMessage * m_msg = (LogMessage*)message;
      m_msg->release();
      delete m_msg;
      break;
                  }
    case WKLOG_MSG_RSP: {
      RspLogMessage * m_msg = (RspLogMessage*)message;
      m_msg->release();
      delete m_msg;
      break;
                      }
    case WKLOG_FLUSHED: {
      FlushedLogMessage * m_msg = (FlushedLogMessage*)message;
      m_msg->release();
      delete m_msg;
      break;
                      }
    case WKACK_CALVIN:
    case WKRACK_PREP:
    case WKRACK_FIN: {
      AckMessage * m_msg = (AckMessage*)message;
      m_msg->release();
      delete m_msg;
      break;
                   }
    case WKCQRY:
    case WKRTXN:
    case WKRTXN_CONT: {
#if WORKLOAD == YCSB
      QueryMsgYCSBcl * m_msg = (QueryMsgYCSBcl*)message;
#elif WORKLOAD == TPCC 
      QueryMsgClientTPCC * m_msg = (QueryMsgClientTPCC*)message;
#elif WORKLOAD == PPS 
      PPSClientQueryMessage * m_msg = (PPSClientQueryMessage*)message;
#elif WORKLOAD == TEST
      QueryMsgClientTPCC * m_msg = (QueryMsgClientTPCC*)message;
#elif WORKLOAD == DA
      DAClientQueryMessage* m_msg = (DAClientQueryMessage*)message;
#endif
      m_msg->release();
      delete m_msg;
      break;
                    }
    case WKRPREPARE: {
      PrepareMessage * m_msg = (PrepareMessage*)message;
      m_msg->release();
      delete m_msg;
      break;
                   }
    case WKRFWD: {
      ForwardMessage * m_msg = (ForwardMessage*)message;
      m_msg->release();
      delete m_msg;
      break;
               }
    case WKRDONE: {
      DoneMessage * m_msg = (DoneMessage*)message;
      m_msg->release();
      delete m_msg;
      break;
                }
    case WKCL_RSP: {
      ClientRspMessage * m_msg = (ClientRspMessage*)message;
      m_msg->release();
      delete m_msg;
      break;
                 }
    default: { assert(false); }
  }
}
/************************/

uint64_t QueryMessage::get_size() {
  uint64_t size = Msg::mget_size();

  return size;
}

void QueryMessage::copy_from_txn(TxnMgr * txn) {
  Msg::mcopy_from_txn(txn);

}

void QueryMessage::copy_to_txn(TxnMgr * txn) {
  Msg::mcopy_to_txn(txn);

}

void QueryMessage::copy_from_buf(char * buf) {
  Msg::mcopy_from_buf(buf);
  uint64_t ptr __attribute__ ((unused));
  ptr = Msg::mget_size();

}

void QueryMessage::copy_to_buf(char * buf) {
  Msg::mcopy_to_buf(buf);
  uint64_t ptr __attribute__ ((unused));
  ptr = Msg::mget_size();
}

/************************/

void QueryMsgYCSBcl::init() {
}

void QueryMsgYCSBcl::release() {
  ClientQryMsg::release();
  // Freeing requests is the responsibility of txn at commit time
/*
  for(uint64_t i = 0; i < requests.size(); i++) {
    DEBUG_M("QueryMsgYCSBcl::release rqst_ycsb free\n");
    alloc_memory.free(requests[i],sizeof(rqst_ycsb));
  }
*/
  requests.release();
}

uint64_t QueryMsgYCSBcl::get_size() {
  uint64_t size = ClientQryMsg::get_size();
  size += sizeof(size_t);
  size += sizeof(rqst_ycsb) * requests.size();
  return size;
}

void QueryMsgYCSBcl::copy_from_query(BaseQry * query) {
  ClientQryMsg::copy_from_query(query);
/*
  requests.init(g_per_qry_req);
  for(uint64_t i = 0; i < ((QryYCSB*)(query))->requests.size(); i++) {
      QryYCSB::copy_request_to_msg(((QryYCSB*)(query)),this,i);
  }
*/
  requests.copy(((QryYCSB*)(query))->requests);
}


void QueryMsgYCSBcl::copy_from_txn(TxnMgr * txn) {
  ClientQryMsg::mcopy_from_txn(txn);
/*
  requests.init(g_per_qry_req);
  for(uint64_t i = 0; i < ((QryYCSB*)(txn->query))->requests.size(); i++) {
      QryYCSB::copy_request_to_msg(((QryYCSB*)(txn->query)),this,i);
  }
*/
  requests.copy(((QryYCSB*)(txn->query))->requests);
}

void QueryMsgYCSBcl::copy_to_txn(TxnMgr * txn) {
  // this only copies over the pointers, so if requests are freed, we'll lose the request data
  ClientQryMsg::copy_to_txn(txn);
  // Copies pointers to txn
  ((QryYCSB*)(txn->query))->requests.append(requests);
/*
  for(uint64_t i = 0; i < requests.size(); i++) {
      QryYCSB::copy_request_to_qry(((QryYCSB*)(txn->query)),this,i);
  }
*/
}

void QueryMsgYCSBcl::copy_from_buf(char * buf) {
  ClientQryMsg::copy_from_buf(buf);
  uint64_t ptr = ClientQryMsg::get_size();
  size_t size;

  COPY_VAL(size,buf,ptr);
  requests.init(size);

  for(uint64_t i = 0 ; i < size;i++) {
    DEBUG_M("QueryMsgYCSBcl::copy rqst_ycsb alloc\n");
    rqst_ycsb * req = (rqst_ycsb*)alloc_memory.alloc(sizeof(rqst_ycsb));
    COPY_VAL(*req,buf,ptr);

    assert(req->key < g_table_size_synth);
    requests.add(req);
  }
 assert(ptr == get_size());
}

void QueryMsgYCSBcl::copy_to_buf(char * buf) {
  ClientQryMsg::copy_to_buf(buf);
  uint64_t ptr = ClientQryMsg::get_size();

  size_t size = requests.size();
  COPY_BUF(buf,size,ptr);

  for(uint64_t i = 0; i < requests.size(); i++) {
    rqst_ycsb * req = requests[i];
    assert(req->key < g_table_size_synth);
    COPY_BUF(buf,*req,ptr);

  }
 assert(ptr == get_size());
}
/************************/

void QueryMsgClientTPCC::init() {
}

void QueryMsgClientTPCC::release() {
  ClientQryMsg::release();
  // Freeing requests is the responsibility of txn
  /*
  for(uint64_t i = 0; i < items.size(); i++) {
    DEBUG_M("QueryMsgClientTPCC::release item free\n");
    alloc_memory.free(items[i],sizeof(Item_no));
  }
  */
  items.release();
}

uint64_t QueryMsgClientTPCC::get_size() {
  uint64_t size = ClientQryMsg::get_size();
  size += sizeof(uint64_t) * 10; 
  size += sizeof(char) * LASTNAME_LEN; 
  size += sizeof(bool) * 3;
  size += sizeof(size_t);
  size += sizeof(Item_no) * items.size();
  return size;
}

void QueryMsgClientTPCC::copy_from_query(BaseQry * query) {
  ClientQryMsg::copy_from_query(query);
  QryTPCC* tpcc_query = (QryTPCC*)(query);
  
  txn_type = tpcc_query->txn_type;
	// common txn input for both payment & new-order
  w_id = tpcc_query->w_id;
  d_id = tpcc_query->d_id;
  c_id = tpcc_query->c_id;

  // payment
  d_w_id = tpcc_query->d_w_id;
  c_w_id = tpcc_query->c_w_id;
  c_d_id = tpcc_query->c_d_id;
  strcpy(c_last,tpcc_query->c_last);
  h_amount = tpcc_query->h_amount;
  by_last_name = tpcc_query->by_last_name;

  // new order
  items.copy(tpcc_query->items);
  rbk = tpcc_query->rbk;
  remote = tpcc_query->remote;
  ol_cnt = tpcc_query->ol_cnt;
  o_entry_d = tpcc_query->o_entry_d;
}


void QueryMsgClientTPCC::copy_from_txn(TxnMgr * txn) {
  ClientQryMsg::mcopy_from_txn(txn);
  copy_from_query(txn->query);
}

void QueryMsgClientTPCC::copy_to_txn(TxnMgr * txn) {
  ClientQryMsg::copy_to_txn(txn);
  QryTPCC* tpcc_query = (QryTPCC*)(txn->query);

  txn->client_id = return_node_id;


  tpcc_query->txn_type = (TPCCTxnType)txn_type;
  if(tpcc_query->txn_type == TPCC_PAYMENT)
    ((TxnManTPCC*)txn)->state = TPCC_PAYMENT0;
  else if (tpcc_query->txn_type == TPCC_NEW_ORDER) 
    ((TxnManTPCC*)txn)->state = TPCC_NEWORDER0;
	// common txn input for both payment & new-order
  tpcc_query->w_id = w_id;
  tpcc_query->d_id = d_id;
  tpcc_query->c_id = c_id;

  // payment
  tpcc_query->d_w_id = d_w_id;
  tpcc_query->c_w_id = c_w_id;
  tpcc_query->c_d_id = c_d_id;
  strcpy(tpcc_query->c_last,c_last);
  tpcc_query->h_amount = h_amount;
  tpcc_query->by_last_name = by_last_name;

  // new order
  tpcc_query->items.append(items);
  tpcc_query->rbk = rbk;
  tpcc_query->remote = remote;
  tpcc_query->ol_cnt = ol_cnt;
  tpcc_query->o_entry_d = o_entry_d;

}

void QueryMsgClientTPCC::copy_from_buf(char * buf) {
  ClientQryMsg::copy_from_buf(buf);
  uint64_t ptr = ClientQryMsg::get_size();

  COPY_VAL(txn_type,buf,ptr); 
	// common txn input for both payment & new-order
  COPY_VAL(w_id,buf,ptr);
  COPY_VAL(d_id,buf,ptr);
  COPY_VAL(c_id,buf,ptr);

  // payment
  COPY_VAL(d_w_id,buf,ptr);
  COPY_VAL(c_w_id,buf,ptr);
  COPY_VAL(c_d_id,buf,ptr);
	COPY_VAL(c_last,buf,ptr);
  COPY_VAL(h_amount,buf,ptr);
  COPY_VAL(by_last_name,buf,ptr);

  // new order
  size_t size;
  COPY_VAL(size,buf,ptr);
  items.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    DEBUG_M("QueryMsgClientTPCC::copy_from_buf item alloc\n");
    Item_no * item = (Item_no*)alloc_memory.alloc(sizeof(Item_no));
    COPY_VAL(*item,buf,ptr);
    items.add(item);
  }

  COPY_VAL(rbk,buf,ptr);
  COPY_VAL(remote,buf,ptr);
  COPY_VAL(ol_cnt,buf,ptr);
  COPY_VAL(o_entry_d,buf,ptr);

 assert(ptr == get_size());
}

void QueryMsgClientTPCC::copy_to_buf(char * buf) {
  ClientQryMsg::copy_to_buf(buf);
  uint64_t ptr = ClientQryMsg::get_size();

  COPY_BUF(buf,txn_type,ptr); 
	// common txn input for both payment & new-order
  COPY_BUF(buf,w_id,ptr);
  COPY_BUF(buf,d_id,ptr);
  COPY_BUF(buf,c_id,ptr);

  // payment
  COPY_BUF(buf,d_w_id,ptr);
  COPY_BUF(buf,c_w_id,ptr);
  COPY_BUF(buf,c_d_id,ptr);
	COPY_BUF(buf,c_last,ptr);
  COPY_BUF(buf,h_amount,ptr);
  COPY_BUF(buf,by_last_name,ptr);

  size_t size = items.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < items.size(); i++) {
    Item_no * item = items[i];
    COPY_BUF(buf,*item,ptr);
  }

  COPY_BUF(buf,rbk,ptr);
  COPY_BUF(buf,remote,ptr);
  COPY_BUF(buf,ol_cnt,ptr);
  COPY_BUF(buf,o_entry_d,ptr);
 assert(ptr == get_size());
}


/***************DA zone*********/
void DAClientQueryMessage::init() {}
void DAClientQueryMessage::copy_from_query(BaseQry* query) {
  ClientQryMsg::copy_from_query(query);
  DAQuery* da_query = (DAQuery*)(query);

  txn_type= da_query->txn_type;
	trans_id= da_query->trans_id;
	item_id= da_query->item_id;
	seq_id= da_query->seq_id;
	write_version=da_query->write_version;
  state= da_query->state;
	next_state= da_query->next_state;
	last_state= da_query->last_state;
}
void DAClientQueryMessage::copy_to_buf(char* buf) {
  ClientQryMsg::copy_to_buf(buf);
  uint64_t ptr = ClientQryMsg::get_size();

  COPY_BUF(buf, txn_type, ptr);
  COPY_BUF(buf, trans_id, ptr);
  COPY_BUF(buf, item_id, ptr);
  COPY_BUF(buf, seq_id, ptr);
  COPY_BUF(buf, write_version, ptr);
  COPY_BUF(buf, state, ptr);
  COPY_BUF(buf, next_state, ptr);
  COPY_BUF(buf, last_state, ptr);
  assert(ptr == get_size());
}
void DAClientQueryMessage::copy_from_txn(TxnMgr* txn) {
  ClientQryMsg::mcopy_from_txn(txn);
  copy_from_query(txn->query);
}

void DAClientQueryMessage::copy_from_buf(char* buf) {
  ClientQryMsg::copy_from_buf(buf);
  uint64_t ptr = ClientQryMsg::get_size();

  COPY_VAL(txn_type, buf, ptr);
  // common txn input for both payment & new-order
  COPY_VAL(trans_id, buf, ptr);
  COPY_VAL(item_id, buf, ptr);
  COPY_VAL(seq_id, buf, ptr);
  COPY_VAL(write_version, buf, ptr);
  // payment
  COPY_VAL(state, buf, ptr);
  COPY_VAL(next_state, buf, ptr);
  COPY_VAL(last_state, buf, ptr);
  assert(ptr == get_size());
}

void DAClientQueryMessage::copy_to_txn(TxnMgr* txn) {
  ClientQryMsg::copy_to_txn(txn);
  DAQuery* da_query = (DAQuery*)(txn->query);


  txn->client_id = return_node_id;
  da_query->txn_type = (DATxnType)txn_type;
  da_query->trans_id = trans_id;
  da_query->item_id = item_id;
  da_query->seq_id = seq_id;
  da_query->write_version = write_version;
  da_query->state = state;
  da_query->next_state = next_state;
  da_query->last_state = last_state;

}

uint64_t DAClientQueryMessage::get_size() {
  uint64_t size = ClientQryMsg::get_size();
  size += sizeof(DATxnType);
  size += sizeof(uint64_t) * 7;
  return size;

}
void DAClientQueryMessage::release() { ClientQryMsg::release(); }

/************************/

void ClientQryMsg::init() {
    first_startts = 0;
}

void ClientQryMsg::release() {
  parts_assign.release();
  first_startts = 0;
}

uint64_t ClientQryMsg::get_size() {
  uint64_t size = Msg::mget_size();
  size += sizeof(client_start_ts);
  /*
  uint64_t size = sizeof(ClientQryMsg);
  */
  size += sizeof(size_t);
  size += sizeof(uint64_t) * parts_assign.size();
  return size;
}

void ClientQryMsg::copy_from_query(BaseQry * query) {
  parts_assign.clear();
  parts_assign.copy(query->parts_assign);
}

void ClientQryMsg::copy_from_txn(TxnMgr * txn) {
  Msg::mcopy_from_txn(txn);
  //ts = txn->txn->time_stamp;
  parts_assign.clear();
  parts_assign.copy(txn->query->parts_assign);
  client_start_ts = txn->client_start_ts;
}

void ClientQryMsg::copy_to_txn(TxnMgr * txn) {
  Msg::mcopy_to_txn(txn);
  //txn->txn->time_stamp = ts;
  txn->query->parts_assign.clear();
  txn->query->parts_assign.append(parts_assign);
  txn->client_start_ts = client_start_ts;
  txn->client_id = return_node_id;
}

void ClientQryMsg::copy_from_buf(char * buf) {
  Msg::mcopy_from_buf(buf);
  uint64_t ptr = Msg::mget_size();
  //COPY_VAL(ts,buf,ptr);
  COPY_VAL(client_start_ts,buf,ptr);
  size_t size;
  COPY_VAL(size,buf,ptr);
  parts_assign.init(size);
  for(uint64_t i = 0; i < size; i++) {
    //COPY_VAL(parts_assign[i],buf,ptr);
    uint64_t part;
    COPY_VAL(part,buf,ptr);
    parts_assign.add(part);
  }
}

void ClientQryMsg::copy_to_buf(char * buf) {
  Msg::mcopy_to_buf(buf);
  uint64_t ptr = Msg::mget_size();
  //COPY_BUF(buf,ts,ptr);
  COPY_BUF(buf,client_start_ts,ptr);
  size_t size = parts_assign.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < size; i++) {
    uint64_t part = parts_assign[i];
    COPY_BUF(buf,part,ptr);
  }
}

/************************/


uint64_t ClientRspMessage::get_size() {
  uint64_t size = Msg::mget_size();
  size += sizeof(uint64_t);
  return size;
}

void ClientRspMessage::copy_from_txn(TxnMgr * txn) {
  Msg::mcopy_from_txn(txn);
  client_start_ts = txn->client_start_ts;
}

void ClientRspMessage::copy_to_txn(TxnMgr * txn) {
  Msg::mcopy_to_txn(txn);
  txn->client_start_ts = client_start_ts;
}

void ClientRspMessage::copy_from_buf(char * buf) {
  Msg::mcopy_from_buf(buf);
  uint64_t ptr = Msg::mget_size();
  COPY_VAL(client_start_ts,buf,ptr);
 assert(ptr == get_size());
}

void ClientRspMessage::copy_to_buf(char * buf) {
  Msg::mcopy_to_buf(buf);
  uint64_t ptr = Msg::mget_size();
  COPY_BUF(buf,client_start_ts,ptr);
 assert(ptr == get_size());
}

/************************/


uint64_t DoneMessage::get_size() {
  uint64_t size = Msg::mget_size();
  return size;
}

void DoneMessage::copy_from_txn(TxnMgr * txn) {
  Msg::mcopy_from_txn(txn);
}

void DoneMessage::copy_to_txn(TxnMgr * txn) {
  Msg::mcopy_to_txn(txn);
}

void DoneMessage::copy_from_buf(char * buf) {
  Msg::mcopy_from_buf(buf);
  uint64_t ptr = Msg::mget_size();
 assert(ptr == get_size());
}

void DoneMessage::copy_to_buf(char * buf) {
  Msg::mcopy_to_buf(buf);
  uint64_t ptr = Msg::mget_size();
 assert(ptr == get_size());
}

/************************/


uint64_t ForwardMessage::get_size() {
  uint64_t size = Msg::mget_size();
  size += sizeof(RC);
#if WORKLOAD == TPCC
	size += sizeof(uint64_t);
#endif
  return size;
}

void ForwardMessage::copy_from_txn(TxnMgr * txn) {
  Msg::mcopy_from_txn(txn);
  rc = txn->get_rc();
#if WORKLOAD == TPCC
  o_id = ((QryTPCC*)txn->query)->o_id;
#endif
}

void ForwardMessage::copy_to_txn(TxnMgr * txn) {
  // Don't copy return ID
  //Msg::mcopy_to_txn(txn);
#if WORKLOAD == TPCC
  ((QryTPCC*)txn->query)->o_id = o_id;
#endif
}

void ForwardMessage::copy_from_buf(char * buf) {
  Msg::mcopy_from_buf(buf);
  uint64_t ptr = Msg::mget_size();
  COPY_VAL(rc,buf,ptr);
#if WORKLOAD == TPCC
  COPY_VAL(o_id,buf,ptr);
#endif
 assert(ptr == get_size());
}

void ForwardMessage::copy_to_buf(char * buf) {
  Msg::mcopy_to_buf(buf);
  uint64_t ptr = Msg::mget_size();
  COPY_BUF(buf,rc,ptr);
#if WORKLOAD == TPCC
  COPY_BUF(buf,o_id,ptr);
#endif
 assert(ptr == get_size());
}

/************************/

uint64_t PrepareMessage::get_size() {
  uint64_t size = Msg::mget_size();
  //size += sizeof(uint64_t);
  return size;
}

void PrepareMessage::copy_from_txn(TxnMgr * txn) {
  Msg::mcopy_from_txn(txn);
}

void PrepareMessage::copy_to_txn(TxnMgr * txn) {
  Msg::mcopy_to_txn(txn);
}

void PrepareMessage::copy_from_buf(char * buf) {
  Msg::mcopy_from_buf(buf);
  uint64_t ptr = Msg::mget_size();
 assert(ptr == get_size());
}

void PrepareMessage::copy_to_buf(char * buf) {
  Msg::mcopy_to_buf(buf);
  uint64_t ptr = Msg::mget_size();
 assert(ptr == get_size());
}

/************************/

uint64_t AckMessage::get_size() {
  uint64_t size = Msg::mget_size();
  size += sizeof(RC);
#if ALGO == OCCTEMPLATE
  size += sizeof(uint64_t) * 2;
#endif
#if WORKLOAD == PPS && ALGO == CALVIN
  size += sizeof(size_t);
  size += sizeof(uint64_t) * part_keys.size();
#endif
  return size;
}

void AckMessage::copy_from_txn(TxnMgr * txn) {
  Msg::mcopy_from_txn(txn);
  //rc = query->rc;
  rc = txn->get_rc();
#if ALGO == OCCTEMPLATE
  lower = txn_timestamp_bounds.get_lower(txn->read_thd_id(),txn->read_txn_id());
  upper = txn_timestamp_bounds.get_upper(txn->read_thd_id(),txn->read_txn_id());
#endif

}

void AckMessage::copy_to_txn(TxnMgr * txn) {
  Msg::mcopy_to_txn(txn);
  //query->rc = rc;

}

void AckMessage::copy_from_buf(char * buf) {
  Msg::mcopy_from_buf(buf);
  uint64_t ptr = Msg::mget_size();
  COPY_VAL(rc,buf,ptr);
#if ALGO == OCCTEMPLATE
  COPY_VAL(lower,buf,ptr);
  COPY_VAL(upper,buf,ptr);
#endif
#if WORKLOAD == PPS && ALGO == CALVIN

  size_t size;
  COPY_VAL(size,buf,ptr);
  part_keys.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    uint64_t item;
    COPY_VAL(item,buf,ptr);
    part_keys.add(item);
  }
#endif
 assert(ptr == get_size());
}

void AckMessage::copy_to_buf(char * buf) {
  Msg::mcopy_to_buf(buf);
  uint64_t ptr = Msg::mget_size();
  COPY_BUF(buf,rc,ptr);
#if ALGO == OCCTEMPLATE
  COPY_BUF(buf,lower,ptr);
  COPY_BUF(buf,upper,ptr);
#endif
#if WORKLOAD == PPS && ALGO == CALVIN

  size_t size = part_keys.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < part_keys.size(); i++) {
    uint64_t item = part_keys[i];
    COPY_BUF(buf,item,ptr);
  }
#endif
 assert(ptr == get_size());
}

/************************/

uint64_t QueryResponseMessage::get_size() {
  uint64_t size = Msg::mget_size(); 
  size += sizeof(RC);
  //size += sizeof(uint64_t);
  return size;
}

void QueryResponseMessage::copy_from_txn(TxnMgr * txn) {
  Msg::mcopy_from_txn(txn);
  rc = txn->get_rc();

}

void QueryResponseMessage::copy_to_txn(TxnMgr * txn) {
  Msg::mcopy_to_txn(txn);
  //query->rc = rc;

}

void QueryResponseMessage::copy_from_buf(char * buf) {
  Msg::mcopy_from_buf(buf);
  uint64_t ptr = Msg::mget_size();
  COPY_VAL(rc,buf,ptr);

 assert(ptr == get_size());
}

void QueryResponseMessage::copy_to_buf(char * buf) {
  Msg::mcopy_to_buf(buf);
  uint64_t ptr = Msg::mget_size();
  COPY_BUF(buf,rc,ptr);
 assert(ptr == get_size());
}

/************************/



uint64_t FinishMessage::get_size() {
  uint64_t size = Msg::mget_size();
  size += sizeof(uint64_t); 
  size += sizeof(RC); 
  size += sizeof(bool); 
#if ALGO == OCCTEMPLATE
  size += sizeof(uint64_t); 
#endif
  return size;
}

void FinishMessage::copy_from_txn(TxnMgr * txn) {
  Msg::mcopy_from_txn(txn);
  rc = txn->get_rc();
  readonly = txn->query->readonly();
#if ALGO == OCCTEMPLATE
  commit_timestamp = txn->get_commit_timestamp();
#endif
}

void FinishMessage::copy_to_txn(TxnMgr * txn) {
  Msg::mcopy_to_txn(txn);
#if ALGO == OCCTEMPLATE
  txn->commit_timestamp = commit_timestamp;
#endif
}

void FinishMessage::copy_from_buf(char * buf) {
  Msg::mcopy_from_buf(buf);
  uint64_t ptr = Msg::mget_size();
  COPY_VAL(pid,buf,ptr);
  COPY_VAL(rc,buf,ptr);
  COPY_VAL(readonly,buf,ptr);
#if ALGO == OCCTEMPLATE
  COPY_VAL(commit_timestamp,buf,ptr);
#endif
 assert(ptr == get_size());
}

void FinishMessage::copy_to_buf(char * buf) {
  Msg::mcopy_to_buf(buf);
  uint64_t ptr = Msg::mget_size();
  COPY_BUF(buf,pid,ptr);
  COPY_BUF(buf,rc,ptr);
  COPY_BUF(buf,readonly,ptr);
#if ALGO == OCCTEMPLATE
  COPY_BUF(buf,commit_timestamp,ptr);
#endif
 assert(ptr == get_size());
}

/************************/

void LogMessage::release() {
  //log_records.release();
}

uint64_t LogMessage::get_size() {
  uint64_t size = Msg::mget_size();
  //size += sizeof(size_t);
  //size += sizeof(RecordLog) * log_records.size();
  return size;
}

void LogMessage::copy_from_txn(TxnMgr * txn) {
  Msg::mcopy_from_txn(txn);
}

void LogMessage::copy_to_txn(TxnMgr * txn) {
  Msg::mcopy_to_txn(txn);
}

void LogMessage::copy_from_record(RecordLog * record) {
  this->record.copyRecord(record);
  
}


void LogMessage::copy_from_buf(char * buf) {
  Msg::mcopy_from_buf(buf);
  uint64_t ptr = Msg::mget_size();
  COPY_VAL(record,buf,ptr);
 assert(ptr == get_size());
}

void LogMessage::copy_to_buf(char * buf) {
  Msg::mcopy_to_buf(buf);
  uint64_t ptr = Msg::mget_size();
  COPY_BUF(buf,record,ptr);
 assert(ptr == get_size());
}

/************************/

uint64_t RspLogMessage::get_size() {
  uint64_t size = Msg::mget_size();
  return size;
}

void RspLogMessage::copy_from_txn(TxnMgr * txn) {
  Msg::mcopy_from_txn(txn);
}

void RspLogMessage::copy_to_txn(TxnMgr * txn) {
  Msg::mcopy_to_txn(txn);
}

void RspLogMessage::copy_from_buf(char * buf) {
  Msg::mcopy_from_buf(buf);
  //uint64_t ptr = Msg::mget_size();
}

void RspLogMessage::copy_to_buf(char * buf) {
  Msg::mcopy_to_buf(buf);
  //uint64_t ptr = Msg::mget_size();
}



/************************/

uint64_t InitDoneMessage::get_size() {
  uint64_t size = Msg::mget_size();
  return size;
}

void InitDoneMessage::copy_from_txn(TxnMgr * txn) {
}

void InitDoneMessage::copy_to_txn(TxnMgr * txn) {
  Msg::mcopy_to_txn(txn);
}

void InitDoneMessage::copy_from_buf(char * buf) {
  Msg::mcopy_from_buf(buf);
}

void InitDoneMessage::copy_to_buf(char * buf) {
  Msg::mcopy_to_buf(buf);
}

/************************/

void QueryMsgYCSB::init() {
}

void QueryMsgYCSB::release() {
  QueryMessage::release();
  // Freeing requests is the responsibility of txn
/*
  for(uint64_t i = 0; i < requests.size(); i++) {
    DEBUG_M("QueryMsgYCSB::release rqst_ycsb free\n");
    alloc_memory.free(requests[i],sizeof(rqst_ycsb));
  }
*/
  requests.release();
}

uint64_t QueryMsgYCSB::get_size() {
  uint64_t size = QueryMessage::get_size();
  size += sizeof(size_t);
  size += sizeof(rqst_ycsb) * requests.size();
  return size;
}

void QueryMsgYCSB::copy_from_txn(TxnMgr * txn) {
  QueryMessage::copy_from_txn(txn);
  requests.init(g_per_qry_req);
  ((TxnManYCSB*)txn)->copy_remote_requests(this);
  //requests.copy(((QryYCSB*)(txn->query))->requests);
}

void QueryMsgYCSB::copy_to_txn(TxnMgr * txn) {
  QueryMessage::copy_to_txn(txn);
  //((QryYCSB*)(txn->query))->requests.copy(requests);
  ((QryYCSB*)(txn->query))->requests.append(requests);
}


void QueryMsgYCSB::copy_from_buf(char * buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();
  size_t size;
  COPY_VAL(size,buf,ptr);
  assert(size<=g_per_qry_req);
  requests.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    DEBUG_M("QueryMsgYCSB::copy rqst_ycsb alloc\n");
    rqst_ycsb * req = (rqst_ycsb*)alloc_memory.alloc(sizeof(rqst_ycsb));
    COPY_VAL(*req,buf,ptr);
    ASSERT(req->key < g_table_size_synth);
    requests.add(req);
  }
 assert(ptr == get_size());
}

void QueryMsgYCSB::copy_to_buf(char * buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = QueryMessage::get_size();
  size_t size = requests.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < requests.size(); i++) {
    rqst_ycsb * req = requests[i];
    COPY_BUF(buf,*req,ptr);
  }
 assert(ptr == get_size());
}
/************************/

void QueryMsgTPCC::init() {
}

void QueryMsgTPCC::release() {
  QueryMessage::release();
  // Freeing items is the responsibility of txn
  /*
  for(uint64_t i = 0; i < items.size(); i++) {
    DEBUG_M("QueryMsgTPCC::release item free\n");
    alloc_memory.free(items[i],sizeof(Item_no));
  }
  */
  items.release();
}

uint64_t QueryMsgTPCC::get_size() {
  uint64_t size = QueryMessage::get_size();

  size += sizeof(uint64_t); //txn_type
  size += sizeof(uint64_t); //state
  size += sizeof(uint64_t) * 3; // w_id, d_id, c_id

  // Payment
  if(txn_type == TPCC_PAYMENT) {
  
    size += sizeof(uint64_t) * 4; // d_w_id, c_w_id, c_d_id;, h_amount
    size += sizeof(char) * LASTNAME_LEN; // c_last[LASTNAME_LEN]
    size += sizeof(bool); // by_last_name

  }

  // New Order
  if(txn_type == TPCC_NEW_ORDER) {
    size += sizeof(uint64_t) * 2; // ol_cnt, o_entry_d,
    size += sizeof(bool) * 2; // rbk, remote
    size += sizeof(Item_no) * items.size();
    size += sizeof(uint64_t); // items size
  }

  return size;
}

void QueryMsgTPCC::copy_from_txn(TxnMgr * txn) {
  QueryMessage::copy_from_txn(txn);
  QryTPCC* tpcc_query = (QryTPCC*)(txn->query);
  
  txn_type = tpcc_query->txn_type;
  state = (uint64_t)((TxnManTPCC*)txn)->state;
	// common txn input for both payment & new-order
  w_id = tpcc_query->w_id;
  d_id = tpcc_query->d_id;
  c_id = tpcc_query->c_id;

  // payment
  if(txn_type == TPCC_PAYMENT) {
    d_w_id = tpcc_query->d_w_id;
    c_w_id = tpcc_query->c_w_id;
    c_d_id = tpcc_query->c_d_id;
    strcpy(c_last,tpcc_query->c_last);
    h_amount = tpcc_query->h_amount;
    by_last_name = tpcc_query->by_last_name;
  }

  // new order
  //items.copy(tpcc_query->items);
  if(txn_type == TPCC_NEW_ORDER) {
    ((TxnManTPCC*)txn)->copy_remote_items(this);
    rbk = tpcc_query->rbk;
    remote = tpcc_query->remote;
    ol_cnt = tpcc_query->ol_cnt;
    o_entry_d = tpcc_query->o_entry_d;
  }

}

void QueryMsgTPCC::copy_to_txn(TxnMgr * txn) {
  QueryMessage::copy_to_txn(txn);

  QryTPCC* tpcc_query = (QryTPCC*)(txn->query);

  tpcc_query->txn_type = (TPCCTxnType)txn_type;
  ((TxnManTPCC*)txn)->state = (TPCCRemTxnType)state;
	// common txn input for both payment & new-order
  tpcc_query->w_id = w_id;
  tpcc_query->d_id = d_id;
  tpcc_query->c_id = c_id;

  // payment
  if(txn_type == TPCC_PAYMENT) {
    tpcc_query->d_w_id = d_w_id;
    tpcc_query->c_w_id = c_w_id;
    tpcc_query->c_d_id = c_d_id;
    strcpy(tpcc_query->c_last,c_last);
    tpcc_query->h_amount = h_amount;
    tpcc_query->by_last_name = by_last_name;
  }

  // new order
  if(txn_type == TPCC_NEW_ORDER) {
    tpcc_query->items.append(items);
    tpcc_query->rbk = rbk;
    tpcc_query->remote = remote;
    tpcc_query->ol_cnt = ol_cnt;
    tpcc_query->o_entry_d = o_entry_d;
  }


}


void QueryMsgTPCC::copy_from_buf(char * buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_VAL(txn_type,buf,ptr); 
  assert(txn_type == TPCC_PAYMENT || txn_type == TPCC_NEW_ORDER);
  COPY_VAL(state,buf,ptr); 
	// common txn input for both payment & new-order
  COPY_VAL(w_id,buf,ptr);
  COPY_VAL(d_id,buf,ptr);
  COPY_VAL(c_id,buf,ptr);

  // payment
  if(txn_type == TPCC_PAYMENT) {
    COPY_VAL(d_w_id,buf,ptr);
    COPY_VAL(c_w_id,buf,ptr);
    COPY_VAL(c_d_id,buf,ptr);
    COPY_VAL(c_last,buf,ptr);
    COPY_VAL(h_amount,buf,ptr);
    COPY_VAL(by_last_name,buf,ptr);
  }

  // new order
  if(txn_type == TPCC_NEW_ORDER) {
    size_t size;
    COPY_VAL(size,buf,ptr);
    items.init(size);
    for(uint64_t i = 0 ; i < size;i++) {
      DEBUG_M("QueryMsgTPCC::copy item alloc\n");
      Item_no * item = (Item_no*)alloc_memory.alloc(sizeof(Item_no));
      COPY_VAL(*item,buf,ptr);
      items.add(item);
    }

    COPY_VAL(rbk,buf,ptr);
    COPY_VAL(remote,buf,ptr);
    COPY_VAL(ol_cnt,buf,ptr);
    COPY_VAL(o_entry_d,buf,ptr);
  }

 assert(ptr == get_size());

}

void QueryMsgTPCC::copy_to_buf(char * buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_BUF(buf,txn_type,ptr); 
  COPY_BUF(buf,state,ptr); 
	// common txn input for both payment & new-order
  COPY_BUF(buf,w_id,ptr);
  COPY_BUF(buf,d_id,ptr);
  COPY_BUF(buf,c_id,ptr);

  // payment
  if(txn_type == TPCC_PAYMENT) {
    COPY_BUF(buf,d_w_id,ptr);
    COPY_BUF(buf,c_w_id,ptr);
    COPY_BUF(buf,c_d_id,ptr);
    COPY_BUF(buf,c_last,ptr);
    COPY_BUF(buf,h_amount,ptr);
    COPY_BUF(buf,by_last_name,ptr);
  }

  if(txn_type == TPCC_NEW_ORDER) {
    size_t size = items.size();
    COPY_BUF(buf,size,ptr);
    for(uint64_t i = 0; i < items.size(); i++) {
      Item_no * item = items[i];
      COPY_BUF(buf,*item,ptr);
    }

    COPY_BUF(buf,rbk,ptr);
    COPY_BUF(buf,remote,ptr);
    COPY_BUF(buf,ol_cnt,ptr);
    COPY_BUF(buf,o_entry_d,ptr);
  }
 assert(ptr == get_size());

}

//---DAquerymessage zone------------

void DAQueryMessage::init() {}
/*
void DAQueryMessage::copy_from_query(BaseQuery* query) {
  QueryMessage::copy_from_query(query);
  DAQuery* da_query = (DAQuery*)(query);

  txn_type= da_query->txn_type;
	trans_id= da_query->trans_id;
	item_id= da_query->item_id;
	seq_id= da_query->seq_id;
	write_version=da_query->write_version;
  state= da_query->state;
	next_state= da_query->next_state;
	last_state= da_query->last_state;
}*/
void DAQueryMessage::copy_to_buf(char* buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_BUF(buf, txn_type, ptr);
  COPY_BUF(buf, trans_id, ptr);
  COPY_BUF(buf, item_id, ptr);
  COPY_BUF(buf, seq_id, ptr);
  COPY_BUF(buf, write_version, ptr);
  COPY_BUF(buf, state, ptr);
  COPY_BUF(buf, next_state, ptr);
  COPY_BUF(buf, last_state, ptr);

}
void DAQueryMessage::copy_from_txn(TxnMgr* txn) {
  QueryMessage::mcopy_from_txn(txn);
  DAQuery* da_query = (DAQuery*)(txn->query);

  txn_type = da_query->txn_type;
  trans_id = da_query->trans_id;
  item_id = da_query->item_id;
  seq_id = da_query->seq_id;
  write_version = da_query->write_version;
  state = da_query->state;
  next_state = da_query->next_state;
  last_state = da_query->last_state;
}

void DAQueryMessage::copy_from_buf(char* buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_VAL(txn_type, buf, ptr);
  // common txn input for both payment & new-order
  COPY_VAL(trans_id, buf, ptr);
  COPY_VAL(item_id, buf, ptr);
  COPY_VAL(seq_id, buf, ptr);
  COPY_VAL(write_version, buf, ptr);
  // payment
  COPY_VAL(state, buf, ptr);
  COPY_VAL(next_state, buf, ptr);
  COPY_VAL(last_state, buf, ptr);
  assert(ptr == get_size());
}

void DAQueryMessage::copy_to_txn(TxnMgr* txn) {
  QueryMessage::copy_to_txn(txn);
  DAQuery* da_query = (DAQuery*)(txn->query);


  txn->client_id = return_node_id;
  da_query->txn_type = (DATxnType)txn_type;
  da_query->trans_id = trans_id;
  da_query->item_id = item_id;
  da_query->seq_id = seq_id;
  da_query->write_version = write_version;
  da_query->state = state;
  da_query->next_state = next_state;
  da_query->last_state = last_state;

}

uint64_t DAQueryMessage::get_size() {
  uint64_t size = QueryMessage::get_size();
  size += sizeof(DATxnType);
  size += sizeof(uint64_t) * 7;
  return size;
}
void DAQueryMessage::release() { QueryMessage::release(); }
