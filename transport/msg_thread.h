
#ifndef _MSG_THREAD_H_
#define _MSG_THREAD_H_

#include "global.h"
#include "universal.h"
#include "nn.hpp"

struct mbuf {
  char * buffer;
  uint64_t begintime;
  uint64_t ptr;
  uint64_t cnt;
  bool wait;

  void init(uint64_t dest_id) {
    buffer = (char*)nn_allocmsg(g_msg_size,0);
  }
  void reset(uint64_t dest_id) {
    //buffer = (char*)nn_allocmsg(g_msg_size,0);
    //memset(buffer,0,g_msg_size);
    begintime = 0;
    cnt = 0;
    wait = false;
	  ((uint32_t*)buffer)[0] = dest_id;
	  ((uint32_t*)buffer)[1] = g_node_id;
    ptr = sizeof(uint32_t) * 3;
  }
  void copy(char * p, uint64_t s) {
    assert(ptr + s <= g_msg_size);
    if(cnt == 0)
      begintime = acquire_ts();
    COPY_BUF_SIZE(buffer,p,ptr,s);
    //memcpy(&((char*)buffer)[size],p,s);
    //size += s;
  }
  bool fits(uint64_t s) {
    return (ptr + s) <= g_msg_size;
  }
  bool ready() {
    if(cnt == 0)
      return false;
    if( (acquire_ts() - begintime) >= g_msg_time_limit )
      return true;
    return false;
  }
};

class MsgThread {
public:
  void init(uint64_t thd_id);
  void run();
  void check_and_send_batches(); 
  void send_batch(uint64_t dest_node_id); 
  void copy_to_buffer(mbuf * sbuf, RemReqType type, BaseQry * qry); 
  uint64_t get_msg_size(RemReqType type, BaseQry * qry); 
  void rack( mbuf * sbuf,BaseQry * qry);
  void rprepare( mbuf * sbuf,BaseQry * qry);
  void rfin( mbuf * sbuf,BaseQry * qry);
  void cl_rsp(mbuf * sbuf, BaseQry *qry);
  void log_msg(mbuf * sbuf, BaseQry *qry);
  void log_msg_rsp(mbuf * sbuf, BaseQry *qry);
  void rinit(mbuf * sbuf,BaseQry * qry);
  void rqry( mbuf * sbuf, BaseQry *qry);
  void rfwd( mbuf * sbuf, BaseQry *qry);
  void rdone( mbuf * sbuf, BaseQry *qry);
  void rqry_rsp( mbuf * sbuf, BaseQry *qry);
  void rtxn(mbuf * sbuf, BaseQry *qry);
  void rtxn_seq(mbuf * sbuf, BaseQry *qry);
  uint64_t read_thd_id() { return _thd_id;}
private:
  mbuf ** buffer;
  uint64_t buffer_cnt;
  uint64_t _thd_id;

};

#endif
