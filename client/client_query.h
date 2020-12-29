
#ifndef _CLIENT_QUERY_H_
#define _CLIENT_QUERY_H_

#include "global.h"
#include "universal.h"
#include "query.h"

class WLSchema;
class QryYCSB;
class Qry_client_ycsb;
class QryTPCC;
class Qry_client_tpcc;

// We assume a separate task queue for each thread in order to avoid 
// contention in a centralized query queue. In reality, more sophisticated 
// queue model might be implemented.
class Qry_queue_client {
public:
	void init(WLSchema * h_wl);
  bool done(); 
	BaseQry * get_next_query(uint64_t server_id,uint64_t thd_id);
  void initQueriesParallel(uint64_t thd_id);
  static void * initQueriesHelper(void * context);
	
private:
	WLSchema * _wl;
  uint64_t size;
  std::vector<std::vector<BaseQry*>> queries;
  uint64_t ** query_cnt;
  volatile uint64_t next_tid;
};

#endif
