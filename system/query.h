
#ifndef _QUERY_H_
#define _QUERY_H_

#include "global.h"
#include "universal.h"
#include "array.h"

class WLSchema;
class QryYCSB;
class QryTPCC;
class PPSQry;
class DAQuery;

class BaseQry {
public:
    virtual ~BaseQry() {}
    virtual bool readonly() = 0;
    virtual void print() = 0;
    virtual void init();
    uint64_t waiting_time;
    void clear();
    void release();
    virtual bool isReconQry() {return false;}

    // Prevent unnecessary remote messages
    Array<uint64_t> parts_assign;
    Array<uint64_t> parts_touched;
    Array<uint64_t> nodes_active;
    Array<uint64_t> related_nodes;

};

class QryGenerator {
public:
    virtual ~QryGenerator() {}
    virtual BaseQry * create_query(WLSchema * h_wl, uint64_t home_partition_id) = 0;
};

// All the queries for a particular thread.
class Qry_thd {
public:
	void init(WLSchema * h_wl, int thd_id);
	BaseQry * get_next_query(); 
	int q_idx;
#if WORKLOAD == YCSB
	QryYCSB * queries;
#elif WORKLOAD == TPCC
	QryTPCC * queries;
#elif WORKLOAD == PPS
	PPSQry * queries;
#elif WORKLOAD == TEST
	QryTPCC * queries;
#elif WORKLOAD == DA
	DAQuery * queries;
#endif
	char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
};

class Qry_queue {
public:
	void init(WLSchema * h_wl);
	void init(int thd_id);
	BaseQry * get_next_query(uint64_t thd_id); 
	
private:
	Qry_thd ** all_queries;
	WLSchema * _wl;
};

#endif
