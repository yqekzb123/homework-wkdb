
#ifndef _QryYCSB_H_
#define _QryYCSB_H_

#include "global.h"
#include "universal.h"
#include "query.h"
#include "array.h"

class WLSchema;
class Msg;
class QueryMsgYCSB;
class QueryMsgYCSBcl;


// Each QryYCSB contains several rqst_ycsbs, 
// each of which is a RD, WR or SCAN 
// to a single table

class rqst_ycsb {
public:
  rqst_ycsb() {}
  rqst_ycsb(const rqst_ycsb& req) : acctype(req.acctype), key(req.key), value(req.value) { }
  void copy(rqst_ycsb * req) {
    this->acctype = req->acctype;
    this->key = req->key;
    this->value = req->value;
  }
//	char table_name[80];
	access_t acctype; 
	uint64_t key;
	char value;
	// only for (qtype == SCAN)
	//UInt32 scan_len;
};

class QueryGenYCSB : public QryGenerator {
public:
  void init();
  BaseQry * create_query(WLSchema * h_wl, uint64_t home_partition_id);

private:
	BaseQry * gen_requests_hot(uint64_t home_partition_id, WLSchema * h_wl);
	BaseQry * gen_requests_zipf(uint64_t home_partition_id, WLSchema * h_wl);
	// for Zipfian distribution
	double zeta(uint64_t n, double theta);
	uint64_t zipf(uint64_t n, double theta);
	
	myrand * mrand;
	static uint64_t the_n;
	static double denom;
	double zeta_2_theta;


};

class QryYCSB : public BaseQry {
public:
  QryYCSB() {
  }
  ~QryYCSB() {
  }

  void print();
  
	void init(uint64_t thd_id, WLSchema * h_wl) {};
  void init();
  void release();
  void release_requests();
  void reset();
  uint64_t get_participants(WLSchema * wl); 
  static std::set<uint64_t> participants(Msg * message, WLSchema * wl); 
  static void copy_request_to_msg(QryYCSB * ycsb_query, QueryMsgYCSB * message, uint64_t id); 
  uint64_t participants(bool *& pps,WLSchema * wl); 
  bool readonly();
	

  //std::vector<rqst_ycsb> requests;
  Array<rqst_ycsb*> requests;
  /*
  uint64_t rid;
  uint64_t access_cnt;
	uint64_t request_cnt;
	uint64_t req_i;
  rqst_ycsb req;
  uint64_t rqry_req_cnt;
  */

};

#endif
