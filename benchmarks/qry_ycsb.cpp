
#include "query.h"
#include "qry_ycsb.h"
#include "mem_alloc.h"
#include "workload.h"
#include "ycsb.h"
#include "table.h"
#include "universal.h"
#include "message.h"

uint64_t QueryGenYCSB::the_n = 0;
double QueryGenYCSB::denom = 0;

void QueryGenYCSB::init() {
	mrand = (myrand *) alloc_memory.alloc(sizeof(myrand));
	mrand->init(acquire_ts());
  if (SKEW_METHOD == ZIPF) {
    zeta_2_theta = zeta(2, g_theta_zipf);
    uint64_t table_size = g_table_size_synth / g_cnt_part;
    the_n = table_size - 1;
    denom = zeta(the_n, g_theta_zipf);
  }
}

BaseQry * QueryGenYCSB::create_query(WLSchema * h_wl, uint64_t home_partition_id) {
  BaseQry * query;
  if (SKEW_METHOD == HOT) {
    query = gen_requests_hot(home_partition_id, h_wl);
  } else if (SKEW_METHOD == ZIPF){
    assert(the_n != 0);
    query = gen_requests_zipf(home_partition_id, h_wl);
  }

  return query;
}

void QryYCSB::print() {
  
  for(uint64_t i = 0; i < requests.size(); i++) {
    printf("%d %ld, ",requests[i]->acctype,requests[i]->key);
  }
  printf("\n");
  /*
    printf("QryYCSB: %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n"
        ,GET_NODE_ID(requests[0]->key)
        ,GET_NODE_ID(requests[1]->key)
        ,GET_NODE_ID(requests[2]->key)
        ,GET_NODE_ID(requests[3]->key)
        ,GET_NODE_ID(requests[4]->key)
        ,GET_NODE_ID(requests[5]->key)
        ,GET_NODE_ID(requests[6]->key)
        ,GET_NODE_ID(requests[7]->key)
        ,GET_NODE_ID(requests[8]->key)
        ,GET_NODE_ID(requests[9]->key)
        );
        */
}

void QryYCSB::init() {
  requests.init(g_per_qry_req);
  BaseQry::init();
}

void QryYCSB::copy_request_to_msg(QryYCSB * ycsb_query, QueryMsgYCSB * message, uint64_t id) {
/*
  DEBUG_M("QryYCSB::copy_request_to_msg rqst_ycsb alloc\n");
  rqst_ycsb * req = (rqst_ycsb*) alloc_memory.alloc(sizeof(rqst_ycsb));
  req->copy(ycsb_query->requests[id]);
  message->requests.add(req);
*/
  message->requests.add(ycsb_query->requests[id]);

}


void QryYCSB::release_requests() {
  // A bit of a hack to ensure that original requests in client query queue aren't freed
  if(SERVER_GENERATE_QUERIES && requests.size() == g_per_qry_req)
    return;
  for(uint64_t i = 0; i < requests.size(); i++) {
    DEBUG_M("QryYCSB::release() rqst_ycsb free\n");
    alloc_memory.free(requests[i],sizeof(rqst_ycsb));
  }

}

void QryYCSB::reset() {
  BaseQry::clear();
#if ALGO != CALVIN
  release_requests();
#endif
  requests.clear();
}

void QryYCSB::release() {
  BaseQry::release();
  DEBUG_M("QryYCSB::release() free\n");
#if ALGO != CALVIN
  release_requests();
#endif
  requests.release();
}

uint64_t QryYCSB::get_participants(WLSchema * wl) {

  uint64_t participant_cnt = 0;
  assert(related_nodes.size()==0);
  assert(nodes_active.size()==0);
  for(uint64_t i = 0; i < g_cnt_node; i++) {
      related_nodes.add(0);
      nodes_active.add(0);
  }
  assert(related_nodes.size()==g_cnt_node);
  assert(nodes_active.size()==g_cnt_node);
  for(uint64_t i = 0; i < requests.size(); i++) {
    uint64_t req_nid = GET_NODE_ID(((WLYCSB*)wl)->key_to_part(requests[i]->key));
    if(requests[i]->acctype == RD) {
      if(related_nodes[req_nid] == 0)
        ++participant_cnt;
      related_nodes.set(req_nid,1);
    }
    if(requests[i]->acctype == WR)
      nodes_active.set(req_nid,1);
  }
  return participant_cnt;
}

uint64_t QryYCSB::participants(bool *& pps,WLSchema * wl) {
  int n = 0;
  for(uint64_t i = 0; i < g_cnt_node; i++)
    pps[i] = false;

  for(uint64_t i = 0; i < requests.size(); i++) {
    uint64_t req_nid = GET_NODE_ID(((WLYCSB*)wl)->key_to_part(requests[i]->key));
    if(!pps[req_nid])
      n++;
    pps[req_nid] = true;
  }
  return n;
}

std::set<uint64_t> QryYCSB::participants(Msg * message, WLSchema * wl) {
  std::set<uint64_t> participant_set;
  QueryMsgYCSBcl* ycsb_msg = ((QueryMsgYCSBcl*)message);
  for(uint64_t i = 0; i < ycsb_msg->requests.size(); i++) {
    uint64_t req_nid = GET_NODE_ID(((WLYCSB*)wl)->key_to_part(ycsb_msg->requests[i]->key));
    participant_set.insert(req_nid);
  }
  return participant_set;
}

bool QryYCSB::readonly() {
  for(uint64_t i = 0; i < requests.size(); i++) {
    if(requests[i]->acctype == WR) {
      return false;
    }
  }
  return true;
}

// The following algorithm comes from the paper:
// Quickly generating billion-record synthetic databases
// However, it seems there is a small bug. 
// The original paper says zeta(theta, 2.0). But I guess it should be 
// zeta(2.0, theta).
double QueryGenYCSB::zeta(uint64_t n, double theta) {
	double sum = 0;
	for (uint64_t i = 1; i <= n; i++) 
		sum += pow(1.0 / i, theta);
	return sum;
}

uint64_t QueryGenYCSB::zipf(uint64_t n, double theta) {
	assert(this->the_n == n);
	assert(theta == g_theta_zipf);
	double alpha = 1 / (1 - theta);
	double zetan = denom;
	double eta = (1 - pow(2.0 / n, 1 - theta)) / 
		(1 - zeta_2_theta / zetan);
//	double eta = (1 - pow(2.0 / n, 1 - theta)) / 
//		(1 - zeta_2_theta / zetan);
	double u = (double)(mrand->next() % 10000000) / 10000000;
	double uz = u * zetan;
	if (uz < 1) return 1;
	if (uz < 1 + pow(0.5, theta)) return 2;
	return 1 + (uint64_t)(n * pow(eta*u -eta + 1, alpha));
}


BaseQry * QueryGenYCSB::gen_requests_hot(uint64_t home_partition_id, WLSchema * h_wl) {
  QryYCSB * query = (QryYCSB*) alloc_memory.alloc(sizeof(QryYCSB));
  query->requests.init(g_per_qry_req);

	uint64_t access_cnt = 0;
	set<uint64_t> all_keys;
	set<uint64_t> partitions_accessed;
  // double r_mpt = (double)(mrand->next() % 10000) / 10000;		
  uint64_t part_limit;
  // if(r_mpt < g_mpr)
  //   part_limit = g_per_txn_part;
  // else
    part_limit = 1;
  uint64_t hot_key_max = (uint64_t)g_perc_data;
  double r_twr = (double)(mrand->next() % 10000) / 10000;		

	int rid = 0;
	for (UInt32 i = 0; i < g_per_qry_req; i ++) {		
		double r = (double)(mrand->next() % 10000) / 10000;		
    double hot =  (double)(mrand->next() % 10000) / 10000;
    uint64_t partition_id;
		rqst_ycsb * req = (rqst_ycsb*) alloc_memory.alloc(sizeof(rqst_ycsb));
		if (r_twr < g_read_txn_perc || r < g_read_tup_perc) 
			req->acctype = RD;
		else
			req->acctype = WR;

    uint64_t row_id = 0; 
    if ( FIRST_PART_LOCAL && rid == 0) {
      if(hot < g_perc_access) {
        row_id = (uint64_t)(mrand->next() % (hot_key_max/g_cnt_part)) * g_cnt_part + home_partition_id;
      } else {
        uint64_t nrand = (uint64_t)mrand->next();
        row_id = ((nrand % (g_table_size_synth/g_cnt_part - (hot_key_max/g_cnt_part))) + hot_key_max/g_cnt_part) * g_cnt_part + home_partition_id;
      }

      partition_id = row_id % g_cnt_part;
      assert(row_id % g_cnt_part == home_partition_id);
    }
    else {
      while(1) {
        if(hot < g_perc_access) {
          row_id = (uint64_t)(mrand->next() % hot_key_max);
        } else {
          row_id = ((uint64_t)(mrand->next() % (g_table_size_synth - hot_key_max))) + hot_key_max;
        }
        partition_id = row_id % g_cnt_part;

        if(g_ppt_strict && partitions_accessed.size() < part_limit && (partitions_accessed.size() + (g_per_qry_req - rid) >= part_limit) && partitions_accessed.count(partition_id) > 0)
          continue;
        if(partitions_accessed.count(partition_id) > 0)
          break;
        if(partitions_accessed.size() == part_limit)
          continue;
        break;
      }
    }
    partitions_accessed.insert(partition_id);
		assert(row_id < g_table_size_synth);
		uint64_t primary_key = row_id;
		//uint64_t part_id = row_id % g_cnt_part;
		req->key = primary_key;
		req->value = mrand->next() % (1<<8);
		// Make sure a single row is not accessed twice
		if (all_keys.find(req->key) == all_keys.end()) {
			all_keys.insert(req->key);
			access_cnt ++;
		} else {
      // Need to have the full g_per_qry_req amount
      i--;
      continue;
    }
		rid ++;

    //query->requests.push_back(*req);
    query->requests.add(req);
	}
  assert(query->requests.size() == g_per_qry_req);
	// Sort the requests in key order.
	if (g_key_order) {
    for(uint64_t i = 0; i < query->requests.size(); i++) {
      for(uint64_t j = query->requests.size() - 1; j > i ; j--) {
        if(query->requests[j]->key < query->requests[j-1]->key) {
          query->requests.swap(j,j-1);
        }
      }
    }
    //std::sort(query->requests.begin(),query->requests.end(),[](rqst_ycsb lhs, rqst_ycsb rhs) { return lhs.key < rhs.key;});
	}
  query->parts_assign.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->parts_assign.add(*it);
  }

  return query;

}

BaseQry * QueryGenYCSB::gen_requests_zipf(uint64_t home_partition_id, WLSchema * h_wl) {
  QryYCSB * query = (QryYCSB*) alloc_memory.alloc(sizeof(QryYCSB));
  new(query) QryYCSB();
  query->requests.init(g_per_qry_req);

	uint64_t access_cnt = 0;
	set<uint64_t> all_keys;
	set<uint64_t> partitions_accessed;
  uint64_t table_size = g_table_size_synth / g_cnt_part;

  double r_twr = (double)(mrand->next() % 10000) / 10000;		

	int rid = 0;
	for (UInt32 i = 0; i < g_per_qry_req; i ++) {		
		double r = (double)(mrand->next() % 10000) / 10000;		
    uint64_t partition_id;
    // if ( FIRST_PART_LOCAL && rid == 0) {
      partition_id = home_partition_id;;
    // } else {
    //   partition_id = mrand->next() % g_cnt_part;
    //     if(g_ppt_strict && g_per_txn_part <= g_cnt_part) {
    //       while( (partitions_accessed.size() < g_per_txn_part &&  partitions_accessed.count(partition_id) > 0) || 
    //           (partitions_accessed.size() == g_per_txn_part &&  partitions_accessed.count(partition_id) == 0)) {
    //         partition_id = mrand->next() % g_cnt_part;
    //       }
    //     }
    // }
		rqst_ycsb * req = (rqst_ycsb*) alloc_memory.alloc(sizeof(rqst_ycsb));
		if (r_twr < g_read_txn_perc || r < g_read_tup_perc) 
			req->acctype = RD;
		else
			req->acctype = WR;
    uint64_t row_id = zipf(table_size - 1, g_theta_zipf);; 
		assert(row_id < table_size);
		uint64_t primary_key = row_id * g_cnt_part + partition_id;
    assert(primary_key < g_table_size_synth);

		req->key = primary_key;
		req->value = mrand->next() % (1<<8);
		// Make sure a single row is not accessed twice
		if (all_keys.find(req->key) == all_keys.end()) {
			all_keys.insert(req->key);
			access_cnt ++;
		} else {
      // Need to have the full g_per_qry_req amount
      i--;
      continue;
    }
    partitions_accessed.insert(partition_id);
		rid ++;

    query->requests.add(req);
	}
  assert(query->requests.size() == g_per_qry_req);
	// Sort the requests in key order.
	if (g_key_order) {
    for(uint64_t i = 0; i < query->requests.size(); i++) {
      for(uint64_t j = query->requests.size() - 1; j > i ; j--) {
        if(query->requests[j]->key < query->requests[j-1]->key) {
          query->requests.swap(j,j-1);
        }
      }
    }
    //std::sort(query->requests.begin(),query->requests.end(),[](rqst_ycsb lhs, rqst_ycsb rhs) { return lhs.key < rhs.key;});
	}
  query->parts_assign.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->parts_assign.add(*it);
  }

  //query->print();
  return query;

}
