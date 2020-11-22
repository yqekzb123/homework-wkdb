
#include "query.h"
#include "qry_tpcc.h"
#include "tpcc.h"
#include "universal_tpcc.h"
#include "mem_alloc.h"
#include "workload.h"
#include "table.h"
#include "message.h"

BaseQry * QueryGenTPCC::create_query(WLSchema * h_wl,uint64_t home_partition_id) {
  double x = (double)(rand() % 100) / 100.0;
	if (x < g_payment_perc)
		return gen_payment(home_partition_id);
	else 
		return gen_new_order(home_partition_id);

}

void QryTPCC::init(uint64_t thd_id, WLSchema * h_wl) {
  items.init(g_max_items_per_txn);
  BaseQry::init();
}

void QryTPCC::init() {
  items.init(g_max_items_per_txn);
  BaseQry::init();
}

void QryTPCC::print() {
  
    printf("QryTPCC: %d "
        "w_id: %ld, d_id: %ld, c_id: %ld, d_w_id: %ld, c_w_id: %ld, c_d_id: %ld\n"
        ,(int)txn_type
        ,w_id
        ,d_id
        ,c_id
        ,d_w_id
        ,c_w_id
        ,c_d_id
        );
    if(txn_type == TPCC_NEW_ORDER) {
      printf("items: ");
      for(uint64_t size = 0; size < items.size(); size++) {
        printf("%ld, "
            ,items[size]->ol_i_id);
      }
      printf("\n");
    }
}

std::set<uint64_t> QryTPCC::participants(Msg * message, WLSchema * wl) {
  std::set<uint64_t> participant_set;
  QueryMsgClientTPCC* tpcc_msg = ((QueryMsgClientTPCC*)message);
  uint64_t id;

  id = GET_NODE_ID(wh_to_part(tpcc_msg->w_id));
  participant_set.insert(id);

  switch(tpcc_msg->txn_type) {
    case TPCC_PAYMENT:
      id = GET_NODE_ID(wh_to_part(tpcc_msg->c_w_id));
      participant_set.insert(id);
      break;
    case TPCC_NEW_ORDER: 
      for(uint64_t i = 0; i < tpcc_msg->ol_cnt; i++) {
        uint64_t req_nid = GET_NODE_ID(wh_to_part(tpcc_msg->items[i]->ol_supply_w_id));
        participant_set.insert(req_nid);
      }
      break;
    default: assert(false);
  }

  return participant_set;
}



uint64_t QryTPCC::participants(bool *& pps,WLSchema * wl) {
  int n = 0;
  for(uint64_t i = 0; i < g_cnt_node; i++)
    pps[i] = false;
  uint64_t id;

  switch(txn_type) {
    case TPCC_PAYMENT:
      id = GET_NODE_ID(wh_to_part(w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      id = GET_NODE_ID(wh_to_part(c_w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      break;
    case TPCC_NEW_ORDER: 
      id = GET_NODE_ID(wh_to_part(w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      for(uint64_t i = 0; i < ol_cnt; i++) {
        uint64_t req_nid = GET_NODE_ID(wh_to_part(items[i]->ol_supply_w_id));
        if(!pps[req_nid]) {
          pps[req_nid] = true;
          n++;
        }
      }
      break;
    default: assert(false);
  }

  return n;
}

bool QryTPCC::readonly() {
  return false;
}

BaseQry * QueryGenTPCC::gen_payment(uint64_t home_partition) {
  QryTPCC * query = new QryTPCC;
	set<uint64_t> partitions_accessed;

	query->txn_type = TPCC_PAYMENT;
  uint64_t home_warehouse;
	if (FIRST_PART_LOCAL) {
    while(wh_to_part(home_warehouse = URand(1, g_wh_num)) != home_partition) {}
  }
	else
		home_warehouse = URand(1, g_wh_num);
  query->w_id =  home_warehouse;
	query->d_w_id = home_warehouse;

  partitions_accessed.insert(wh_to_part(query->w_id));

	query->d_id = URand(1, g_dist_per_wh);
	query->h_amount = URand(1, 5000);
  query->rbk = false;
	// double x = (double)(rand() % 10000) / 10000;
	int y = URand(1, 100);

	//if(x > g_mpr) { 
	// if(x > 0.15) { 
		// home warehouse
		query->c_d_id = query->d_id;
		query->c_w_id = query->w_id;
	// } else {	
	// 	// remote warehouse
	// 	query->c_d_id = URand(1, g_dist_per_wh);
	// 	if(g_wh_num > 1) {
	// 		while((query->c_w_id = URand(1, g_wh_num)) == query->w_id) {}
	// 		if (wh_to_part(query->w_id) != wh_to_part(query->c_w_id)) {
  //       partitions_accessed.insert(wh_to_part(query->c_w_id));
	// 		}
	// 	} else 
  //     query->c_w_id = query->w_id;
	// }
	if(y <= 60) {
		// by last name
		query->by_last_name = true;
		Lastname(NURand(255,0,999),query->c_last);
	} else {
		// by cust id
		query->by_last_name = false;
		query->c_id = NURand(1023, 1, g_cust_per_dist);
	}

  query->parts_assign.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->parts_assign.add(*it);
  }
  return query;
}

BaseQry * QueryGenTPCC::gen_new_order(uint64_t home_partition) {
  QryTPCC * query = new QryTPCC;
	set<uint64_t> partitions_accessed;

	query->txn_type = TPCC_NEW_ORDER;
  query->items.init(g_max_items_per_txn);
	if (FIRST_PART_LOCAL) {
    while(wh_to_part(query->w_id = URand(1, g_wh_num)) != home_partition) {}
  }
	else
		query->w_id = URand(1, g_wh_num);

	query->d_id = URand(1, g_dist_per_wh);
	query->c_id = NURand(1023, 1, g_cust_per_dist);
  // TODO TPCC rollback
	//rbk = URand(1, 100) == 1 ? true : false;
	query->rbk = false;
	query->ol_cnt = URand(5, g_max_items_per_txn);
	query->o_entry_d = 2013;

  partitions_accessed.insert(wh_to_part(query->w_id));

  double r_mpr = (double)(rand() % 10000) / 10000;
  uint64_t part_limit;
  // if(r_mpr < g_mpr)
  //   part_limit = g_per_txn_part;
  // else
    part_limit = 1;

  std::set<uint64_t> ol_i_ids;
  while(query->items.size() < query->ol_cnt) {
      Item_no * item = new Item_no;

    while(ol_i_ids.count( item->ol_i_id = NURand(8191, 1, g_max_items)) > 0) {}
    ol_i_ids.insert(item->ol_i_id);
    item->ol_quantity = URand(1, 10);
    double r_rem = (double)(rand() % 100000) / 100000;
		if (r_rem > 0.01 || r_mpr > g_mpr || g_wh_num == 1) {
			// home warehouse
			item->ol_supply_w_id = query->w_id;
    } else {
      if(partitions_accessed.size() < part_limit) {
        item->ol_supply_w_id = URand(1, g_wh_num);
        partitions_accessed.insert(wh_to_part(item->ol_supply_w_id));
      } else {
        // select warehouse from among those already selected
        while( partitions_accessed.count(wh_to_part(item->ol_supply_w_id = URand(1, g_wh_num))) == 0) {}
      }
    }

    query->items.add(item);
  }

  query->parts_assign.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->parts_assign.add(*it);
  }
  return query;

}

uint64_t QryTPCC::get_participants(WLSchema * wl) {
   uint64_t participant_cnt = 0;
   uint64_t active_cnt = 0;
  assert(related_nodes.size()==0);
  assert(nodes_active.size()==0);
  for(uint64_t i = 0; i < g_cnt_node; i++) {
      related_nodes.add(0);
      nodes_active.add(0);
  }
  assert(related_nodes.size()==g_cnt_node);
  assert(nodes_active.size()==g_cnt_node);

  uint64_t home_wh_node;
  home_wh_node = GET_NODE_ID(wh_to_part(w_id));
  related_nodes.set(home_wh_node,1);
  nodes_active.set(home_wh_node,1);
  participant_cnt++;
  active_cnt++;
  if(txn_type == TPCC_PAYMENT) {
      uint64_t req_nid = GET_NODE_ID(wh_to_part(c_w_id));
      if(related_nodes[req_nid] == 0) {
        participant_cnt++;
        related_nodes.set(req_nid,1);
        active_cnt++;
        nodes_active.set(req_nid,1);
      }

  } else if (txn_type == TPCC_NEW_ORDER) {
    for(uint64_t i = 0; i < ol_cnt; i++) {
      uint64_t req_nid = GET_NODE_ID(wh_to_part(items[i]->ol_supply_w_id));
      if(related_nodes[req_nid] == 0) {
        participant_cnt++;
        related_nodes.set(req_nid,1);
        active_cnt++;
        nodes_active.set(req_nid,1);
      }
    }
  }
  return participant_cnt;
}

void QryTPCC::reset() {
  BaseQry::clear();
#if ALGO != CALVIN
  release_items();
#endif
  items.clear();
}

void QryTPCC::release() {
  BaseQry::release();
  DEBUG_M("QryTPCC::release() free\n");
#if ALGO != CALVIN
  release_items();
#endif
  items.release();
}

void QryTPCC::release_items() {
  // A bit of a hack to ensure that original requests in client query queue aren't freed
  if(SERVER_GENERATE_QUERIES)
    return;
  for(uint64_t i = 0; i < items.size(); i++) {
    DEBUG_M("QryTPCC::release() Item_no free\n");
    alloc_memory.free(items[i],sizeof(Item_no));
  }

}
