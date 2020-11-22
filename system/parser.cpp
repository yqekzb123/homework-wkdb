
#include "global.h"
#include "universal.h"

void print_usage() {
	printf("[usage]:\n");
	printf("\t-nidINT       ; NODE_ID\n");
	printf("\t-pINT       ; PART_CNT\n");
	printf("\t-nINT       ; NODE_CNT\n");
	printf("\t-cnINT       ; CLIENT_NODE_CNT\n");

	printf("\t-vINT       ; VIRTUAL_PART_CNT\n");

	printf("\t-tINT       ; THREAD_CNT\n");
	printf("\t-trINT       ; REM_THREAD_CNT\n");
	printf("\t-tsINT       ; SEND_THREAD_CNT\n");
	printf("\t-ctINT       ; CLIENT_THREAD_CNT\n");
	printf("\t-ctrINT       ; CLIENT_REM_THREAD_CNT\n");
	printf("\t-ctsINT       ; CLIENT_SEND_THREAD_CNT\n");

	printf("\t-tppINT       ; MAX_TXN_PER_PART\n");
	printf("\t-tifINT       ; MAX_TXN_IN_FLIGHT\n");
	printf("\t-mprINT       ; MPR\n");
	printf("\t-mpiINT       ; MPIR\n");
	printf("\t-doneINT       ; DONE_TIMER\n");
	printf("\t-btmrINT       ; BATCH_TIMER\n");
	printf("\t-stmrINT       ; SEQ_BATCH_TIMER\n");
	printf("\t-progINT       ; PROG_TIMER\n");
	printf("\t-abrtINT       ; ABORT_PENALTY (ms)\n");

	printf("\t-qINT       ; QUERY_INTVL\n");
	printf("\t-dINT       ; PRT_LAT_DISTR\n");
	printf("\t-aINT       ; PART_ALLOC (0 or 1)\n");
	printf("\t-mINT       ; MEM_PAD (0 or 1)\n");

	printf("\t-rnINT       ; REPLICA_CNT (0+)\n");
	printf("\t-rtINT       ; REPL_TYPE (AA: 1, AP: 2)\n");
	
	printf("\t-o STRING   ; output file\n");
	printf("\t-i STRING   ; input file\n");
	printf("\t-cf STRING   ; txn file\n");
	printf("\t-ndly   ; NETWORK_DELAY\n");
	printf("  [YCSB]:\n");
	printf("\t-dpFLOAT       ; DATA_PERC\n");
	printf("\t-apFLOAT       ; ACCESS_PERC\n");
	printf("\t-pptINT       ; PART_PER_TXN\n");
	printf("\t-spptINT       ; STRICT_PPT\n");
	printf("\t-eINT       ; PERC_MULTI_PART\n");
	printf("\t-wFLOAT     ; WRITE_PERC\n");
	printf("\t-zipfFLOAT     ; ZIPF_THETA\n");
	printf("\t-sINT       ; SYNTH_TABLE_SIZE\n");
	printf("\t-rpqINT       ; REQ_PER_QUERY\n");
	printf("\t-fINT       ; FIELD_PER_TUPLE\n");
	printf("  [TPCC]:\n");
	printf("\t-whINT       ; NUM_WH\n");
	printf("\t-ppFLOAT    ; PERC_PAYMENT\n");
	printf("\t-upINT      ; WH_UPDATE\n");
  
}

void parser(int argc, char * argv[]) {

	for (int i = 1; i < argc; i++) {
		assert(argv[i][0] == '-');
    if (argv[i][1] == 'n' && argv[i][2] == 'd' && argv[i][3] == 'l' && argv[i][4] == 'y')
      g_delay_net = atoi( &argv[i][5] );
    else if (argv[i][1] == 'd' && argv[i][2] == 'o' && argv[i][3] == 'n' && argv[i][4] == 'e')
      g_timer_done = atoi( &argv[i][5] );
    else if (argv[i][1] == 'b' && argv[i][2] == 't' && argv[i][3] == 'm' && argv[i][4] == 'r')
      g_limit_batch_time = atoi( &argv[i][5] );
    else if (argv[i][1] == 's' && argv[i][2] == 't' && argv[i][3] == 'm' && argv[i][4] == 'r')
      g_limit_seq_batch_time = atoi( &argv[i][5] );
    else if (argv[i][1] == 's' && argv[i][2] == 'p' && argv[i][3] == 'p' && argv[i][4] == 't')
      g_ppt_strict = atoi( &argv[i][5] ) == 1;
    else if (argv[i][1] == 'p' && argv[i][2] == 'r' && argv[i][3] == 'o' && argv[i][4] == 'g')
			g_thd_cnt = atoi( &argv[i][5] );
    else if (argv[i][1] == 'a' && argv[i][2] == 'b' && argv[i][3] == 'r' && argv[i][4] == 't')
			g_penalty_abort = atoi( &argv[i][5] );
    else if (argv[i][1] == 'z' && argv[i][2] == 'i' && argv[i][3] == 'p' && argv[i][4] == 'f')
			g_theta_zipf = atof( &argv[i][5] );
    else if (argv[i][1] == 'n' && argv[i][2] == 'i' && argv[i][3] == 'd')
			g_node_id = atoi( &argv[i][4] );
    else if (argv[i][1] == 'c' && argv[i][2] == 't' && argv[i][3] == 'r')
			g_cl_rem_thd_cnt = atoi( &argv[i][4] );
    else if (argv[i][1] == 'c' && argv[i][2] == 't' && argv[i][3] == 's')
			g_cl_send_thd_cnt = atoi( &argv[i][4] );
    else if (argv[i][1] == 'l' && argv[i][2] == 'p' && argv[i][3] == 's')
			g_per_server_load = atoi( &argv[i][4] );
    else if (argv[i][1] == 't' && argv[i][2] == 'p' && argv[i][3] == 'p')
			g_per_part_max_txn = atoi( &argv[i][4] );
    else if (argv[i][1] == 't' && argv[i][2] == 'i' && argv[i][3] == 'f')
			g_max_inflight = atoi( &argv[i][4] );
    else if (argv[i][1] == 'm' && argv[i][2] == 'p' && argv[i][3] == 'r')
			g_mpr = atof( &argv[i][4] );
    else if (argv[i][1] == 'm' && argv[i][2] == 'p' && argv[i][3] == 'i')
			g_mpitem = atof( &argv[i][4] );
    else if (argv[i][1] == 'p' && argv[i][2] == 'p' && argv[i][3] == 't')
      g_per_txn_part = atoi( &argv[i][4] );
    else if (argv[i][1] == 'r' && argv[i][2] == 'p' && argv[i][3] == 'q')
      g_per_qry_req = atoi( &argv[i][4] );
    else if (argv[i][1] == 'c' && argv[i][2] == 'n')
      g_cl_node_cnt = atoi( &argv[i][3] );
    else if (argv[i][1] == 't' && argv[i][2] == 'r')
      g_rem_thd_cnt = atoi( &argv[i][3] );
    else if (argv[i][1] == 't' && argv[i][2] == 's')
      g_send_thd_cnt = atoi( &argv[i][3] );
    else if (argv[i][1] == 'c' && argv[i][2] == 't')
      g_cl_thd_cnt = atoi( &argv[i][3] );
    else if (argv[i][1] == 'w' && argv[i][2] == 'h')
      g_wh_num = atoi( &argv[i][3] );
    else if (argv[i][1] == 'c' && argv[i][2] == 'f')
			txn_file = argv[++i];
    else if (argv[i][1] == 'p' && argv[i][2] == 'p')
      g_payment_perc = atof( &argv[i][3] );
    else if (argv[i][1] == 'u' && argv[i][2] == 'p')
      g_update_wh = atoi( &argv[i][3] );
    else if (argv[i][1] == 'd' && argv[i][2] == 'p')
      g_perc_data = atof( &argv[i][3] );
    else if (argv[i][1] == 'a' && argv[i][2] == 'p')
      g_perc_access = atof( &argv[i][3] );
    else if (argv[i][1] == 'r' && argv[i][2] == 'n')
      g_cnt_repl = atof( &argv[i][3] );
    else if (argv[i][1] == 'r' && argv[i][2] == 't')
      g_type_repl = atof( &argv[i][3] );
    else if (argv[i][1] == 't'&& argv[i][2] == 'w') {
			g_write_txn_perc = atof( &argv[i][3] );
			g_read_txn_perc = 1.0 - atof( &argv[i][3] );
    }
    else if (argv[i][1] == 'p')
      g_cnt_part = atoi( &argv[i][2] );
    else if (argv[i][1] == 'n')
      g_cnt_node = atoi( &argv[i][2] );
    else if (argv[i][1] == 't')
			g_thd_cnt = atoi( &argv[i][2] );
    else if (argv[i][1] == 'q')
			g_intvl_query = atoi( &argv[i][2] );
    else if (argv[i][1] == 'd')
			g_prt_distr_lat = atoi( &argv[i][2] );
    else if (argv[i][1] == 'a')
			g_alloc_part = atoi( &argv[i][2] );
    else if (argv[i][1] == 'm')
			g_pad_mem = atoi( &argv[i][2] );
    else if (argv[i][1] == 'o')
			output_file = argv[++i];
    else if (argv[i][1] == 'i')
			input_file = argv[++i];
    else if (argv[i][1] == 'e')
			g_multi_part_perc = atof( &argv[i][2] );
    else if (argv[i][1] == 'w') {
			g_write_tup_perc = atof( &argv[i][2] );
			g_read_tup_perc = 1.0 - atof( &argv[i][2] );
    }
    else if (argv[i][1] == 's')
			g_table_size_synth = atoi( &argv[i][2] );
    else if (argv[i][1] == 'f')
			g_per_tuple_field = atoi( &argv[i][2] );
		else if (argv[i][1] == 'h') {
			print_usage();
			exit(0);
		} else {
      printf("%s\n",argv[i]);
      fflush(stdout);
			assert(false);
    }
	}
  g_thread_total_cnt = g_thd_cnt + g_rem_thd_cnt + g_send_thd_cnt + g_abort_thread_cnt;
#if LOGGING
  g_thread_total_cnt += g_logger_thread_cnt; // logger thread
#endif
#if ALGO == CALVIN
    g_thread_total_cnt += 2; // sequencer + scheduler thread
  // Remove abort thread
  g_abort_thread_cnt = 0;
  g_thread_total_cnt -= 1;
#endif
  g_total_client_thread_cnt = g_cl_thd_cnt + g_cl_rem_thd_cnt + g_cl_send_thd_cnt;
  g_node_total_cnt = g_cnt_node + g_cl_node_cnt + g_cnt_repl*g_cnt_node;
  if(ISCLIENT) {
    g_this_thread_cnt = g_cl_thd_cnt;
    g_this_rem_thread_cnt = g_cl_rem_thd_cnt;
    g_send_thd_cnt_this = g_cl_send_thd_cnt;
    g_this_total_thread_cnt = g_total_client_thread_cnt;
  } else {
    g_this_thread_cnt = g_thd_cnt;
    g_this_rem_thread_cnt = g_rem_thd_cnt;
    g_send_thd_cnt_this = g_send_thd_cnt;
    g_this_total_thread_cnt = g_thread_total_cnt;
  }

    //g_max_inflight = g_max_inflight / g_cnt_node;
      printf("CC Alg %d\n",ALGO);
      printf("g_timer_done %ld\n",g_timer_done);
			printf("g_thd_cnt %d\n",g_thd_cnt );
			printf("g_penalty_abort %ld\n",g_penalty_abort); 
			printf("g_theta_zipf %f\n",g_theta_zipf );
			printf("g_node_id %d\n",g_node_id );
			printf("g_cl_rem_thd_cnt %d\n",g_cl_rem_thd_cnt );
			printf("g_cl_send_thd_cnt %d\n",g_cl_send_thd_cnt );
			printf("g_per_part_max_txn %d\n",g_per_part_max_txn );
			printf("g_per_server_load %d\n",g_per_server_load );
			printf("g_max_inflight %d\n",g_max_inflight );
			printf("g_mpr %f\n",g_mpr );
			printf("g_mpitem %f\n",g_mpitem );
      printf("g_per_txn_part %d\n",g_per_txn_part );
      printf("g_per_qry_req %d\n",g_per_qry_req );
      printf("g_cl_node_cnt %d\n",g_cl_node_cnt );
      printf("g_rem_thd_cnt %d\n",g_rem_thd_cnt );
      printf("g_send_thd_cnt %d\n",g_send_thd_cnt );
      printf("g_cl_thd_cnt %d\n",g_cl_thd_cnt );
      printf("g_wh_num %d\n",g_wh_num );
      printf("g_payment_perc %f\n",g_payment_perc );
      printf("g_update_wh %d\n",g_update_wh );
      printf("g_cnt_part %d\n",g_cnt_part );
      printf("g_cnt_node %d\n",g_cnt_node );
			printf("g_thd_cnt %d\n",g_thd_cnt );
			printf("g_intvl_query %ld\n",g_intvl_query );
			printf("g_prt_distr_lat %d\n",g_prt_distr_lat );
			printf("g_alloc_part %d\n",g_alloc_part );
			printf("g_pad_mem %d\n",g_pad_mem );
			printf("g_multi_part_perc %f\n",g_multi_part_perc );
			printf("g_read_tup_perc %f\n",g_read_tup_perc );
			printf("g_write_tup_perc %f\n",g_write_tup_perc );
			printf("g_read_txn_perc %f\n",g_read_txn_perc );
			printf("g_write_txn_perc %f\n",g_write_txn_perc );
			printf("g_table_size_synth %ld\n",g_table_size_synth );
			printf("g_per_tuple_field %d\n",g_per_tuple_field );
      printf("g_perc_data %f\n",g_perc_data);
      printf("g_perc_access %f\n",g_perc_access);
      printf("g_ppt_strict %d\n",g_ppt_strict);
      printf("g_delay_net %d\n",g_delay_net);
      printf("g_thread_total_cnt %d\n",g_thread_total_cnt);
      printf("g_total_client_thread_cnt %d\n",g_total_client_thread_cnt);
      printf("g_node_total_cnt %d\n",g_node_total_cnt);
      printf("g_limit_seq_batch_time %ld\n",g_limit_seq_batch_time);

    // Initialize client-specific globals
    if (g_node_id >= g_cnt_node)
        global_init_client();
    init_globals();
}
