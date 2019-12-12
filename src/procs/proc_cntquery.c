/*
No copyright is claimed in the United States under Title 17, U.S. Code.
All Other Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
/* Allows query to see if counters for a key are positive or set to some minimum
 * value
 *
 * used to determine if something "mostly" occurs
 *
 * it has 3 ports...  a increment, a decrement and an query
 *
 */

#define PROC_NAME "cntquery"

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <math.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "stringhash5.h"
#include "procloader.h"
#include "sysutil.h"
#include "datatypes/wsdt_ts.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
// Use to fix proc_tags: 
//char *proc_menus[]             =  {};
char *proc_tags[]              =  {"State tracking", "Math", "Detection", "Matching", NULL };
char *proc_alias[]             =  {"metacntquery", "cntquery_shared", "metacntquery_shared", NULL};
char proc_purpose[]            =  "counts (and optionally query) the number of times a specified label has been seen.";
proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'m',"","cnt",
     "minimum value for positive query",0,0},
     {'L',"","label",
     "use labeled tuple hash for lookups",0,0},
     {'C',"","LABEL",
     "add count to query tuples",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'E',"","duration",
     "expire counters every E seconds (or use m for minutes, h for hours)",0,0},
     {'F',"","filename",
     "load existing database (key table) from file",0,0},
     {'O',"","filename",
     "write database (key table) to file",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "LABEL of tuple key";
char *proc_input_types[]       =  {"tuple", "any", NULL};
// (Potential) Output types: meta[LOCAL_MAX_TYPES]
char *proc_output_types[]      =  {"any", NULL};
char proc_requires[]           =  "";
// Ports: INCREMENT, DECREMENT, ADDQUERY
proc_port_t proc_input_ports[] =  {
     {"none","query item if it meets minimum criteria"},
     {"INCREMENT","increment item at key"},
     {"ADDQUERY","increment item at key, query"},
     {"DECREMENT","decrement item at key"},
     {NULL, NULL}
};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  { "cntquery [-J <SHT5 LABEL>] <LABEL> [-m <cnt> | -L <label> | -C <LABEL> | -M <records> | -E <duration>]", NULL };
proc_example_t proc_examples[] =  {
        { "... | cntquery VALUE | ...", "Simply count the number of times each particular value has been seen."},
	{ "... | ADDQUERY:cntquery -E 1h -m 10 VALUE | ...", "Count the number of times a value has been seen and pass when it has been seen at least 10 times. Expire the count every one hour"},
	 {NULL,""}
};char proc_description[]	= "count (and optionally query) the number of times a specified label has been seen.  This kid is useful for finding events that occur a specified number of times.\n\nThis kid requires as input a label from the tuple, which it then uses to generate counts.  There are three ports: INCREMENT, ADDQUERY, and DECREMENT.  INCREMENT and DECREMENT change the value associated with a key, whereas ADDQUERY will increment and pass the item if the minimum threshold is reached (as specified with the '-m' option)."; 

#define LOCAL_MAX_SH5_TABLE 1000000
#define LOCAL_MAX_TYPES 25
#define MIN_CNT 1

//define saturation counter ranges
#define MAX_EVENT_CNT 100000
#define MIN_EVENT_CNT (-MAX_EVENT_CNT)

typedef struct _event_data_t {
     uint32_t epoch;
     int32_t cnt;
} event_data_t;

//function prototypes for local functions
static int process_query(void *, wsdata_t*, ws_doutput_t*, int);
static int process_addquery(void *, wsdata_t*, ws_doutput_t*, int);
static int process_increment(void *, wsdata_t*, ws_doutput_t*, int);
static int process_decrement(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t query_cnt;
     uint64_t increment_cnt;
     uint64_t decrement_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];
     wslabel_set_t lset;
     stringhash5_t * cntquery_table;
     uint32_t max_cntquery_table;
     wslabel_t * hash_label;
     int min_cnt;
     int add_count;
     wslabel_t * label_count;
     uint32_t epoch;
     time_boundary_t epoch_boundary;
     wslabel_t * label_datetime;
     int expire;
     char * outfile;
     char * open_table;

     char * sharelabel;
     int sharer_id;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:E:C:L:m:M:F:O:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'E':
               proc->expire = 1;
               proc->epoch_boundary.increment_ts = sysutil_get_duration_ts(optarg);
               if (proc->epoch_boundary.increment_ts) {
                    fprintf(stderr,"%s new epoch every ", PROC_NAME);
                    sysutil_print_time_interval(stderr,
                                                proc->epoch_boundary.increment_ts);
                    fprintf(stderr,"\n");
               }
               else {
                    tool_print("time must be divisible by the hour %d",
                               (int)proc->epoch_boundary.increment_ts);
                    return 0;
               }
               break;
          case 'L':
               wslabel_set_add(type_table, &proc->lset, optarg);
               tool_print("adding search key %s", optarg);
               break;
          case 'm':
               proc->min_cnt = atoi(optarg);
               break;
          case 'C':
               proc->add_count = 1;
               proc->label_count = wsregister_label(type_table, optarg);
               break;
          case 'M':
               proc->max_cntquery_table = atoi(optarg);
               break;
          case 'F':
               proc->open_table = strdup(optarg);
               // loading of the table is postponed to the stringhash5_create_shared_sht
               // call in proc_init
               break;
          case 'O':
               proc->outfile = strdup(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset, argv[optind]);
          tool_print("filtering on %s", argv[optind]);
          optind++;
     }
     
     return 1;
}

// the following is a function to take in command arguments and initalize
// this processor's instance..
//  also register as a source here..
// return 1 if ok
// return 0 if fail
int proc_init(wskid_t * kid, int argc, char ** argv, void ** vinstance, ws_sourcev_t * sv,
              void * type_table) {
     
     //allocate proc instance of this processor
     proc_instance_t * proc =
          (proc_instance_t*)calloc(1, sizeof(proc_instance_t));
     *vinstance = proc;

     proc->max_cntquery_table = LOCAL_MAX_SH5_TABLE;
     proc->min_cnt = MIN_CNT;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (proc->expire) {
          proc->label_datetime = wssearch_label(type_table, "DATETIME");
     }
     
     //other init - init the stringhash table

     //calloc shared sh5 option struct
     stringhash5_sh_opts_t * sh5_sh_opts;
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     //set shared sh5 option fields
     sh5_sh_opts->open_table = proc->open_table;

     if (proc->sharelabel) {
          int ret;

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->cntquery_table, 
                                              proc->sharelabel, proc->max_cntquery_table, 
                                              sizeof(event_data_t), &proc->sharer_id, 
                                              sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          // read the stringhash5 table from the open_table file
          uint32_t ret = 0;
          if (proc->open_table) {
               ret = stringhash5_open_sht_table(&proc->cntquery_table, proc, proc->max_cntquery_table, 
                                                sizeof(event_data_t), sh5_sh_opts); 
          }
          // create the stringhash5 table from scratch
          if (!ret) {
               proc->cntquery_table = stringhash5_create(0, proc->max_cntquery_table, 
                                                         sizeof(event_data_t));
               if (!proc->cntquery_table) {
                    return 0;
               }
          }
     }

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //use the stringhash5-adjusted value of max_records to reset max_cntquery_table
     proc->max_cntquery_table = proc->cntquery_table->max_records;

     free(proc->open_table);

     return 1; 
}

// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
//return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * meta_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vinstance;

     if (!wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          return NULL;
     }

     //find out 
     if (wslabel_match(type_table, port, "INCREMENT")) {
          tool_print("have increment");
          return process_increment;
     }
     if (wslabel_match(type_table, port, "DECREMENT")) {
          tool_print("have decrement");
          return process_decrement;
     }
     
     //DEFAULT to QUERY 

     //register output..
     // pass the input type to the output..
     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type, NULL);

     if (wslabel_match(type_table, port, "ADDQUERY")) {
          tool_print("have increment");
          return process_addquery;
     }
     return process_query;

}

static inline event_data_t * get_edata(proc_instance_t * proc, wsdata_t * wsd, int create) {
     event_data_t * edata = NULL;
     if (wsd->dtype == dtype_tuple) {
          int i;
          wsdata_t ** members;
          int mlen;

          for (i = 0; i < proc->lset.len; i++) {
               if (tuple_find_label(wsd, proc->lset.labels[i], &mlen, &members)) {
                    if (members[0]) {
                         if (create) {
                              edata = stringhash5_find_attach_wsdata(proc->cntquery_table, members[0]);
                         }
                         else {
                              edata = stringhash5_find_wsdata(proc->cntquery_table, members[0]);
                         }
                         break;
                    }
               }
          }
          if (edata && proc->expire) {
               //look for timestamp
               if (tuple_find_label(wsd, proc->label_datetime, &mlen, &members)) {
                    if (members[0]->dtype == dtype_ts) {
                         wsdt_ts_t * ts = (wsdt_ts_t*)members[0]->data;
                         if (sysutil_test_time_boundary(&proc->epoch_boundary, ts->sec)) {
                              proc->epoch++;
                         }  
                    }
               }
               if (edata->epoch != proc->epoch) {
                    edata->cnt = 0;
                    edata->epoch = proc->epoch;
               }
          }
          return edata;
     }
     if (create) {
          return stringhash5_find_attach_wsdata(proc->cntquery_table, wsd);
     }
     else {
          return stringhash5_find_wsdata(proc->cntquery_table, wsd);
     }
     return NULL;
}

static int process_increment(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->increment_cnt++;

     event_data_t * edata = get_edata(proc, input_data, 1);
     if (!edata) {
          return 0;
     }
     if (edata->cnt < MAX_EVENT_CNT) {
          edata->cnt++;
     }
     stringhash5_unlock(proc->cntquery_table);
     return 0;
}

static int process_decrement(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->decrement_cnt++;

     event_data_t * edata = get_edata(proc, input_data, 1);
     if (!edata) {
          return 0;
     }
     if (edata->cnt > MIN_EVENT_CNT) {
          edata->cnt--;
     }
     stringhash5_unlock(proc->cntquery_table);
     return 0;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_addquery(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->query_cnt++;

     event_data_t * edata = get_edata(proc, input_data, 1);
     if (!edata) {
          return 0;
     }
     if (edata->cnt < MAX_EVENT_CNT) {
          edata->cnt++;
     }
     if (edata->cnt >= proc->min_cnt) {
          if (proc->add_count && (input_data->dtype == dtype_tuple)) {
               tuple_member_create_uint(input_data, (uint32_t)edata->cnt,
                                        proc->label_count);
          }
          ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
          proc->outcnt++;
     }
     stringhash5_unlock(proc->cntquery_table);
     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_query(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->query_cnt++;

     event_data_t * edata = get_edata(proc, input_data, 0);
     if (!edata) {
          return 0;
     }
     if (edata->cnt >= proc->min_cnt) {
          if (proc->add_count && (input_data->dtype == dtype_tuple)) {
               tuple_member_create_uint(input_data, (uint32_t)edata->cnt,
                                        proc->label_count);
          }
          ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
          proc->outcnt++;
     }
     stringhash5_unlock(proc->cntquery_table);
     return 0;
}

static void serialize_table(proc_instance_t * proc) {
     if (proc->outfile && (!proc->sharelabel || !proc->sharer_id)) {
	  tool_print("Writing data table to %s", proc->outfile);
          FILE * fp = fopen(proc->outfile, "w");
	  if (fp) {
	       stringhash5_dump(proc->cntquery_table, fp);
	       fclose(fp);
	  }
          else {
               perror("failed writing data table");
               tool_print("unable to write to file %s", proc->outfile);
          }
     }
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("query cnt %" PRIu64, proc->query_cnt);
     tool_print("increment cnt %" PRIu64, proc->increment_cnt);
     tool_print("decrement cnt %" PRIu64, proc->decrement_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     serialize_table(proc);
     stringhash5_destroy(proc->cntquery_table);

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     if (proc->outfile) {
          free(proc->outfile);
     }
     free(proc);

     return 1;
}

