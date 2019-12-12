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

#define PROC_NAME "mergetuple"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "stringhash5.h"
#include "procloader.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"Stream manipulation", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "merges data from tuples";
char proc_description[] = "Merge two or more tuples into a single tuple.  Tuples for merging are identified by a key (specified with the '-K' option) and the new tuple will contain the labels specified with the -C option (or labels not specified with an optional parameter).  Additional options include merging of distinct elements only (-D), the number of tuples to merge (-N, default is 2), and specifying the size of the hash table for storing the streaming data (-M, default is 350000)";
proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'K',"","LABEL",
     "key used for merge",0,0},
     {'D',"","",
     "merge only distinct elements",0,0},
     {'C',"","LABEL",
     "data to carry over on merge",0,0},
     {'N',"","cnt",
     "count of data to merge",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "LABEL of data to merge";
char *proc_input_types[]       =  {"any","tuple", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {
     {"none","Check and save last data at key"},
     {NULL, NULL}
};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  {"mergetuple -K <LABELKEY> [-C <LABEL> | ...] [-D] [-N <count>] [-M <records>]", NULL};
proc_example_t proc_examples[] =  {
	{"$m1, $m2 | mergetuple -K KEY -C DATETIME -C PARAM1 -C PARAM2 -C PARAM3 -C PARAM4 -C PARAM5 -C PARAM6 CONTENT -N 2 -> $merge", "Merge tuples from streams $m1 and $m2 using KEY to determine tuples to merge.  Carry forward the DATETIME, PARAM1, PARAM2, PARAM4, PARAM3, PARAM5, PARAM6, and CONTENT labels from the tuples found in the two streams."},
	{NULL, NULL}
};

typedef struct _key_data_t {
     uint16_t cnt;
     wsdata_t * tuple;
} key_data_t;

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t tuples;
     uint64_t destroy;
     uint64_t outcnt;

     stringhash5_t * last_table;
     uint32_t buflen;
     uint16_t maxcnt;
     wslabel_set_t lset_merge; 
     wslabel_set_t lset_carry; 
     wslabel_t * label_key;
     ws_outtype_t * outtype_tuple;
     ws_doutput_t * dout;
     int distinct;

     char * sharelabel;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:N:C:K:M:D")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'D':
               proc->distinct = 1;
               break;
          case 'K':
               proc->label_key = wssearch_label(type_table, optarg);
               tool_print("looking for key at label %s", optarg);
               break;
          case 'C':
               wslabel_set_add(type_table, &proc->lset_carry, optarg);
               tool_print("adding label %s", optarg);
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          case 'N':
               proc->maxcnt = atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset_merge, argv[optind]);
          tool_print("merging data %s", argv[optind]);
          optind++;
     }
     
     return 1;
}

static void last_destroy(void * vdata, void * vproc) {
     key_data_t * kdata = (key_data_t *)vdata;
     proc_instance_t * proc = (proc_instance_t *)vproc;

     if (kdata->tuple) {
          wsdata_delete(kdata->tuple);
          proc->destroy++;
     }
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
          (proc_instance_t*)calloc(1,sizeof(proc_instance_t));
     *vinstance = proc;

     ws_default_statestore(&proc->buflen);

     proc->maxcnt = 2;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     if (!proc->label_key || !proc->lset_merge.len) {
          tool_print("must specify key and merge data");
          return 0;
     }

     //do not overwrite maxcnt if it was set by a command line option
     if (!proc->maxcnt) {
          proc->maxcnt = proc->lset_merge.len;
     }
     dprint("max cnt %u", proc->maxcnt);

     //other init - init the stringhash table
     if (proc->sharelabel) {
          stringhash5_sh_opts_t * sh5_sh_opts;
          int ret;

          //calloc shared sh5 option struct
          stringhash5_sh_opts_alloc(&sh5_sh_opts);

          //set shared sh5 option fields
          sh5_sh_opts->sh_callback = last_destroy;
          sh5_sh_opts->proc = proc; 

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->last_table, 
                                              proc->sharelabel, proc->buflen, 
                                              sizeof(key_data_t), NULL, sh5_sh_opts);

          //free shared sh5 option struct
          stringhash5_sh_opts_free(sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          proc->last_table = stringhash5_create(0, proc->buflen, sizeof(key_data_t));
          if (!proc->last_table) {
               return 0;
          }
          stringhash5_set_callback(proc->last_table, last_destroy, proc);
     }

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->last_table->max_records;

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

     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype(olist, wsdatatype_get(type_table,
                                                                     "TUPLE_TYPE"), NULL);
     }

     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          return proc_tuple;
     }

     return NULL; // a function pointer
}

static inline void add_carry_data(proc_instance_t * proc, wsdata_t * tadd,
                                  wsdata_t * tcarry) {
     if (!proc->lset_carry.len) {
          return;
     }
     int i, j;
     wsdata_t ** mset;
     int mset_len;
     for (i = 0; i < proc->lset_carry.len; i++) {
          if (tuple_find_label(tcarry, proc->lset_carry.labels[i],
                               &mset_len, &mset)) {
               for (j = 0; j < mset_len; j++ ) {
                    add_tuple_member(tadd, mset[j]);
               }
          }
     }
}

static inline key_data_t * get_keydata(proc_instance_t * proc, wsdata_t * key,
                                       wsdata_t * tdata) {
     key_data_t * kdata = NULL;
     ws_hashloc_t * hashloc = key->dtype->hash_func(key);
     if (hashloc && hashloc->len) {
          kdata = (key_data_t *) stringhash5_find_attach(proc->last_table,
                                                         (uint8_t*)hashloc->offset,
                                                         hashloc->len);
     }
     if (kdata && !kdata->tuple) {
          kdata->tuple = ws_get_outdata(proc->outtype_tuple);
          if (kdata->tuple) {
               proc->tuples++;
               wsdata_add_reference(kdata->tuple);
               add_tuple_member(kdata->tuple, key);
               dprint("key references %d", key->references);
               add_carry_data(proc, kdata->tuple, tdata);
          }
     }
     return kdata;
}

static inline int merge_key(proc_instance_t * proc, key_data_t * kdata,
                            wsdata_t * mdata, wslabel_t * label) {
     if (!kdata->tuple) {
          return 0;
     }
     if (proc->distinct) {
          if (tuple_find_label(kdata->tuple, label,
                               NULL, NULL)) {
               return 0;
          }
     }
     add_tuple_member(kdata->tuple, mdata);
     dprint("mdata references %d", mdata->references);
     kdata->cnt++;
     dprint("merge key %d", kdata->cnt);

     return 1;
}

static inline void output_kdata(proc_instance_t * proc, key_data_t * kdata,
                                ws_doutput_t * dout) {
     if (!kdata->tuple) {
          return;
     }
     dprint("output tuple");

     dprint("tuple references %d", kdata->tuple->references);
     ws_set_outdata(kdata->tuple, proc->outtype_tuple, dout);
     proc->outcnt++;
     wsdata_delete(kdata->tuple);

     dprint("final tuple references %d", kdata->tuple->references);

     //reset
     kdata->tuple = NULL;
     kdata->cnt = 0;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     proc->meta_process_cnt++;
     
     dprint("mergetuple proc_tuple");
     wsdata_t ** mset;
     int mset_len;
     int i,j;

     //get value
     key_data_t * kdata = NULL;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          for (j = 0; j < mset_len; j++ ) {
               if ((kdata = get_keydata(proc, mset[j], input_data)) != NULL) {
                    break;
               }
          }
     }
     if (!kdata) {
          return 0;
     }
     dprint("found key");
     int found = 0;
     for (i = 0; i < proc->lset_merge.len; i++) {
          if (tuple_find_label(input_data, proc->lset_merge.labels[i],
                               &mset_len, &mset)) {
               for (j = 0; j < mset_len; j++ ) {
                    if (merge_key(proc, kdata, mset[j],
                                  proc->lset_merge.labels[i])) {
                         found = 1;
                    }
               }
          }
     }
     dprint("kdata->cnt %u, proc->maxcnt %u", kdata->cnt, proc->maxcnt);
     if (found && (kdata->cnt >= proc->maxcnt)) {
          output_kdata(proc, kdata, dout);
     }
     stringhash5_unlock(proc->last_table);

     //always return 1 since we don't know if table will flush old data
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("tuples %" PRIu64, proc->tuples);
     tool_print("destroy %" PRIu64, proc->destroy);
     tool_print("output %" PRIu64, proc->outcnt);

     //destroy table
     stringhash5_scour_and_destroy(proc->last_table, last_destroy, proc);

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     free(proc);

     return 1;
}

