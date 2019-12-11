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
// keyflow -- a keyflow is a set of the same keys around the same time period.
//create key hash based on keys observed around the same time..
//  times out keyhash when max time interval is reached

#define PROC_NAME "keyflow"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_ts.h"
#include "stringhash5.h"
#include "procloader.h"
#include "sysutil.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"Annotation", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "Creates a hash for keys that are close in time";
char proc_description[] = "A keyflow is a set of the same keys around the same time period.  The "
     "keyflow kid takes a given key and assigns a unique hash to kids with the same value at the "
     "given key until a time gap of the specified length is encountered (-t option, default is "
     "60) or until a specified number of events have transpired (-s option).  A new label member "
     "is added to each tuple (default is KEYFLOW; the '-L' is used to change this label to a "
     "user-specified label name) containing the hash value.  The '-M' option is used to adjust "
     "the size of the table tracking the keyflows.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'t',"","sec",
     "time out keyflow after t seconds (or use m for minutes, h for hours)",0,0},
     {'s',"","cnt",
     "time out keyflow after s events",0,0},
     {'L',"","label",
     "label of output keyflows",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'F',"","filename",
     "load existing database (key table) from file",0,0},
     {'O',"","filename",
     "write database (key table) to file",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "LABEL of key to track";
char *proc_input_types[]       =  {"tuple", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
char proc_requires[]           =  "None";
// Ports: 
proc_port_t proc_input_ports[] =  {
     {"none","count item at key"},
     {NULL, NULL}
};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {"KEYFLOW", NULL};
char *proc_synopsis[]          =  {"keyflow <LABEL> [-t <sec> | -s <count>] [-M <records>] [-L <label>]", NULL};
proc_example_t proc_examples[] =  {
	{"... | keyflow WORD -t 3s | ...", "Assigns a KEYFLOW label containing a hash value that can be used to correlate continuous WORD values that do not have any gaps greater than 3 seconds in length."},
	{"... | tuplehash WORD1 WORD2 -L PAIR | keyflow PAIR -s 10 | ...", "Create a new KEYFLOW value for each 10 events of each combination of WORD1 and WORD2."},
	{NULL, NULL}
};

typedef struct _key_data_t {
     time_t last_time;
     wsdt_uint64_t hash;
} key_data_t;

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     stringhash5_t * key_table;
     uint32_t buflen;
     wslabel_set_t lset;
     wslabel_t * label_date;
     wslabel_t * label_keyflow;
     ws_outtype_t * outtype_tuple;
     ws_tuplemember_t * tmember_keyflow;
     time_t timeout;
     uint32_t hash_seed;
     uint64_t seq_max;
     uint64_t seq;
     char * outfile;
     char * open_table;

     char * sharelabel;
     int sharer_id;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:s:L:t:M:F:O:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 's':
               proc->seq_max = strtoull(optarg, NULL, 10);
               break;
          case 'L':
               proc->label_keyflow = wsregister_label(type_table, optarg);
               break;
          case 't':
               proc->timeout = sysutil_get_duration_ts(optarg);
               tool_print("setting keyflow timeout to %u",
                          (uint32_t)proc->timeout);
               break;
          case 'M':
               proc->buflen = atoi(optarg);
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
          tool_print("using key %s", argv[optind]);
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
          (proc_instance_t*)calloc(1,sizeof(proc_instance_t));
     *vinstance = proc;

     ws_default_statestore(&proc->buflen);

     proc->timeout = 60;
     proc->label_date = wssearch_label(type_table, "DATETIME");
     proc->label_keyflow = wsregister_label(type_table, "KEYFLOW");

     proc->hash_seed = rand();
     proc->tmember_keyflow = register_tuple_member_type(type_table,
                                                        "UINT64_TYPE", NULL);
     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     //other init - init the stringhash table

     //calloc shared sh5 option struct
     stringhash5_sh_opts_t * sh5_sh_opts;
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     //set shared sh5 option fields
     sh5_sh_opts->open_table = proc->open_table;

     if (proc->sharelabel) {
          int ret;

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->key_table, 
                                              proc->sharelabel, proc->buflen, 
                                              sizeof(key_data_t), &proc->sharer_id, 
                                              sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          // read the stringhash5 table from the open_table file
          uint32_t ret = 0;
          if (proc->open_table) {
               ret = stringhash5_open_sht_table(&proc->key_table, proc, proc->buflen, 
                                                sizeof(key_data_t), sh5_sh_opts);
          }
          // create the stringhash5 table from scratch
          if (!ret) {
               proc->key_table = stringhash5_create(0, proc->buflen, sizeof(key_data_t));
               if (!proc->key_table) {
                    return 0;
               }
          }
     }

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->key_table->max_records;

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

     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          proc->outtype_tuple = ws_add_outtype(olist, meta_type, NULL);
          return proc_tuple;
     }

     return NULL; // a function pointer
}

static inline int add_member(proc_instance_t * proc, wsdata_t * tdata,
                             wsdata_t * member, time_t ts) {
     key_data_t * kdata = NULL;
     void * key;
     int keylen;
     ws_hashloc_t * hashloc;
     hashloc = member->dtype->hash_func(member);
     key = hashloc->offset;
     keylen = hashloc->len;

     kdata = (key_data_t *) stringhash5_find_attach(proc->key_table,
                                                    key, keylen);
     if (!kdata) {
          return 0;
     }

     //if time is off.. just set of last time...

     if (proc->seq_max) {
          if (!kdata->last_time || ((proc->seq - kdata->last_time) >
                                    proc->seq_max)) {
               kdata->hash = evahash64(key, keylen, proc->hash_seed);
               kdata->hash *= proc->seq | 0x01LLU;
          }
          kdata->last_time = proc->seq;
          proc->seq++;
     }
     else {
          if (!kdata->last_time || (ts > kdata->last_time)) {
               if (!kdata->last_time || (proc->timeout <= (ts - kdata->last_time))) {
                    kdata->hash = evahash64(key, keylen, proc->hash_seed);
                    //use time as hash permutation
                    kdata->hash *= (uint64_t)ts | 0x01LLU;
               }
               kdata->last_time = ts;
          }
     }
     wsdt_uint64_t * keytag = tuple_member_alloc_label(tdata,
                                                    proc->tmember_keyflow,
                                                    proc->label_keyflow);
     if (keytag) {
          *keytag = kdata->hash;
     }
     stringhash5_unlock(proc->key_table);

     return 1;
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     //search for items in tuples
     wsdt_ts_t * ts = NULL;
     wsdata_t ** mset;
     int mset_len;
     int i, j;
     if (tuple_find_label(input_data, proc->label_date,
                          &mset_len, &mset)) {
          for (i = 0; i < mset_len; i++) {
               if (mset[i]->dtype == dtype_ts) {
                    ts = (wsdt_ts_t*)mset[i]->data;
                    break;
               }
          }
     }
     if (!ts) {
          return 0;
     }
     for (i = 0; i < proc->lset.len; i++) {
          if (tuple_find_label(input_data, proc->lset.labels[i],
                               &mset_len, &mset)) {
               for (j = 0; j < mset_len; j++ ) {
                    add_member(proc, input_data, mset[j], ts->sec);
               }
          }
     }

     //always pass thru..
     proc->outcnt++;
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     return 1;
}

static void serialize_table(proc_instance_t * proc) {
     if (proc->outfile && (!proc->sharelabel || !proc->sharer_id)) {
	  tool_print("Writing data table to %s", proc->outfile);
          FILE * fp = fopen(proc->outfile, "w");
	  if (fp) {
	       stringhash5_dump(proc->key_table, fp);
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
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     serialize_table(proc);
     stringhash5_destroy(proc->key_table);

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

