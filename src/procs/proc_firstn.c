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

// selects the first N records from each key

#define PROC_NAME "firstn"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "stringhash5.h"
#include "procloader.h"
#include "sysutil.h"
#include "wstypes.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
// Use to fix proc_tags: 
//char *proc_menus[]             =  { "Filters", NULL };
char *proc_tags[]              =  {"Filtering", "Stream manipulation", NULL};
char *proc_alias[]             =  { "first", "firstn_shared", "first_shared", NULL };
char proc_purpose[]            =  "selects the first n tuples from each key";
char proc_description[] = "emit the first N tuples for each value in a specified LABEL (key).  Specifying N is done with the '-N' or '-n' options (default is 10).  Options allow for expiring values in the table to allow fresh looks at data being emitted after specified expiration times (-E).  Counts of the number of times a value has been seen can also be added to the tuple.  It is also possible to control the size of the storage table with the '-M' option (default value is 350000 bytes).";
proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'F',"","filename",
     "load existing database (key table) from file",0,0},
     {'O',"","filename",
     "write database (key table) to file",0,0},
     {'n',"","count",
     "number of records per key",0,0},
     {'N',"","count",
     "number of records per key",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'E',"","duration",
     "Expire time of filter in seconds (or use m for minutes, h for hours)",0,0},
     {'C',"","LABEL",
     "Output current count as label",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "LABEL of key to track";
char *proc_input_types[]       =  {"tuple", "any", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"any", NULL};
char proc_requires[]           =  "None";
// Ports: 
proc_port_t proc_input_ports[] =  {
     {"none","check key, pass if less than max count"},
     {NULL, NULL}
};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  {"firstn [-J <SHT5 LABEL>] <LABEL> [-[n|N] <count>] [-M <value>] [-E <time>] [-C <NEWLABEL>]", NULL};
proc_example_t proc_examples[] =  {
	{"... | firstn WORD | ...", "Send on the first 10 tuples for each given WORD."},
	{"... | firstn WORD -N 5 -E 1d -C COUNT | ...", "Send on the first 5 tuples for each WORD, resetting the table for each WORD after 1d.  Also, add a label, COUNT, indicating how many times this WORD has been seen up to N."},
	{NULL, NULL}
};

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _key_data_t {
     uint32_t cnt;
     time_t expire;
} key_data_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     uint32_t max_cnt;
     ws_outtype_t * outtype_tuple;
     stringhash5_t * key_table;
     uint32_t tablemax;
     wslabel_set_t lset;
     wslabel_t * label_datetime;
     int do_expire;
     time_t expire_offset;
     wslabel_t * label_count;
     char * outfile;
     char * open_table;

     char * sharelabel;
     int sharer_id;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:O:F:C:E:n:N:M:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'O':
               proc->outfile = strdup(optarg);
               break;
          case 'F':
               proc->open_table = strdup(optarg);
               // loading of the table is postponed to the stringhash5_create_shared_sht
               // call in proc_init
               break;
          case 'C':
               proc->label_count = wsregister_label(type_table, optarg);
               break;
          case 'n':
          case 'N':
               proc->max_cnt = atoi(optarg);
               break;
          case 'M':
               proc->tablemax = atoi(optarg);
               break;
          case 'E':
               proc->expire_offset = sysutil_get_duration_ts(optarg);
               proc->do_expire = 1;
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

     ws_default_statestore(&proc->tablemax);

     proc->max_cnt = 10;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     proc->label_datetime = wssearch_label(type_table, "DATETIME");
     
     //other init - init the stringhash table

     //calloc shared sh5 option struct
     stringhash5_sh_opts_t * sh5_sh_opts;
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     //set shared sh5 option fields
     sh5_sh_opts->open_table = proc->open_table;

     if (proc->sharelabel) {
          int ret;

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->key_table, 
                                              proc->sharelabel, proc->tablemax, 
                                              sizeof(key_data_t), &proc->sharer_id, 
                                              sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          // read the stringhash5 table from the open_table file
          uint32_t ret = 0;
          if (proc->open_table) {
               ret = stringhash5_open_sht_table(&proc->key_table, proc, proc->tablemax, 
                                                sizeof(key_data_t), sh5_sh_opts);
          }
          // create the stringhash5 table from scratch
          if (!ret) {
               proc->key_table = stringhash5_create(0, proc->tablemax, 
                                                    sizeof(key_data_t));
               if (!proc->key_table) {
                    return 0;
               }
          }
     }

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //use the stringhash5-adjusted value of max_records to reset tablemax
     proc->tablemax = proc->key_table->max_records;

     if (proc->open_table) {
          free(proc->open_table);
     }

     if (!proc->lset.len) {
          tool_print("must specify key to process");
          return 0;
     }

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
          proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
     }

     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          return proc_flush;
     }
     if (meta_type == dtype_tuple) {
          return proc_tuple;
     }
     // we are happy.. now set the processor function
     return NULL; // a function pointer
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     time_t current_time = 0;
     if (proc->do_expire) {
          wsdata_t ** mset;
          int mset_len;
          if (tuple_find_label(input_data, proc->label_datetime,
                               &mset_len, &mset)) {
               if (mset_len && (mset[0]->dtype == dtype_ts)) {
                    wsdt_ts_t * ts = (wsdt_ts_t*)mset[0]->data;
                    current_time = ts->sec;
               }
          }
          if (!current_time) {
               current_time = time(NULL);
          }
     }

     int pass = 0;
     key_data_t * kdata;
     uint32_t cnt = 0;
     int id;
     wsdata_t * member;
     wslabel_t * label;
     tuple_labelset_iter_t iter;
     tuple_init_labelset_iter(&iter, input_data, &proc->lset);
     while (tuple_search_labelset(&iter, &member, &label, &id)) {
          kdata = stringhash5_find_attach_wsdata(proc->key_table, member);
          if (kdata) {
               if (proc->do_expire) {
                    if (!kdata->expire) {
                         kdata->expire = current_time + proc->expire_offset;
                    }
                    else if (current_time >= kdata->expire) {
                         kdata->cnt = 0;
                         kdata->expire = current_time + proc->expire_offset;
                    }
               }
               if (kdata->cnt < proc->max_cnt) {
                    pass = 1;
               }
               kdata->cnt++;
               cnt = kdata->cnt;
               stringhash5_unlock(proc->key_table);
          }
     }

     // this is new data.. pass as output
     if (pass) {
          if (proc->label_count && cnt) {
               tuple_member_create_uint(input_data, cnt, proc->label_count);
          }
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }
     return 1;
}

static void serialize_table(proc_instance_t * proc) {
     if (proc->outfile && (!proc->sharelabel || !proc->sharer_id)) {
	  tool_print("Writing key table to %s", proc->outfile);
          FILE * fp = fopen(proc->outfile, "w");
	  if (fp) {
	       stringhash5_dump(proc->key_table, fp);
	       fclose(fp);
	  }
          else {
               perror("failed writing key table");
               tool_print("unable to write to file %s", proc->outfile);
          }
     }
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     serialize_table(proc);

     // NOTE:  This stringhash5_flush is not a real flush; it simply
     // does a bucket init.  If we truly want to flush here and
     // get the contents of the table, we need to call 
     // stringhash5_scour_and_flush
     if (!proc->sharelabel || !proc->sharer_id) {
          stringhash5_flush(proc->key_table);
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     if (proc->outcnt && proc->meta_process_cnt) {
     tool_print("output percentage %.2f%%",
                (double)proc->outcnt * 100/(double)proc->meta_process_cnt);
     }

     //destroy table
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

