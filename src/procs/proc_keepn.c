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
//keeps last wsdata tuple container based on key

#define PROC_NAME "keepn"
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
char *proc_tags[]              =  {"Stream manipulation", "Sessionization", "State tracking", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "Keep last values from key";
char proc_description[] = "The keepn kid stores the last N of a list of values specified.  The value of n is specified with the '-n' or '-N' options, with the subtle difference that the '-N' option will cause the tuple to be emitted when n is reached, while the '-n' option only emits when a flush event is detected.  (If n is not specified at initialization, the default operation is to operate as if the '-n 5' option had been specified.  At flush, only keys that have reached the n event limit will be emitted.  The '-f' option changes the output from the last n values to the first n values.  The '-c' option will emit the KEEPCOUNT with each tuple, which is the value of n.  The '-V' option specifies which values to keep in the list of n items, and multiple '-V' options may be specified.  Note however that only the last n items seen from any of the values specified will be kept (e.g., it will not keep n of each of the specified values).  The '-M' option is used to size the number of records in the table.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'f',"","",
     "keep first n elements at key",0,0},
     {'c',"","",
     "emit count of stored elements",0,0},
     {'n',"","count",
     "number of each member to store at key",0,0},
     {'N',"","count",
     "emit only when count reaches limit",0,0},
     {'V',"","label",
     "label of member to store at key (can be multiple)",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]     =  "LABEL of key to index on";
char *proc_input_types[]       =  {"any","tuple", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {
     {"none","Store value at key"},
     {NULL, NULL}
};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {"KEEPCOUNT", NULL};
char *proc_synopsis[]          =  {"keepn <LABEL> [-f] [-c] [-[n|N] <count>] [-V <label> | ...] [-M <records>]", NULL};
proc_example_t proc_examples[] =  {
	{"... | keepn KEY -N 3 -V VALUE | ...", "For every 3 VALUE seen from a given KEY, emit the list."},
	{"... | keepn KEY -n 3 -V VALUE | ...", "Keep the last 3 VALUE seen for a given KEY.  Only emit this list when given a flush event."},
	{"... | keepn KEY -n 3 -f -V VALUE | ...", "Keep the first 3 VALUE seen for a given KEY.  Emit this list when given a flush event."},
	{"... | keepn KEY -n 3 -V VALUE -V CONTENT | ...", "Keep the last 3 values of any VALUE or CONTENT seen and emit this list at a flush event."},
	{NULL, NULL}
};

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     stringhash5_t * last_table;
     uint32_t buflen;
     wslabel_nested_set_t nest;
     wslabel_t * label_key;
     ws_outtype_t * outtype_tuple;
     ws_doutput_t * dout;
     int cnt;
     int keepfirst;
     int emit_count;
     wslabel_t * label_count;
     int hitemit;

     char * sharelabel;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:cfn:N:V:M:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'c':
               proc->emit_count = 1;
               break;
          case 'f':
               proc->keepfirst = 1;
               break;
          case 'N':
               proc->hitemit = 1;
               //break; -- intentional fall through
          case 'n':
               proc->cnt = atoi(optarg);
               tool_print("storing %d objects at key", proc->cnt);
               if(proc->cnt <= 0) {
                    error_print("invalid count of objects ... specified %d", proc->cnt);
                    return 0;
               }
               break;
          case 'V':
               wslabel_nested_search_build(type_table, &proc->nest, optarg); 
               tool_print("storing value");
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          proc->label_key = wssearch_label(type_table, argv[optind]);
          tool_print("using key %s", argv[optind]);
          optind++;
     }
     
     return 1;
}

static void rec_erase(void * vdata, void * vproc) {
     dprint("rec_erase");
     proc_instance_t * proc = (proc_instance_t *)vproc;
     wsdata_t ** wsdp = (wsdata_t **)vdata;
     if (!wsdp || !wsdp[0]) {
          return;
     }
     int i;
     for (i = 0; i < proc->cnt; i++) {
          if (!wsdp[i]) {
               break;
          }
          wsdata_delete(wsdp[i]); //remove reference
     }
     memset(wsdp, 0, sizeof(wsdata_t *) * proc->cnt);
}

static void last_destroy_from(uint32_t index, void * vdata, void * vproc) {
     dprint("last_destroy");
     proc_instance_t * proc = (proc_instance_t *)vproc;
     wsdata_t ** wsdp = (wsdata_t **)vdata;
     if (!wsdp || !wsdp[0]) {
          return;
     }
     if(index >= proc->cnt) {
          error_print("invalid starting index for last_destroy_from");
          return;
     }
     wsdata_t * tdata = ws_get_outdata(proc->outtype_tuple);
     if (!tdata) {
          error_print("%s cannot allocate tdata", PROC_NAME);
          return;
     }
     int i;
     uint32_t c = 0;
     for (i = 0; i < proc->cnt; i++) {
          if (!wsdp[i]) {
               break;
          }
          add_tuple_member(tdata, wsdp[i]);   
          if(i >= index) {
               wsdata_delete(wsdp[i]); //remove reference
               wsdp[i] = NULL;
          }
          c++;
     }
     if (proc->emit_count) {
          tuple_member_create_uint(tdata, c - 1, proc->label_count);
     }
     memset(wsdp + index, 0, sizeof(wsdata_t *) * (proc->cnt-index));
     dprint("keepn %d", i);
     proc->outcnt++;
     ws_set_outdata(tdata, proc->outtype_tuple, proc->dout);
}

static void last_destroy(void * vdata, void * vproc) {
     return last_destroy_from(0, vdata, vproc);
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

     proc->cnt = 5;
     proc->label_count = wsregister_label(type_table, "KEEPCOUNT");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->label_key || (proc->nest.cnt == 0) || !proc->cnt) {
          tool_print("ERROR in proc_init: must specify key and values");
          return 0;
     }

     proc->cnt++; //increment to store key

     //other init - init the stringhash table
     if (proc->sharelabel) {
          // Note:  the following seems backwards.  But, if hitemit is set, we only
          // want full sets of elements, so rec_erase is used to throw away all of 
          // the partials at expire time.  But, if hitemit is not set, then we are
          // expecting the partials, hence the use of last_destroy here.
          if (!proc->hitemit) {
               stringhash5_sh_opts_t * sh5_sh_opts;
               int ret;

               //calloc shared sh5 option struct
               stringhash5_sh_opts_alloc(&sh5_sh_opts);

               //set shared sh5 option fields
               sh5_sh_opts->sh_callback = last_destroy;
               sh5_sh_opts->proc = proc; 

               ret = stringhash5_create_shared_sht(type_table, (void **)&proc->last_table, 
                                                   proc->sharelabel, proc->buflen, 
                                                   sizeof(wsdata_t *) * proc->cnt, 
                                                   NULL, sh5_sh_opts);

               //free shared sh5 option struct
               stringhash5_sh_opts_free(sh5_sh_opts);

               if (!ret) return 0;
          }
          else {
               stringhash5_sh_opts_t * sh5_sh_opts;
               int ret;

               //calloc shared sh5 option struct
               stringhash5_sh_opts_alloc(&sh5_sh_opts);

               //set shared sh5 option fields
               sh5_sh_opts->sh_callback = rec_erase;
               sh5_sh_opts->proc = proc; 

               ret = stringhash5_create_shared_sht(type_table, (void **)&proc->last_table, 
                                                   proc->sharelabel, proc->buflen, 
                                                   sizeof(wsdata_t *) * proc->cnt, 
                                                   NULL, sh5_sh_opts);

               //free shared sh5 option struct
               stringhash5_sh_opts_free(sh5_sh_opts);

               if (!ret) return 0;
          }
     }
     else {
          proc->last_table = stringhash5_create(0, proc->buflen, sizeof(wsdata_t *) * proc->cnt);
          if (!proc->last_table) {
               return 0;
          }
          if (!proc->hitemit) {
               stringhash5_set_callback(proc->last_table, last_destroy, proc);
          }
          else {
               stringhash5_set_callback(proc->last_table, rec_erase, proc);
          }
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

     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          proc->outtype_tuple = ws_add_outtype(olist, wsdatatype_get(type_table,
                                                                     "TUPLE_TYPE"), NULL);
          return proc_flush;
     }
     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          proc->outtype_tuple = ws_add_outtype(olist, meta_type, NULL);
          return proc_tuple;
     }

     return NULL; // a function pointer
}

static int proc_add_value(void * vproc, void * tlist,
                          wsdata_t * tdata, wsdata_t * value) { 

     proc_instance_t * proc = (proc_instance_t *)vproc;
     wsdata_t ** lastdata = (wsdata_t**)tlist;

     int i;
     for (i = 1; i < proc->cnt; i++) {
          if (!lastdata[i]) {
               lastdata[i] = value;
               wsdata_add_reference(value);

               if (proc->hitemit && (i == (proc->cnt - 1))) {
                    last_destroy_from(1, lastdata, proc);
               }

               return 1;
          }
     }

     if (proc->keepfirst) {
          return 0;
     }

     dprint("full, deleting oldest %p %p", lastdata[1], lastdata);
     //else..delete oldest, reorder
     wsdata_delete(lastdata[1]);
     dprint("done wsdata_delete");

     //shuffle
     for (i = 2; i < proc->cnt; i++) {
          lastdata[i-1] = lastdata[i];
          dprint("moving %d %d", i-1, i);
     }
     dprint("adding %d", proc->cnt);
     lastdata[proc->cnt - 1] = value;
     wsdata_add_reference(value);

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     proc->meta_process_cnt++;

     wsdata_t ** lastdata = NULL;

     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          wsdata_t * key = mset[0];

          lastdata = (wsdata_t **) stringhash5_find_attach_wsdata(proc->last_table,
                                                                  key);
          if (lastdata && !lastdata[0]) {
               lastdata[0] = key;
               wsdata_add_reference(key);
          }

          tuple_nested_search(input_data, &proc->nest,
                              proc_add_value,
                              proc,
                              lastdata);
          if (lastdata) {
               stringhash5_unlock(proc->last_table);
          }

     }
     if (!lastdata) {
          return 0;
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;

     // Note:  the following seems backwards.  But, if hitemit is set, we only
     // want full sets of elements, so rec_erase is used to throw away all of 
     // the partials at flush time.  But, if hitemit is not set, then we are
     // expecting the partials, hence the use of last_destroy here.
     if (!proc->hitemit) {
          stringhash5_scour_and_flush(proc->last_table, last_destroy, proc);
     }
     else {
          stringhash5_scour_and_flush(proc->last_table, rec_erase, proc);
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     stringhash5_destroy(proc->last_table);

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     free(proc);

     return 1;
}

