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
#define PROC_NAME "flush"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "datatypes/wsdt_flush.h"
#include "datatypes/wsdt_tuple.h"
#include "wstypes.h"

#define LOCAL_MAX_TYPES 50

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  { "Stream Manipulation", NULL };
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "periodically issue a flush request to clear stored state";
proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'t',"","min",
     "flush after N seconds (or use s for seconds, m for minutes, h for hours)",0,0},
     {'C',"","",
     "the number of items to see before flushing",0,0},
     {'N',"","",
     "only issue flushes, do not pass other data through",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "NONE";
char *proc_input_types[]       =  { "tuple", "flush", NULL };
// (Potential) Output types: flush, tuple, meta[LOCAL_MAX_TYPES]
char *proc_output_types[]      =  { "tuple", "flush", NULL };
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  { "flush [-t <min>|-C <value>] [-N]", NULL };
proc_example_t proc_examples[] =  {
    {"... | flush -C 10000 | keycount KEY | ...", "flush the keycount table after each 10000 events.  This will force the printing of the keycount table after each 10000 events, and start a new keycount table."},
    {"... | flush -t 60 | bandwidth | ...", "flush the bandwidth stats every 60 seconds"},
    {"... | flush -t 60s | bandwidth | ...", "flush the bandwidth stats every 60 seconds"},
    {"... | flush -t 60m | bandwidth | ...", "flush the bandwidth stats every 60 minutes"},
    {"... | flush -t 12h | bandwidth | ...", "flush the bandwidth stats every 12 hours"},
    {NULL,""}
};char proc_description[] = "Issues flush requests to the downstream processor to clear any stored state from internal tables.  Flushes can be issued based on the number of event counts seen or according to a time delta.";



//function prototypes for local functions
static int proc_process(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     uint64_t counter;
     uint64_t limit;

     uint32_t nopassthrough;

     wsdata_t * wsd_flush;
     wslabel_t * label_datetime;
     ws_outtype_t * outtype_flush;
     ws_outtype_t * outtype_tuple;
     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];
     time_boundary_t epoch_boundary;
     int flush_time;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc) {

     int op;

     while ((op = getopt(argc, argv, "t:NC:")) != EOF) {
          switch (op) {
          case 't':
               proc->flush_time = 1;
               proc->epoch_boundary.increment_ts = sysutil_get_duration_ts(optarg);
               if (proc->epoch_boundary.increment_ts) {
                    fprintf(stderr,"%s flush every ", PROC_NAME);
                    sysutil_print_time_interval(stderr,
                                                proc->epoch_boundary.increment_ts);
                    fprintf(stderr,"\n");
               }
               else {
                    tool_print("invalid time %d",
                               (int)proc->epoch_boundary.increment_ts);
                    return 0;
               }
               break;
          case 'N':
               proc->nopassthrough=1;
               break;
          case 'C':
               proc->limit = strtoull(optarg, NULL, 10);
               break;
          default:
               return 0;
          }
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

     //read in command options
     if (!proc_cmd_options(argc, argv, proc)) {
          return 0;
     }

     if (proc->flush_time) {
          proc->label_datetime = wssearch_label(type_table, "DATETIME");
     }

     proc->wsd_flush = wsdata_alloc(dtype_flush);
     if (proc->wsd_flush) {
          wsdata_add_reference(proc->wsd_flush);
     }
     else {
          return 0;
     }
     
     return 1; 
}


// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
//return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * input_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vinstance;

     //ignore input flushes
     if (input_type == dtype_flush) {
          return NULL;
     }

     //register output..
     //  in this example we could have just passed the input type
     //  in fact that is how to create a generic meta function..
     if (!proc->outtype_flush) {
          proc->outtype_flush = ws_add_outtype(olist, dtype_flush, NULL);
     }

     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
     }

     if (input_type == dtype_tuple) {
          proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
          return proc_process_tuple;
     }

     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }
     dprint("here");
     proc->outtype_meta[type_index] = ws_add_outtype(olist, input_type,
                                                     NULL);

     // we are happy.. now set the processor function
     return proc_process; // a function pointer
}

void do_flush_time(proc_instance_t * proc, time_t sec, ws_doutput_t * dout) {
     if (sysutil_test_time_boundary(&proc->epoch_boundary, sec) == 1) {
          proc->outcnt++;
          //tool_print("flushing");
          ws_set_outdata(proc->wsd_flush, proc->outtype_flush, dout);
     }
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 2 if not output
static int proc_process_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     if (proc->flush_time) {
          wsdata_t ** mset;
          int mset_len;
          int found = 0;

          if (tuple_find_label(input_data, proc->label_datetime,
                               &mset_len, &mset)) {
               if (mset_len && (mset[0]->dtype == dtype_ts)) {
                    //just choose first match key name
                    wsdt_ts_t * ts = (wsdt_ts_t*)mset[0]->data;
                    do_flush_time(proc, ts->sec, dout);
                    found = 1;
               }
          }
          if (!found) {
               do_flush_time(proc, time(NULL), dout);
          }
     }

     if (!proc->nopassthrough) {
          proc->outcnt++;
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
     }

     if (!proc->flush_time) {
          proc->counter++;

          if (proc->counter >= proc->limit) {
               proc->outcnt++;
               ws_set_outdata(proc->wsd_flush, proc->outtype_flush, dout);
               proc->counter=0;
          }
     }


     return 1;
}

static int proc_process(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     if (!proc->nopassthrough) {
          proc->outcnt++;
          ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     }

     proc->counter++;

     if (proc->counter >= proc->limit) {
          ws_set_outdata(proc->wsd_flush, proc->outtype_flush, dout);
          proc->counter=0;
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     wsdata_delete(proc->wsd_flush);

     //free dynamic allocations
     free(proc);

     return 1;
}

