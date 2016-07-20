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
#define PROC_NAME "sample"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_flush.h"
#include "procloader.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
// Use to fix proc_tags: 
//char *proc_menus[]             =  { "Filters", NULL };
char *proc_tags[]              =  {"Filters", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "sample data - filter and select based on prob";
char proc_description[] = "Filter events by sampling based on a specified probability (default probability is 0.5).  When specifying the '-p' option, values should be between 0 and 1.  The '-F' option will forward upstream flushes for downstream processing.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'F',"","",
     "forwards flushes to subsequently connected kid(s)",0,0},
     {'p',"","probability",
     "probability with which each metadata independently continues",0,0},
     {'t',"","time",
     "sample every t seconds",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "None";
char *proc_input_types[]       =  {"any", NULL};
// (Potential) Output types: meta[LOCAL_MAX_TYPES]
char *proc_output_types[]      =  {"any", NULL};
char proc_requires[]           =  "None";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  {"sample [-p <value>] [-F]", NULL};
proc_example_t proc_examples[] =  {
	{"... | sample | ...", "Filters out roughly 50% of events"},
	{"... | sample -p 0.1 -F | ...", "Filter roughly 90% of traffic and forward flush events to downstream processors."},
	{NULL, NULL}
};

#define LOCAL_MAX_TYPES 25

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];
     int heartbeat_int;
     int fwdflush;
     uint32_t heartbeat_time;
     uint32_t nexttime;
} proc_instance_t;

static inline void set_heartbeat(proc_instance_t * proc, double heartbeat) {
     proc->heartbeat_int = (int)(heartbeat * (double)RAND_MAX);
}

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;
     double heartbeat=0.0;

     while ((op = getopt(argc, argv, "p:t:F")) != EOF) {
          switch (op) {
          case 't':
               proc->heartbeat_time=atoi(optarg);
               break;
          case 'p':
               heartbeat=strtod(optarg, NULL);
               set_heartbeat(proc, heartbeat);
               tool_print("setting heartbeat %f %d", heartbeat, proc->heartbeat_int);
               break;
          case 'F':
               proc->fwdflush = 1;
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

     proc->heartbeat_time = 0;
     set_heartbeat(proc, 0.5);

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
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

     //register output..
     // pass the input type to the output..
     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type, NULL);

     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          tool_print("trying to register flusher type");
          return proc_process_flush;
     }

     // we are happy.. now set the processor function
     return proc_process_meta; // a function pointer
}

//return 0 if data should be dropped, return 1 if we have heartbeat
static inline int check_heartbeat(proc_instance_t * proc) {
     if(proc->heartbeat_time==0) {
          if(rand() <= proc->heartbeat_int)
               return 1;
     } else {
          uint32_t t=time(NULL);
          if(t>=proc->nexttime) {
               proc->nexttime=t+proc->heartbeat_time;
               return 1;
          } else
               return 0;
     }

     return 0;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     if (check_heartbeat(proc)) {
          ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
          proc->outcnt++;
     }
     return 1;
}

static int proc_process_flush(void * vinstance, wsdata_t* flush_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     // perform necessary flush specific things, if any

     if (proc->fwdflush) {
          // distinguish exit flushes from inline/intermediate flushes as we
          // only forward inline flushes
          if(!dtype_is_exit_flush(flush_data)) {
               ws_set_outdata(flush_data, proc->outtype_meta[type_index], dout);
               proc->outcnt++;
          }
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free dynamic allocations
     free(proc);

     return 1;
}

