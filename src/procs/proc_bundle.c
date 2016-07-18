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
#define PROC_NAME "bundle"

//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <netinet/in.h>
#include <ctype.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_bundle.h"
#include "procloader.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"Input", "Stream manipulation", "Profiling", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "bundles data to minimize thread components";
char proc_description[] = "Takes a pre-specified number of events as input and creates a single bundle datatype that references all items as a single data element.  The value of 'N' can be specified using the '-[n|N]' option, and the default value is 16, with a maximum value of 1023. The bundle is more or less an array of pointers to the original input metadata items, accepting all datatypes as input.  The bundle type reduces the overhead cost associated with placing each individual element on the work queue, there is a definite performance improvement when using it in parallel, or threaded, pipelines.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'N',"","count",
     "number of records per bundle",0,0},
     {'n',"","count",
     "number of records per bundle",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "";
char *proc_input_types[]       =  {"tuple", "flush", "bundle", NULL};
// (Potential) Output types: bundle
char *proc_output_types[]      =  {"bundle", NULL};
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  {"bundle [-[n|N] <count>", NULL};
proc_example_t proc_examples[] =  {
	{"... | bundle -N 1000 | unbundle | bandwidth", "Takes a 1000 tuples at a time and places them in a single bundle"},
	{"... | bundle -n 1024 -> $bd; $bd | unbundle | nflow | reorderbuffer | ...; $bd | unbundle | bandwidth", "Demonstrates use of bundling across threads.  In the threaded case, a single bundle is placed on the next external queue for the unbundle kid heavily reducing the queueing costs across thread instances."},
	{"... @|| bandwidth", "A shorthand notation for using bundling/unbundling at thread boundaries using the default value of 'n'.  This would expand to '... | bundle || unbundle | bandwidth'"},
	{NULL, NULL},
};

//function prototypes for local functions
static int proc_process(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t meta_flow_cnt;
     uint64_t outcnt;
     uint64_t drops;

     int max;
     ws_outtype_t * outtype_bundle;
     wsdata_t *bundle;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv,
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "n:N:")) != EOF) {
          switch (op) {
          case 'n':
          case 'N':
               proc->max = atoi(optarg);
               if (proc->max > WSDT_BUNDLE_MAX) {
                    status_print("WARNING - specified bundle size (%d) exceeds WSDT_BUNDLE_MAX (%d) ... resetting to max",
                              proc->max, WSDT_BUNDLE_MAX);
                    proc->max = WSDT_BUNDLE_MAX;
               }
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

     proc->max = 16;

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
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * input_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vinstance;


     if (!proc->outtype_bundle) {
          proc->outtype_bundle =
               ws_add_outtype_byname(type_table, olist, "BUNDLE_TYPE", NULL);
     }

     if (wsdatatype_match(type_table, input_type, "TUPLE_TYPE")) {
          return proc_process;  // not matching expected type
     }
     if (wsdatatype_match(type_table, input_type, "BUNDLE_TYPE")) {
          return proc_process;  // not matching expected type
     }
     if (wsdatatype_match(type_table, input_type, "FLUSH_TYPE")) {
          return proc_flush;  // not matching expected type
     }

     error_print("%s does not support '%s' input", PROC_NAME, input_type->name);
     return NULL;
}

static int proc_process(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     if (!proc->bundle) {
          proc->bundle = ws_get_outdata(proc->outtype_bundle);
          if (!proc->bundle) {
               proc->drops++;
               return 0;
          }
          wsdata_add_reference(proc->bundle);
     }
     wsdt_bundle_t * b = (wsdt_bundle_t*)proc->bundle->data;

     b->wsd[b->len] = input_data;
     wsdata_add_reference(input_data);
     b->len++;


     if (b->len >= proc->max) {
          ws_set_outdata(proc->bundle, proc->outtype_bundle, dout);
          wsdata_delete(proc->bundle);
          proc->bundle = NULL;
          proc->outcnt++;
     }

     return 1;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (dtype_is_exit_flush(input_data)) {
          if (proc->bundle) {
               ws_set_outdata(proc->bundle, proc->outtype_bundle, dout);
               wsdata_delete(proc->bundle);
               proc->bundle = NULL;
               proc->outcnt++;
          }
     }
     else {
          proc_process(vinstance, input_data, dout, type_index);
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("meta_flow cnt %" PRIu64, proc->meta_flow_cnt);
     tool_print("output cnt  %" PRIu64, proc->outcnt);
     tool_print("drop     %" PRIu64, proc->drops);

     //free dynamic allocations
     free(proc);

     return 1;
}
