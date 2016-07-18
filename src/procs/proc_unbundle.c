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
#define PROC_NAME "unbundle"

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
char *proc_tags[]              =  {"Stream manipulation", "Profiling", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "unbundles data to minimize thread contention";
char proc_description[] = "Takes bundled data and separates it into its constituent event types.  Requires that the 'bundle' kid is used upstream and is useful when data is being transferred across thread boundaries to minimize overhead costs associated with placing many items on a kids external queue.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "";
char *proc_input_types[]       =  {"bundle", NULL};
// (Potential) Output types: tuple, flush
char *proc_output_types[]      =  {"tuple", "bundle", "flush", NULL};
char proc_requires[]           =  "bundle";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  {"unbundle", NULL};
proc_example_t proc_examples[] =  {
        {"source_keygen | bundle -N 1000 | unbundle | bandwidth", "Takes 1000 items at a time and places them in a single bundle"},
        {"... | bundle -n 1024 -> $bd; $bd | unbundle | nflow | reorderbuffer | ...; $bd | unbundle | bandwidth", "Demonstrates use of bundling across threads.  In the threaded case, a single bundle is placed on the next external queue for the unbundle kid heavily reducing the queueing costs across thread instances."},
        {"source_keygen @|| bandwidth", "A shorthand notation for using bundling/unbundling at thread boundaries using the default value of 'n'.  This would expand to 'source_keygen | bundle -n 16 || unbundle | bandwidth'"},
        {NULL, NULL},
};

//function prototypes for local functions
static int proc_unbundle(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     ws_outtype_t * outtype_bundle;
     ws_outtype_t * outtype_flush;
     wsdatatype_t * dtype_tuple;
     wsdatatype_t * dtype_flush;
     wsdatatype_t * dtype_bundle;
} proc_instance_t;

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

     proc->dtype_tuple = wsdatatype_get(type_table, "TUPLE_TYPE");
     proc->dtype_flush = wsdatatype_get(type_table, "FLUSH_TYPE");
     proc->dtype_bundle = wsdatatype_get(type_table, "BUNDLE_TYPE");
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

     if (!proc->outtype_tuple) {
          proc->outtype_tuple =
               ws_add_outtype_byname(type_table, olist, "TUPLE_TYPE", NULL);
     }

     if (!proc->outtype_bundle) {
          proc->outtype_bundle =
               ws_add_outtype_byname(type_table, olist, "BUNDLE_TYPE", NULL);
     }

     if (!proc->outtype_flush) {
          proc->outtype_flush =
               ws_add_outtype_byname(type_table, olist, "FLUSH_TYPE", NULL);
     }
     if (wsdatatype_match(type_table, input_type, "BUNDLE_TYPE")) {
          return proc_unbundle;  // not matching expected type
     }

     return NULL;
}

static int proc_unbundle(void * vinstance, wsdata_t* input_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     wsdt_bundle_t * b = (wsdt_bundle_t*)input_data->data;

     int i;
     for (i = 0; i < b->len; i++) {
          if (b->wsd[i]->dtype == proc->dtype_tuple) {
               ws_set_outdata(b->wsd[i], proc->outtype_tuple, dout);
          } 
          else if (b->wsd[i]->dtype == proc->dtype_bundle) {
               ws_set_outdata(b->wsd[i], proc->outtype_bundle, dout);
          }
          else if (b->wsd[i]->dtype == proc->dtype_flush) {
               ws_set_outdata(b->wsd[i], proc->outtype_flush, dout);
          }
          // we will delete this dtype in the bundle dtype destructor,
          // so do not invoke 'wsdata_delete(b->wsd[i]);' here!!!
          proc->outcnt++; 
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


