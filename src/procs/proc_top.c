/*
   No copyright is claimed in the United States under Title 17, U.S. Code.
   All Other Rights Reserved.

   Permission is hereby granted, free of charge, to any person obtaining a copy
   of
   this software and associated documentation files (the "Software"), to deal in
   the Software without restriction, including without limitation the rights to
   use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
   of the Software, and to permit persons to whom the Software is furnished to
   do
   so, subject to the following conditions:

   The above copyright notice and this permission notice shall be included in
   all
   copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
   */

//finds the top N of a stream
#define PROC_NAME "top"
//#define DEBUG 1

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "wstypes.h"
#include "datatypes/wsdt_tuple.h"
#include "wsheap.h"
#include "procloader.h"

char proc_version[]     = "1.5";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "selects the top N events based on value";

char *proc_synopsis[] = { "top <LABEL> [-N <records>] [-r] ", NULL};
char *proc_tags[] = {"Statistics", "State", "Tracking", NULL};
char proc_description[] = {"The top kid will track the top values of LABEL data.  this data must be an integer, an unsigned integer or a double"};
proc_example_t proc_examples[] = {
          {NULL,""}
};
char proc_requires[] = "none";
char proc_nonswitch_opts[]    = "LABEL of value to examine";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[] = {
     {NULL, NULL}
};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {"ACC", NULL};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'N',"","records",
     "maximum output records",0,0},
     {'r',"","",
     "find the N lowest events",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

#define VTYPE_UNKNOWN 0
#define VTYPE_U64 1
#define VTYPE_I64 2
#define VTYPE_DOUBLE 3

typedef union _local_value_t {
     uint64_t u64;
     int64_t i64;
     double dbl;
} local_value_t;

typedef struct _local_event_t {
     local_value_t value;
     wsdata_t * wsd;
} local_event_t;

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;
     wsheap_t * heap;
     uint64_t max;
     int value_type;
     wslabel_nested_set_t nest;
     int reverse;
     ws_outtype_t * outtype_tuple;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "rN:")) != EOF) {
          switch (op) {
          case 'r':
               proc->reverse = 1;
               break;
          case 'N':
               proc->max = strtoul(optarg, NULL, 0);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          //detect sublabels
          wslabel_nested_search_build(type_table, &proc->nest,
                                      argv[optind]);
          optind++;
     }

     return 1;
}

static int proc_compair_uint64(void * vfirst, void * vsecond) {
     local_event_t * first = (local_event_t*)vfirst;
     local_event_t * second = (local_event_t*)vsecond;

     if (first->value.u64 < second->value.u64) {
          return -1;
     }
     if (first->value.u64 == second->value.u64) {
          return 0;
     }
     else {
          return 1;
     }
}

static int proc_compair_uint64_reverse(void * vfirst, void * vsecond) {
     local_event_t * first = (local_event_t*)vfirst;
     local_event_t * second = (local_event_t*)vsecond;

     if (first->value.u64 < second->value.u64) {
          return 1;
     }
     if (first->value.u64 == second->value.u64) {
          return 0;
     }
     else {
          return -1;
     }
}

static int proc_compair_int64(void * vfirst, void * vsecond) {
     local_event_t * first = (local_event_t*)vfirst;
     local_event_t * second = (local_event_t*)vsecond;

     if (first->value.i64 < second->value.i64) {
          return -1;
     }
     if (first->value.i64 == second->value.i64) {
          return 0;
     }
     else {
          return 1;
     }
}

static int proc_compair_int64_reverse(void * vfirst, void * vsecond) {
     local_event_t * first = (local_event_t*)vfirst;
     local_event_t * second = (local_event_t*)vsecond;

     if (first->value.i64 < second->value.i64) {
          return 1;
     }
     if (first->value.i64 == second->value.i64) {
          return 0;
     }
     else {
          return -1;
     }
}

static int proc_compair_double(void * vfirst, void * vsecond) {
     local_event_t * first = (local_event_t*)vfirst;
     local_event_t * second = (local_event_t*)vsecond;

     if (first->value.dbl < second->value.dbl) {
          return -1;
     }
     if (first->value.dbl == second->value.dbl) {
          return 0;
     }
     else {
          return 1;
     }
}

static int proc_compair_double_reverse(void * vfirst, void * vsecond) {
     local_event_t * first = (local_event_t*)vfirst;
     local_event_t * second = (local_event_t*)vsecond;

     if (first->value.dbl < second->value.dbl) {
          return 1;
     }
     if (first->value.dbl == second->value.dbl) {
          return 0;
     }
     else {
          return -1;
     }
}

static void proc_replace(void * vrec, void * vreplace, void * vproc) {
     local_event_t * rec = (local_event_t*)vrec;
     local_event_t * replace = (local_event_t*)vreplace;
     if (rec->wsd) {
          wsdata_delete(rec->wsd);
     }
     rec->wsd = replace->wsd;
     wsdata_add_reference(rec->wsd);
     memcpy(&rec->value, &replace->value, sizeof(local_value_t));
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

     proc->max = 10;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (proc->nest.cnt == 0) {
          tool_print("must supply value to search for");
          return 0;
     }

     proc->heap = wsheap_init(proc->max, sizeof(local_event_t),
                              proc_compair_uint64,
                              proc_replace, proc); 

     if (!proc->heap) {
          tool_print("unable to create heap");
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
     if (meta_type == dtype_flush) {
          return proc_flush;
     }
     if (meta_type == dtype_tuple) {
          return proc_tuple;
     }

     return NULL; // a function pointer
}

static int nest_search_callback_match(void * vproc, void * vevent,
                                      wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     local_event_t * event = (local_event_t*)vevent;

     //if already filled
     if (event->wsd) {
          return 0;
     }

     switch(proc->value_type) {
     case VTYPE_UNKNOWN:
          //detect type...
          if (member->dtype == dtype_double) {
               proc->value_type = VTYPE_DOUBLE;
               if (proc->reverse) {
                    proc->heap->cmp = proc_compair_double_reverse;
               }
               else {
                    proc->heap->cmp = proc_compair_double;
               }
          }
          else if ((member->dtype == dtype_int) ||
                   (member->dtype == dtype_int64)) {
               proc->value_type = VTYPE_I64;
               if (proc->reverse) {
                    proc->heap->cmp = proc_compair_int64_reverse;
               }
               else {
                    proc->heap->cmp = proc_compair_int64;
               }
          }
          else {
               proc->value_type = VTYPE_U64;
               if (proc->reverse) {
                    proc->heap->cmp = proc_compair_uint64_reverse;
               }
               else {
                    proc->heap->cmp = proc_compair_uint64;
               }
          }
          return nest_search_callback_match(proc, vevent, tdata, member); 
     case VTYPE_U64:
          if (dtype_get_uint64(member, &event->value.u64)) {
               event->wsd = tdata;
               return 1;
          }
          break;
     case VTYPE_I64:
          if (dtype_get_int64(member, &event->value.i64)) {
               event->wsd = tdata;
               return 1;
          }
          break;
     case VTYPE_DOUBLE:
          if (dtype_get_double(member, &event->value.dbl)) {
               event->wsd = tdata;
               return 1;
          }
          break;
     }

     return 0;
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     dprint("proc_tuple");
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     //search key
     local_event_t event;
     event.wsd = NULL;

     int found = tuple_nested_search(input_data, &proc->nest,
                                     nest_search_callback_match,
                                     proc, &event);

     if (found) {
          event.wsd = input_data;
          wsheap_insert_replace(proc->heap, (void *)&event);
     }

     return 1;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     //tool_print("flushing heavy hitters");

     uint64_t count = proc->heap->count;
     wsheap_sort_inplace(proc->heap);

     uint64_t i;
     for (i = 0; i < count; i++) {
          local_event_t * event = (local_event_t*)proc->heap->heap[i];

          ws_set_outdata(event->wsd, proc->outtype_tuple, dout);
          proc->outcnt++;
          wsdata_delete(event->wsd);
          event->wsd = NULL;
     }

     wsheap_reset(proc->heap);

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     wsheap_destroy(proc->heap);

     //free dynamic allocations
     free(proc);

     return 1;
}

