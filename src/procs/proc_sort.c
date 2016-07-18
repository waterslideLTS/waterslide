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
//keeps count of keys.. keep a representative tuple for each key..
#define PROC_NAME "sort"

//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_int.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_double.h"
#include "quicksort.h"
#include "procloader.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"Profiling", "Stream manipulation", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "Performs a windowed sort from largest to smallest numeric LABEL value";
 
proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'r',"","",
     "sort small to large aka in reverse",0,0},
     {'M',"","records",
     "maximum buffer size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "LABEL of value to sort";
char *proc_input_types[]       =  {"tuple", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
char proc_requires[]           =  "none";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  { "sort <LABEL> [-r ] [-M <SIZE>]", NULL};
proc_example_t proc_examples[] =  {
          {"... | sort VALUE | ...", "sorts tuples from largest to smallest VALUE"},
	  {"... | sort -r VALUE | ...", "sorts tuples from smallest to largest VALUE"},
	  {"... | sort COUNT -M 50 | ...", "Sizes the table to 50 records (least recently used values will be dropped.)"},
          {NULL,NULL}
};
char proc_description[] = "The sort kid sorts tuples from largest to smallest based on LABEL's value."
		" It only sorts numeric values (any real number) and cannot sort alphabetically."
		" Default is to sort from largest value to smallest; however, the -r option"
		" will result in a sort from smallest to largest. This kid uses a quicksort algorithm"
		" to perform sort."
		" Hashtable size is specified via the -M option, the default size is used of 350000 (specified in waterslide.h) or the"
		" environment variable WS_STATESTORE_MAX. Note: if label specified does not exist, does"
		" not sort or pass through anything.";

typedef struct _sort_data_t {
     double value;
     wsdata_t * wsd;
} sort_data_t;

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     sort_data_t * buf;
     sort_data_t ** sdata;

     uint32_t maxlen;
     int len;
     wslabel_t * label_value;
     ws_outtype_t * outtype_tuple;
     int reverse;
} proc_instance_t;


static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "rM:")) != EOF) {
          switch (op) {
          case 'r':
               proc->reverse = 1;
               break;
          case 'M':
               proc->maxlen = atoi(optarg);
               break;
          default:
               return 0;
          }
     }

     if(optind == argc) {
          tool_print("no key specified...exiting");
          return 0;
     }

     while (optind < argc) {
          proc->label_value = wssearch_label(type_table, argv[optind]);
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

     ws_default_statestore(&proc->maxlen);

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     //make an array of ptr
     proc->buf = (sort_data_t*)calloc(proc->maxlen, sizeof(sort_data_t));
     if (!proc->buf) {
          error_print("failed calloc of proc->buf");
          return 0;
     }
     proc->sdata = (sort_data_t**)calloc(proc->maxlen, sizeof(sort_data_t *));
     if (!proc->sdata) {
          error_print("failed calloc of proc->sdata");
          return 0;
     }

     int i;
     for (i = 0; i < proc->maxlen; i++) {
          proc->sdata[i] = &proc->buf[i];
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
     else if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          return proc_tuple;
     }

     return NULL; // a function pointer
}

static int local_cmp(void * d1, void *d2, void * ignore) {
     sort_data_t * s1 = (sort_data_t*)d1;
     sort_data_t * s2 = (sort_data_t*)d2;

     if (s2->value > s1->value) {
          return 1;
     }
     return 0;
}

static int local_cmp_reverse(void * d1, void *d2, void * ignore) {
     sort_data_t * s1 = (sort_data_t*)d1;
     sort_data_t * s2 = (sort_data_t*)d2;

     if (s2->value < s1->value) {
          return 1;
     }
     return 0;
}

static inline void sort_dump(proc_instance_t * proc, ws_doutput_t * dout) {
     if (proc->len) {
          if (proc->reverse) {
               quicksort((void**)proc->sdata, proc->len, local_cmp_reverse, NULL);
          }
          else {
               quicksort((void**)proc->sdata, proc->len, local_cmp, NULL);
          }
     }
     int i;
     for (i = 0; i < proc->len; i++) {
          ws_set_outdata(proc->sdata[i]->wsd, proc->outtype_tuple, dout);
          proc->outcnt++;
          wsdata_delete(proc->sdata[i]->wsd);
     }
     proc->len = 0;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     //search key
     double value = 0;
     int found = 0;

     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->label_value,
                          &mset_len, &mset)) {
          if (dtype_get_double(mset[0], &value)) {
               found = 1;
          }
     }
     else {
          return 0;
     }

     if (!found) {
          value = -1;
     }

     proc->sdata[proc->len]->value = value;
     proc->sdata[proc->len]->wsd = input_data;
     wsdata_add_reference(input_data);

     proc->len++;

     if (proc->len == proc->maxlen) {
          sort_dump(proc, dout);
     }

     return 1;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     dprint("sort flush %s", proc->len);
     if (proc->len) {
          sort_dump(proc, dout);
     }
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     if (proc->len) {
	  tool_print("ERROR - unflushed data %d", proc->len);
	  int i;
	  for (i = 0; i < proc->len; i++) {
	       wsdata_delete(proc->sdata[i]->wsd);
	  }
     }

     //free dynamic allocations
     free(proc->sdata);
     free(proc->buf);
     free(proc);

     return 1;
}

