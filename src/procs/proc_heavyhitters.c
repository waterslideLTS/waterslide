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

//keeps count of keys.. keep a representative tuple for each key..
#define PROC_NAME "heavyhitters"
//#define DEBUG 1

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_uint16.h"
#include "datatypes/wsdt_uint8.h"
#include "datatypes/wsdt_int.h"
#include "datatypes/wsdt_uint64.h"
#include "heavyhitters.h"
#include "wstypes.h"
#include "procloader.h"

char proc_version[]     = "1.5";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "keeps track of top items";

char *proc_synopsis[] = { "heavyhitters <LABEL> [-1 ] [-K <label>] [-V <label>] [-L <label>] [-R] [-N <records>] [-S] [-h]", NULL};
char *proc_tags[] = {"Statistics", "State", "Tracking", NULL};
char proc_description[] = {"The heavyhitters kid will track the top hits using a specified label (-K explicitly sets that label). For example, if provided the "
	"WORD label, this kid will return the top words seen within the data. It tracks the number of tuples it sees with "
	"that WORD and will track the top values until flushed or tables are filled. "
	"By default, heavyhitters will keep the first tuple it sees with a new value "
	"of the label it's tracking. Using -S, it can be told to keep the last tuple it sees with the tracked label. "
	"Or with -R, heavyhitters will only keep the tracked label and no other tuple data. "
	"Using -V <label>, heavyhitters will use the value in <label> to determine the top items by adding that value to the global count (instead "
	"of the default of 1). "
	"Also, using the -N option, heavy hitters can be told how many records to output: e.g. -N 5 will give the top 5. 10 is the default output. "
	"Lastly, heavyhitters adds the label ACC that "
     "contains the accumulated value used to determine the top hits (to specify a different label, use -L). "
	"The heavyhitters kid is based on the 'Efficient Computation of Frequent and Top-k Elements in Data Streams,' by Metwally, Agrawal and Abbadi; 2005.  The algorithm does an approximate frequency estimation using a fixed amount of memory.  On heavily skewed data sets, the approximation will have extremely tight error bounds and the results will be highly accurate.  However, if the distribution of items is all unique (i.e., single or low counts; low skew), the output counts may appear to be dramatically incorrect.  Error bounds are not printed in current output, and will be much higher for low skew distributions."};
proc_example_t proc_examples[] = {
          {"... | heavyhitters WORD | ...", "determines the most used words"},
          {"... | heavyhitters WORD -R | ...", "determines the most used words and only keep the words, not the whole tuple"},
          {"... | heavyhitters SENTENCE -V WORDLENGTH | ...", "determines the longest sentences by accumulating the work length"},

          {NULL,""}
};
char proc_requires[] = "none";
char proc_nonswitch_opts[]    = "LABEL of key to count (only one)";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[] = {
     {"FLUSH", "Port on which only flushes are processed."},
     {NULL, NULL}
};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {"ACC", NULL};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'1',"","",
     "only flush once",0,0},
     {'V',"","label",
     "set label of value to accumulate with",0,0},
     {'L',"","label",
     "set label of new accumulated value",0,0},
     {'R',"","",
     "keep copy of key, not whole tuple",0,0},
     {'N',"","records",
     "maximum output records",0,0},
     {'M',"","records",
     "maximum internal-space records",0,0},
     {'S',"","",
     "store last instance of key rather than first",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

typedef struct _key_data_t {
     wsdata_t * wsd;
} key_data_t;


static void kdata_release(void * vkdata) {
     key_data_t * kdata = (key_data_t*)vkdata;
     if (kdata && kdata->wsd) {
          wsdata_delete(kdata->wsd);
     }
}

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     heavyhitters_t * hitters;
     uint64_t max_out;
     uint64_t max_buffers;
     wslabel_t * label_key;
     wslabel_t * label_value;
     ws_outtype_t * outtype_tuple;
     wslabel_t * label_acc;
     int keep_key;
     int flush_once;
     int flushes;
     int swap_lastkey;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "S1RL:M:V:N:")) != EOF) {
          switch (op) {
          case 'S':
               proc->swap_lastkey = 1;
               break;
          case '1':
               proc->flush_once = 1;
               break;
          case 'R':
               proc->keep_key = 1;
               tool_print("store only last key rather than whole tuple");
               break;
          case 'L':
               proc->label_acc = wsregister_label(type_table, optarg);
               tool_print("result label %s", optarg);
               break;
          case 'V':
               proc->label_value = wssearch_label(type_table, optarg);
               tool_print("using value %s", optarg);
               break;
          case 'N':
               proc->max_out = strtoul(optarg, NULL, 0);
               break;
          case 'M':
               proc->max_buffers = strtoul(optarg, NULL, 0);
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

     proc->max_out = 10;
     proc->label_acc = wsregister_label(type_table, "ACC");

     proc->max_buffers = 25000;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->label_key) {
          tool_print("must specify at least a key label");
          return 0;
     }
     if (proc->max_out > proc->max_buffers) {
          proc->max_out = proc->max_buffers;
          tool_print("limited to %"PRIu64" elements to track", proc->max_buffers);
     }

     proc->hitters = heavyhitters_init(proc->max_buffers, sizeof(key_data_t),
                                       kdata_release);

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
          if ( wslabel_match(type_table, port, "FLUSH") ) return NULL;
          return proc_tuple;
     }

     return NULL; // a function pointer
}

static inline void add_member(proc_instance_t * proc, wsdata_t * tdata,
                              wsdata_t * key, uint64_t value) {
     ws_hashloc_t * hashloc = key->dtype->hash_func(key);
     if (!hashloc || !hashloc->len) {
          return;
     }

     hh_node_t * node = heavyhitters_increment(proc->hitters, hashloc->offset,
                                               hashloc->len, value);

     key_data_t * kdata = (key_data_t*)node->data;
     if (kdata) {
          if (kdata->wsd && proc->swap_lastkey) {
               wsdata_delete(kdata->wsd);
               kdata->wsd = NULL;
          }
          if (!kdata->wsd) {
               if (proc->keep_key) {
                    kdata->wsd = key;
                    wsdata_add_reference(key);
               }
               else {
                    kdata->wsd = tdata;
                    wsdata_add_reference(tdata);
               }
          }
     }
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
     uint64_t value = 1;

     wsdata_t ** mset;
     int mset_len;
     int i;

     if (proc->label_value){
          if (tuple_find_label(input_data, proc->label_value,
                               &mset_len, &mset)) {
               //find first value
               for (i = 0; i < mset_len; i++) {
                    if (dtype_get_uint64(mset[i], &value)) {
                         break;
                    }
               }
          }
     }

     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          for (i = 0; i < mset_len; i++) {
               add_member(proc, input_data, mset[i], value);
          }
     }

     return 1;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     //tool_print("flushing heavy hitters");
     proc->flushes++;

     uint64_t cnt = 0;
     dprint("flushing %"PRIu64, proc->hitters->count);
     if (proc->flush_once && (proc->flushes > 1)) {
          heavyhitters_reset(proc->hitters);
          return 0;
     }

     hh_node_t * cursor;
     for (cursor = proc->hitters->q_head;
          cursor && (cnt < proc->max_out);
          cursor = cursor->q_next) {
          cnt++;

          key_data_t * kdata = (key_data_t*)cursor->data;
          if (kdata && kdata->wsd) {
               wsdata_t * tdata;
               if (proc->keep_key) {
                    tdata = ws_get_outdata(proc->outtype_tuple);
                    wsdata_add_reference(tdata);
                    if (tdata) {
                         add_tuple_member(tdata, kdata->wsd);
                    }
               }
               else {
                    tdata = kdata->wsd;
                    wsdata_add_reference(tdata);
               }
                    
               if (tdata) {
                    tuple_member_create_uint64(tdata, cursor->value, proc->label_acc);
                    ws_set_outdata(tdata, proc->outtype_tuple, dout);
                    proc->outcnt++;
                    wsdata_delete(tdata);
               }
          }
     }

     heavyhitters_reset(proc->hitters);

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     heavyhitters_destroy(proc->hitters);

     //free dynamic allocations
     free(proc);

     return 1;
}

