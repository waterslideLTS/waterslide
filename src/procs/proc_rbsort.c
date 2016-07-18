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
// performs a hopefully efficient online sort of keys using a Red-Black Tree
// to guarantee better worse-case performance
#define PROC_NAME "rbsort"

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
#include "procloader.h"
#include "rbtree.h"

#define LOCAL_MAX_TYPES 25

char proc_version[]     = "1.0";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "performs an online sort from smallest to largest (in a sliding window)";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'r',"","",
     "reverse sort (from largest to smallest)",0,0},
     {'M',"","records",
     "maximum buffer size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABEL of value to sort";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char *proc_tuple_member_labels[] = {"ACC", NULL};

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t *, ws_doutput_t *, int);
static int proc_flush(void *, wsdata_t *, ws_doutput_t *, int);
static int local_cmp(const void *, const void *);
static int local_cmp_reverse(const void *, const void *);


typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t len;
     uint64_t outcnt;

     uint32_t maxlen;
     wslabel_t * label_value;
     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];

     rb_tree_t * tree;
     int (*cmp_f)(const void * a, const void * b); 
     
} proc_instance_t;


static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "rM:")) != EOF) {
          switch (op) {
          case 'r':
               proc->cmp_f = local_cmp_reverse;
               break;
          case 'M':
               proc->maxlen = atoi(optarg);
               break;
          default:
               return 0;
          }
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
          (proc_instance_t*)calloc(1, sizeof(proc_instance_t));
     *vinstance = proc;

     ws_default_statestore(&proc->maxlen);

     proc->cmp_f = local_cmp;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if(!proc->label_value) {
          // default to label of KEY
          tool_print("no key specified...WARNING...defaulting to label: KEY");
          proc->label_value = wssearch_label(type_table, "KEY");
     }

     //other init
     proc->tree = RBTreeCreate(proc->cmp_f, NULL, NULL, proc->maxlen);

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
          return proc_flush;
     }
     else if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          return proc_tuple;
     }

     return NULL; // a function pointer
}

static int local_cmp(const void * d1, const void * d2) {
     if (*(double *)d1 > *(double *)d2) {
          return 1;
     }
     else if (*(double *)d1 < *(double *)d2) {
          return -1;
     }

     return 0;
}

static int local_cmp_reverse(const void * d1, const void * d2) {
     if (*(double *)d1 < *(double *)d2) {
          return 1;
     }
     else if (*(double *)d1 > *(double *)d2) {
          return -1;
     }

     return 0;
}

static inline void sort_emit(proc_instance_t * proc, ws_doutput_t * dout) {
     rb_node_t * min_node = NULL;
     if (proc->tree->num_nodes) {
          min_node = RBGetMinimum(proc->tree);

          ws_set_outdata(min_node->wsd, proc->outtype_meta[min_node->type_index], dout);
          proc->outcnt++;
          wsdata_delete(min_node->wsd);
          RBDelete(proc->tree, min_node);
     }
}

static inline void sort_dump(proc_instance_t * proc, ws_doutput_t * dout) {
     rb_node_t * min_node = NULL;
     while (proc->tree->num_nodes) {
          min_node = RBGetMinimum(proc->tree);

          ws_set_outdata(min_node->wsd, proc->outtype_meta[min_node->type_index], dout);
          proc->outcnt++;
          wsdata_delete(min_node->wsd);
          RBDelete(proc->tree, min_node);
     }
}

// proc processing function assigned to a specific data type in proc_io_init
// return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t * input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t *)vinstance;
     proc->meta_process_cnt++;

     //search key
     double * value = (double *)malloc(sizeof(double));
     if (!value) {
          error_print("failed malloc of value");
          return 0;
     }

     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->label_value,
                          &mset_len, &mset)) {
          if (!dtype_get_double(mset[0], value)) {
               // we did not find a proper value for the tuple
               return 0;
          }
     }
     else {
          // tuple label not found
          return 0;
     }

     proc->len++;

     if (proc->tree->num_nodes == proc->maxlen) {
          //if(*value < *(double *)proc->tree->min_node->key) 
          if(-1 == proc->cmp_f(value, proc->tree->min_node->key)) {
               // go ahead and emit this value...no need to insert into rbtree
               // ...it is already overdue
               //fprintf(stderr, "quick emit\n");
               ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
               proc->outcnt++;
          }
          else {
               // insert and then emit the next in ordered list
               wsdata_add_reference(input_data);
               RBTreeInsert(proc->tree, value, input_data, type_index);
               sort_emit(proc, dout);
          }
     }
     else {
          // add new value to tree
          wsdata_add_reference(input_data);
          RBTreeInsert_initial(proc->tree, value, input_data, type_index);
     }

     return 1;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     if (proc->tree->num_nodes) {
          sort_dump(proc, dout);
     }
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("processed data cnt %" PRIu64, proc->len);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     RBTreeDestroy(proc->tree);

     //free dynamic allocations
     free(proc);

     return 1;
}

