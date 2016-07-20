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
#define PROC_NAME "dupetuple"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "waterslide_io.h"
#include "procloader.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_fixedstring.h"

char proc_version[]    = "1.5";
char *proc_menus[]     = { "Filters", NULL };
char *proc_tags[] = { "Stream Manipulation", NULL };
char *proc_alias[]     = { "dtuple", "duptup", "tupdup", NULL};
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "make a copy of the input tuple.";
char proc_description[] = "The dupetuple module makes a copy of the input tuple and forwards the copy.\n"
                  "The -d option specifies 2 depth levels of copy supported so far:\n"
                  "\t1. Create a new tuple and point the existing tuple members to it.\n"
                  "\t2. Same as 1 but create new copied members for the new tuple.\n"
                  "In both cases, the labels of the original tuple and the tuple members are copied "
                  "and a recursive copy is performed for all of the member tuples.\n"
                  "Depth 1 is a fast copy that allows for labels that are created "
                  "on the fly to be searched since the new tuple will have the new search "
                  "index entries.  But since the original tuple members are also members of the "
                  "new tuple, any changes made to those members (even in the original tuples) "
                  "will appear in the new tuple as well.  This can be a problem if you want "
                  "the tuple members to be independently modified.\n"
                  "Depth 2 creates new tuple members and copies the datatype so that changes "
                  "to the old or new tuple are local to that tuple.\n"
                  "For now, tuple member datatypes that have sub-dependancies (such as the "
                  "string type that is dependent on a tinystring or some other buffer datatype) do not "
                  "make copies of the dependent datatypes.\n"
                  "The default depth is 1.\n";
char *proc_synopsis[] = {"dupetuple", NULL};
proc_example_t proc_examples[] = {
     {"... | dupetuple | ..." , "create a new copy of the input tuple with pointers to existing members"},
     {"... | dupetuple -d 2 | ..." , "create a new copy of the input tuple and members"},
     {NULL, ""}};
char proc_requires[] = "";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'d',"","DEPTH",
     "set the depth of the copy <1 or 2>",0,0},
     {'L',"","TUPLE_LABEL",
     "add a label to the container labels",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[] = {{NULL, NULL}};
char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};

//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_t * label_out;
     int depth;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "d:L:")) != EOF) {
          switch (op) {
          case 'd':
               proc->depth = atoi(optarg);
               if ((proc->depth != 1) && (proc->depth !=2)) {
                    tool_print("Only depth of 1 or 2 supported! Exiting...");
                    return 0;
               }
               break;
          case 'L':
               proc->label_out = wsregister_label(type_table, optarg);
               tool_print("adding label %s to output tuple", optarg);
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
     proc->depth = 1;  //default is to copy tuple structure only

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

     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          if (!proc->outtype_tuple) {
               proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, proc->label_out);
          }
          return process_tuple;
     }
     return NULL;
}

static inline int tuple_deep_full_copy(wsdata_t* src, wsdata_t* dst, int depth) {
     wsdt_tuple_t * src_tuple = (wsdt_tuple_t*)src->data;
     // duplicate container label (not search member labels) ... for hierarchical/nested tuples, these
     // container labels will wind up being the member search labels for the nested subtuples
     wsdata_duplicate_labels(src, dst);
     uint32_t i;
     for (i = 0; i < src_tuple->len; i++) {
          if(src_tuple->member[i]->dtype == dtype_tuple) {
               wsdata_t * dst_sub_tuple = wsdata_alloc(dtype_tuple);
               if(!dst_sub_tuple) {
                    return 0;
               }
               // recursively copy each subtuple
               if(!tuple_deep_full_copy(src_tuple->member[i], dst_sub_tuple, depth)) {
                    wsdata_delete(dst_sub_tuple);
                    return 0;
               }
               // don't assume that add tuple member will always succeed
               if(!add_tuple_member(dst, dst_sub_tuple)) {
                    wsdata_delete(dst_sub_tuple);
                    return 0;
               }
          }
          else {
               wsdata_t * new_data;
               switch (depth)
               {
               case 1:
                    if(!add_tuple_member(dst, src_tuple->member[i])) {
                         return 0;
                    }
                    break;
               case 2: 
                    new_data = src_tuple->member[i]->dtype->copy_func(src_tuple->member[i]);
                    if(!add_tuple_member(dst,new_data)) {
                         return 0;
                    }
                    break;
               default:
                    return 0;
               }
          }
     }
     return 1;
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     wsdata_t * newtupledata = ws_get_outdata(proc->outtype_tuple);

     if (!newtupledata) {
          return 0;
     }

     if(!tuple_deep_full_copy(input_data, newtupledata, proc->depth)) {
          // couldn't deep copy tuple
          wsdata_delete(newtupledata);
          return 0;
     }
     ws_set_outdata(newtupledata, proc->outtype_tuple, dout);
     proc->outcnt++;
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

