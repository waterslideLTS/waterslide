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
#define PROC_NAME "splittuple"
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

char proc_version[]     = "1.5";
char *proc_alias[]     = { "tsplit", "tuplesplit", NULL};
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "creates multiple tuples from a larger tuple";
char *proc_tags[] = { "stream", NULL };
char proc_requires[] = "";

char proc_description[] = "splittuple is used to create new tuples from a "
"single input tuple.\n\n"
"Given a list of labels, splittuple will create a new tuple containing "
"the specified label for each label in the list. The -C flag can be used " 
"to carry a specific tuple member into every new tuple created. The -C "
"flag can be used multiple times to carry multiple members into the "
"newly-created tuples.";

char *proc_synopsis[]   = { "splittuple [-C <LABEL>] [-L <NAME>] <LABEL>", NULL };

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'C',"","label",
     "carry member with every tuple",0,0},
     {'L',"","label",
     "output tuple label",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

proc_example_t proc_examples[] = {
     {"... | splittuple WORD | print -VV", "split the input tuple into a new tuple containing just the WORD member"},
     {"... | splittuple -C DATETIME WORD SUBJECT | ...", "split the input tuple into two new tuples that both contain the same DATETIME member and either WORD or the SUBJECT member"},
     {"... | splittuple -L FRAGMENT -C DATETIME -C SUBJECT WORD | ...","split the input tuple into a new tuple containing WORD member, along with the DATETIME and SUBJECT members. Give the new tuple the label name of FRAGMENT"},
     {NULL,""}
};

char *proc_tuple_member_labels[] = {NULL};
char proc_nonswitch_opts[]    = "list of LABELS to split up tuple";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[] = {{NULL, NULL}};
char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[]   = {NULL};

#define MAX_LABELS 96

//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_nested_set_t nest;
     wslabel_nested_set_t nest_carry;
     wslabel_t * label_out;
     //wslabel_set_t lset;
     //wslabel_set_t lset_carry;
     //wslabel_t * labels[MAX_LABELS];
     //uint32_t numlabels;
     wsdata_t * carry[WSDT_TUPLE_MAX];
     int carrylen;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "C:L:")) != EOF) {
          switch (op) {
          case 'L':
               proc->label_out = wsregister_label(type_table, optarg);
               tool_print("adding label %s to output tuple", optarg);
               break;
          case 'C':
               wslabel_nested_search_build(type_table, &proc->nest_carry,
                                           optarg);
               tool_print("carrying data %s with every tuple", optarg);
               break;
          default:
               return 0;
          }
     }

     while (optind < argc) {
          wslabel_nested_search_build(type_table, &proc->nest, argv[optind]);
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
          proc->outtype_tuple = ws_add_outtype(olist, meta_type, proc->label_out);
          return process_tuple;
     }
     return NULL;
}

static inline int local_add_tuple(proc_instance_t * proc, ws_doutput_t * dout,
                                  wsdata_t * member) {
     wsdata_t * newtupledata = ws_get_outdata(proc->outtype_tuple);
     if (!newtupledata) {
          return 0;
     }
     int i;
     for (i = 0; i < proc->carrylen; i++) {
          add_tuple_member(newtupledata, proc->carry[i]);
     }
     add_tuple_member(newtupledata, member);
     ws_set_outdata(newtupledata, proc->outtype_tuple, dout);
     proc->outcnt++;
     return 1;
}

static int proc_nest_carry_callback(void * vinstance, void * vignore,
                                    wsdata_t * tdata,
                                    wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (proc->carrylen < WSDT_TUPLE_MAX) {
          proc->carry[proc->carrylen] = member;
          proc->carrylen++;
     }

     return 1;
}

static int proc_nest_split_callback(void * vinstance, void * vdout,
                                    wsdata_t * tdata,
                                    wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     ws_doutput_t * dout = (ws_doutput_t *)vdout;
     local_add_tuple(proc, dout, member);

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     proc->carrylen = 0;

     if (proc->nest_carry.cnt) {
          tuple_nested_search(input_data, &proc->nest_carry,
                              proc_nest_carry_callback,
                              proc,
                              NULL);
     }
     
     tuple_nested_search(input_data, &proc->nest,
                         proc_nest_split_callback,
                         proc,
                         dout);

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

