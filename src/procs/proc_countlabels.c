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
#define PROC_NAME "countlabels"

//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include <math.h>
#include "waterslide.h"
#include "datatypes/wsdt_tuple.h"
#include "wstypes.h"
#include "sysutil.h"
#include "waterslidedata.h"
#include "procloader.h"

#define COND_EQUAL 1
#define COND_GREATER 2
#define COND_LESS 3

//function prototypes for local functions
char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"Annotation", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "count the number of specified labels in a tuple";
char proc_description[] = "count the number of specified labels in a tuple.  This kid can also be used to filter based on the number of labels found in a tuple using the '-g' (greater than count), '-e' (equal to count), or '-l' (less than count) options.  NOTE: You can only use one of these options at a time.  The '-L' option can be used to change the default appended label, LABELCNT, to a user-specified label.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'m',"","",
     "count the total number of tuple members in this tuple"},
     {'e',"","value",
     "pass if count is equal to value",0,0},
     {'l',"","value",
     "pass if count less than value",0,0},
     {'g',"","value",
     "pass if count greater than value",0,0},
     {'L',"","label",
     "Label of output count",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]     =  "LABELs of attributes to count";
char *proc_input_types[]       =  {"tuple", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {"LABELCNT", "MEMBERCNT", NULL};
char *proc_synopsis[]          =  {"countlabels <LABEL> [-m][-e <value>][-l <value>][-g <value>][-L <NEW_LABEL>]", NULL};
proc_example_t proc_examples[] =  {
	{"... | countlabels CONTENT | ...", "Count the number of CONTENT labels in a given tuple; place calculated value in the LABELCNT buffer."},
        {"... | countlabels ATTACHMENT -g 3 -L NUMATTACHMENTS | ...", "Count the number of ATTACHMENT labels in a given tuple, pass if greater than 3 ATTACHMENTs are present, append a new label, NUMATTACHMENTS, with the count to the tuple."}, 
	{NULL, NULL}
}; 
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     int condition;
     int condition_value;
     ws_outtype_t * outtype_tuple;
     wslabel_nested_set_ext_t nest;
     wslabel_t * label_out;
     wslabel_t * label_membercnt;
     int num_attr;
     int num_members;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "me:l:g:L:")) != EOF) {
          switch (op) {
          case 'e':
               proc->condition_value = atoi(optarg);
               proc->condition = COND_EQUAL;
               break;
          case 'l':
               proc->condition_value = atoi(optarg);
               proc->condition = COND_LESS;
               break;
          case 'g':
               proc->condition_value = atoi(optarg);
               proc->condition = COND_GREATER;
               break;
          case 'm':
               proc->num_members = 1;
               break;
          case 'L':
               proc->label_out = wsregister_label(type_table, optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build_ext(type_table, &proc->nest, argv[optind],
                                          proc->num_attr);
          proc->num_attr++;
          tool_print("searching for string with label %s", argv[optind]);
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

     proc->label_out = wsregister_label(type_table, "LABELCNT");
     proc->label_membercnt = wsregister_label(type_table, "MEMBERCNT");


     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     if (!proc->num_attr) {
          tool_print("ERROR: must specify member labels to count"); 
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

     if (input_type != dtype_tuple) {
          return NULL;  // not matching expected type
     }
     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
     }

     return proc_process_meta; // a function pointer
}

static int proc_nest_count_attr(void * vinstance, void * vcnt,
                                wsdata_t * tdata, wsdata_t * attr,
                                wslabel_t * label, int id) {
     return 1;
}

static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int val = tuple_nested_search_ext(input_data, &proc->nest,
                                       proc_nest_count_attr,
                                       proc, NULL);

     int pass = 1;
     switch (proc->condition) {
     case COND_EQUAL:
          if (val != proc->condition_value) {
               pass = 0;
          }
          break;
     case COND_LESS:
          if (val >= proc->condition_value) {
               pass = 0;
          }
          break;
     case COND_GREATER:
          if (val <= proc->condition_value) {
               pass = 0;
          }
          break;
     }

     if (pass) {
          tuple_member_create_uint(input_data, (uint32_t)val, proc->label_out);
          if (proc->num_members) {
               wsdt_tuple_t * tdata = (wsdt_tuple_t *)input_data->data;
               tuple_member_create_uint(input_data, (int32_t)tdata->len,
                                        proc->label_membercnt);
          }
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
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

