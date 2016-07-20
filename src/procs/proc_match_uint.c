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

//match uints
#define PROC_NAME "match_uint"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include "waterslide.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_uint16.h"
#include "datatypes/wsdt_uint8.h"
#include "datatypes/wsdt_int.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_double.h"
#include "waterslidedata.h"
#include "wstypes.h"
#include "procloader.h"

#define MATCHU_EQ 1
#define MATCHU_LT 2
#define MATCHU_GT 4
#define MATCHU_NE 8

char proc_name[]       = PROC_NAME;
char *proc_tags[]     = { "matching", NULL };
char proc_purpose[]    = "matches unsigned integer values to a given value";
char *proc_synopsis[]	= { "match_uint [-e|-g|-l] <value> <LABEL> [-L] <label>", NULL};
char proc_description[] = "Used to match an unsigned integer from the tuple to a given value. It requires as arguments an unsigned integer value, a tuple member to match, and a comparison operator to be used (using the '-e', '-n', '-g', or '-l' options). The '-L' option can also be used to add a new label to the match value. Any tuple that satisfies the match is then passed on to be processed in the remainder of the graph.";
proc_example_t proc_examples[]	= {
	{ "... | match_uint -e 80 NUMBER | ...", "This will only pass if the given NUMBER is equal to 80"},
	{ "... | match_uint -n 80 NUMBER | ...", "This will only pass if the given NUMBER is NOT equal to 80"},
	{ "... | match_uint -g 1024 NUMBER | ...", "This will only pass if the given NUMBER is greater than 1024"},
	{ "... | match_uint -l 1800 NUMBER | ...", "This will only pass if the given NUMBER is less than 1800"},
	{ "... | TAG:match_uint -e 80 NUMBER | ...", "This will only pass all tuples, but tag the ones that matched"},
        { "... | INVERSE:match_uint -e 80 NUMBER | ...", "This will only pass if the given NUMBER is NOT equal to 80. This example gives the same effect as using the '-n' option"},
	{ "... | INVERSE:match_uint -e 75 NUMBER -L NOT75 | ...", "This will only pass if the given NUMBER is NOT equal to 75 and it will assign matched values a new label of NOT75. This example gives the same effect as using the '-n' option"},
	{NULL,""}
}; 
char *proc_alias[]     = { "matchu", "uintmatch", "uintcmp", "cmpuint", NULL };
char proc_version[]     = "1.5";
char proc_requires[]	="";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[] = {
     {"none","pass if match"},
     {"TAG","pass all, tag tuple if match (requires -L flag)"},
     {"INVERSE","pass if no match"},
     {NULL, NULL}
};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[]        = {NULL};
proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'e',"","value",
     "uint must equal value",0,0},
     {'n',"","value",
     "uint must not equal value",0,0},
     {'g',"","value",
     "uint must be greater than value",0,0},
     {'l',"","value",
     "uint must be less than value",0,0},
     {'L',"","",
     "label to add to matched value",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of unsigned integer tuple  member to match";

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_meta_inverse(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_meta_tag(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_all(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_all_inverse(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_all_tag(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_nested_set_t nest;
     wslabel_t * label_match;
     uint64_t eq_value;
     uint64_t gt_value;
     uint64_t lt_value;
     uint64_t ne_value;
     uint32_t match_type;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "L:g:l:e:n:")) != EOF) {
          switch (op) {
          case 'n':
               proc->match_type |= MATCHU_NE;
               proc->ne_value = strtoull(optarg, NULL, 10);
               break;
          case 'g':
               proc->match_type |= MATCHU_GT;
               proc->gt_value = strtoull(optarg, NULL, 10);
               break;
          case 'l':
               proc->match_type |= MATCHU_LT;
               proc->lt_value = strtoull(optarg, NULL, 10);
               break;
          case 'e':
               proc->match_type |= MATCHU_EQ;
               proc->eq_value = strtoull(optarg, NULL, 10);
               break;
          case 'L':
               proc->label_match = wsregister_label(type_table,
                                                    optarg);
               tool_print("setting label to %s", optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build(type_table, &proc->nest, argv[optind]);
          tool_print("searching for string with label %s",
                     argv[optind]);
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
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * input_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vinstance;

     if (!wsdatatype_match(type_table, input_type, "TUPLE_TYPE")) {
          return NULL;  // not matching expected type
     }
     proc->outtype_tuple = ws_add_outtype(olist, input_type, NULL);

     if (wslabel_match(type_table, port, "INVERSE")) {
          if (!proc->nest.cnt) {
               return proc_process_all_inverse;
          }
          else {
               return proc_process_meta_inverse; // a function pointer
          }
     }
     else if (wslabel_match(type_table, port, "TAG")) {
          if (!proc->nest.cnt) {
               return proc_process_all_tag;
          }
          else {
               return proc_process_meta_tag; // a function pointer
          }
     }
     else {
          if (!proc->nest.cnt) {
               return proc_process_all;
          }
          else {
               return proc_process_meta; // a function pointer
          }
     }
}

static inline int find_match(proc_instance_t * proc, wsdata_t * wsd,
                             uint64_t uval, wsdata_t * tdata) {
     int rtn = 1;
     if (proc->match_type & MATCHU_EQ) {
          if (uval != proc->eq_value) {
               rtn = 0;
          }
     }
     else if (proc->match_type & MATCHU_NE) {
          if (uval == proc->ne_value) {
               rtn = 0;
          }
     }
     else {
          if (proc->match_type & MATCHU_LT) {
               if (uval >= proc->lt_value) {
                    rtn = 0;
               }
          }
          if (proc->match_type & MATCHU_GT) {
               if (uval <= proc->gt_value) {
                    rtn = 0;
               }
          }
     }
     if (rtn && proc->label_match && wsd) {
          tuple_add_member_label(tdata, wsd, proc->label_match);
     }
     return rtn;
}

static inline int member_match(proc_instance_t *proc, wsdata_t *member,
                               wsdata_t * wsd_label, wsdata_t * tdata) {
     int found = 0;

     uint64_t val;

     if (dtype_get_uint(member, &val)) {
          found = find_match(proc, wsd_label, val, tdata);
     }

     return found;
}

static int nest_search_callback_match(void * vproc, void * ignore,
                                      wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     return member_match(proc, member, member, tdata);
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int found = tuple_nested_search(input_data, &proc->nest,
                                     nest_search_callback_match,
                                     proc, NULL);

     if (found) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta_tag(void * vinstance, wsdata_t* input_data,
                                 ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int found = tuple_nested_search(input_data, &proc->nest,
                                     nest_search_callback_match,
                                     proc, NULL);

     if (found && proc->label_match) {
          wsdata_add_label(input_data, proc->label_match);
     }
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta_inverse(void * vinstance, wsdata_t* input_data,
                                     ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int found = tuple_nested_search(input_data, &proc->nest,
                                     nest_search_callback_match,
                                     proc, NULL);

     if (!found) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_all(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     wsdt_tuple_t * tuple = input_data->data;

     proc->meta_process_cnt++;

     int i;
     int tlen = tuple->len; //use this length because we are going to grow tuple
     wsdata_t * member;
     int found = 0;

     dprint("doing tuple search");
     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          found += member_match(proc, member, member, input_data);
     }
     if (found) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_all_tag(void * vinstance, wsdata_t* input_data,
                                   ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     wsdt_tuple_t * tuple = input_data->data;

     proc->meta_process_cnt++;

     int i;
     int tlen = tuple->len; //use this length because we are going to grow tuple
     wsdata_t * member;
     int found = 0;

     dprint("doing tuple search");
     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          found += member_match(proc, member, member, input_data);
     }
     if (found && proc->label_match) {
          wsdata_add_label(input_data, proc->label_match);
     }
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_all_inverse(void * vinstance, wsdata_t* input_data,
                                       ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     wsdt_tuple_t * tuple = input_data->data;

     proc->meta_process_cnt++;

     int i;
     int tlen = tuple->len; //use this length because we are going to grow tuple
     wsdata_t * member;
     int found = 0;

     dprint("doing tuple search");
     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          found += member_match(proc, member, NULL, NULL);
     }
     if (!found) {
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

