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
#define PROC_NAME "denest"
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

char proc_name[]        = PROC_NAME;
char *proc_tags[]	= { "Stream Manipulation", NULL };
char proc_purpose[]     = "flatten a nested tuple";
char *proc_synopsis[]	= { "denest [-A] [-L <LABEL>] <nested tuple list for denesting>", NULL };
char proc_description[]	= "Flatten nested tuples for easier access to the tuples inside the nesting. With the -A flag the denested tuple elements are appended to the existing tuple. The -L flag allows for a new label to be created and applied to denested elements.";
proc_example_t proc_examples[]  = {
	{"... | denest FLATTENME | ...", "flatten the nested tuple 'FLATTENME' and pass on only the denested tuple elements discarding the rest of the original tuple."},
	{"... | denest -A FLATTENME | ...", "flatten the nested tuple 'FLATTENME', append all denested elements to the origional tuple, and pass the original tuple with the denested elements appended to it."},
	{"... | denest FLATTENME -L NEWLABEL | ...", "flatten the nested tuple 'FLATTENME' and pass on only the denested tuple elements discarding the rest of the original tuple. Create a new label 'NEWLABEL' to be applied to all denested elements."},
	{"... | denest -A FLATTENME -L NEWLABEL | ...", "flatten the nested tuple 'FLATTENME', append all denested elements to the origional tuple, and pass the original tuple with the denested elements appended to it. Create a new label 'NEWLABEL' to be applied to all denested elements."},
	{NULL, ""}
};	
char *proc_alias[]     = { "unnest", "flatten", NULL};
char proc_version[]     = "1.5";
char proc_requires[]    = "";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[]  = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[]        = {NULL};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'A',"","",
     "append denested strings to existing tuple",0,0},
     {'S',"","LABEL",
     "put extracted items into new nested tuple",0,0},
     {'L',"","LABEL",
     "apply label to denested elements",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "list of nested tuple to denest";

//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_nested_set_t nest;
     int append;
     wslabel_t * label_apply;
     wslabel_t * label_subtuple;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "s:S:AaL:")) != EOF) {
          switch (op) {
          case 'S':
          case 's':
               proc->label_subtuple = wsregister_label(type_table, optarg);
               //intentionally fall through
          case 'A':
          case 'a':
               proc->append = 1;
               break;
          case 'L':
               proc->label_apply = wsregister_label(type_table, optarg);
               break;
          default:
               return 0;
          }
     }

     while (optind < argc) {
          //detect sublabels
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
   
     if (proc->nest.cnt == 0) {
          tool_print("must specify tuple members to denest");
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
               proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
          }
          return process_tuple;
     }
     return NULL;
}

static int proc_nest_tuple(void * vproc, void * vnewtup,
                         wsdata_t * tdata, wsdata_t * member) {

     proc_instance_t * proc = (proc_instance_t *)vproc;
     wsdata_t * newtup = (wsdata_t *)vnewtup;

     if (member->dtype == dtype_tuple) {
          wsdt_tuple_t * tup = (wsdt_tuple_t *)member->data;
          int i;
          for (i = 0; i < tup->len; i++) {
               add_tuple_member(newtup, tup->member[i]);
               if (proc->label_apply) {
                    tuple_add_member_label(newtup, tup->member[i],
                                           proc->label_apply);
               }
          }
     }
     else {
          if (proc->label_apply) {
               tuple_member_add_ptr(newtup, member, proc->label_apply);
          }
          else {
               add_tuple_member(newtup, member);
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

     wsdata_t * newtupledata;
     if (proc->label_subtuple) {
          newtupledata = tuple_member_create_wsdata(input_data, dtype_tuple,
                                                    proc->label_subtuple);
          if (!newtupledata) {
               return 0;
          }
     }
     else if (proc->append) {
          newtupledata = input_data;
     }
     else {
          newtupledata = ws_get_outdata(proc->outtype_tuple);

          if (!newtupledata) {
               return 0;
          }
     }

     tuple_nested_search(input_data, &proc->nest,
                         proc_nest_tuple,
                         proc, newtupledata);

     if (proc->append) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }
     else {
          wsdt_tuple_t * tup = (wsdt_tuple_t *)newtupledata->data;

          if (tup->len) {
               ws_set_outdata(newtupledata, proc->outtype_tuple, dout);
               proc->outcnt++;
          }
          else {
               wsdata_delete(newtupledata);
          }
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

