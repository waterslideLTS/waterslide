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
#define PROC_NAME "label"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "datatypes/wsdt_tuple.h"

char proc_version[]     = "1.5";
char *proc_tags[]	= {"annotation", NULL};
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "labeler", "metalabel", "namer", NULL};
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Adds a user-specifed label to either a tuple member or the tuple container.";
char *proc_synopsis[]  = {"label <LABEL> [-m <LABEL>] [-R] [-e]", NULL};
char proc_description[] = "Appends a user-specified label to either a user-specified tuple member or if no member is specified, "
				"appends the label to the entire tuple container. It can also apply labels to members without labels "
				"and create new members labeled with only the user-specified label. "
				"New tuple members are added at the end of the tuple.";
proc_example_t proc_examples[] = {
	{"... | label NEWLABEL -m WORD | ...", "adds the label 'NEWLABEL' to all tuple members labeled 'WORD'"},
	{"... | label NEWLABEL -m WORD -R |  ...", "creates a new pointer to members with label 'WORD' and replaces all of the old labels with just 'NEWLABEL'"},
	{"... | label NEWLABEL | ..." , "applies the label 'NEWLABEL' to the entire container"},
	{"... | csv_in COLUMNONE | label -e OTHERCOLUMNS | ...", "csv_in will apply label 'COLUMNONE' to first column and then we label the rest of the unlabeled columns with label 'OTHERCOLUMNS'"},
	{NULL, ""}
};
char proc_requires[] = "none";


proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'m',"","LABEL",
     "label of tuple member to add label",0,0},
     {'R',"","",
     "create a pointer to member with just the new label and all other labels removed",0,0},
     {'P',"","",
     "create a pointer to member with just the new label, attach to root tuple",0,0},
     {'e',"","",
     "add label to empty member (no subtuples)",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABEL to add";
char *proc_input_types[]    = {"any", NULL};
char *proc_output_types[]    = {"any", NULL};
proc_port_t proc_input_ports[] = {{NULL, NULL}};

char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};

#define LOCAL_MAX_TYPES 25

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_relabel(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_add_to_empty(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];
     wslabel_nested_set_t nest;
     wslabel_t * label_tmember;
     wslabel_t * label;
     int relabel;
     int attach_root;
     int add2empty;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "PeRm:")) != EOF) {
          switch (op) {
          case 'e':
               proc->add2empty = 1;
               break;
          case 'P':
               proc->attach_root = 1;
               //intentional fallthrough
          case 'R':
               proc->relabel = 1;
               break;
          case 'm':
               if (wslabel_nested_search_build(type_table, &proc->nest,
                                               optarg)) {
                    tool_print("searching for label %s", optarg);
               }
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          proc->label = wsregister_label(type_table, argv[optind]);
          tool_print("labeling objects as %s", argv[optind]);
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
    
     if (!proc->label) {
          error_print("need to specify label");
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

     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          return NULL;
     }

     //register output..
     // pass the input type to the output..
     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type,
                                                     proc->label);

     if (proc->nest.cnt &&
         wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          return proc_process_tuple;
     }
     // we are happy.. now set the processor function
     if (proc->add2empty && (meta_type == dtype_tuple)) {
          return proc_add_to_empty;
     }
     if (proc->relabel) {
          return proc_relabel;
     }
     else {
          return proc_process_meta; // a function pointer
     }
}

static int proc_add_to_empty(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     wsdt_tuple_t * tuple = (wsdt_tuple_t*)input_data->data;
     
     int len = tuple->len;
     int i;
     for (i = 0; i < len; i++) {
          wsdata_t * member = tuple->member[i];
          if (member->label_len == 0) {
               tuple_add_member_label(input_data, member, proc->label);
          }
     }

     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     proc->outcnt++;
     return 1;
}


static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     if (!wsdata_check_label(input_data, proc->label)) {
          wsdata_add_label(input_data, proc->label);
     }

     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     proc->outcnt++;
     return 1;
}

static int proc_relabel(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     wsdata_t * ptr = wsdata_ptr(input_data);

     wsdata_add_label(ptr, proc->label);

     // this is new data.. pass as output
     ws_set_outdata(ptr, proc->outtype_meta[type_index], dout);
     proc->outcnt++;
     return 1;
}

//callback when searching nested tuple
static int proc_nest_match_callback(void * vproc, void * vroot,
                                    wsdata_t * subtdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     wsdata_t * root = (wsdata_t *)vroot;

     if (proc->relabel) {
          if (proc->attach_root) {
               tuple_member_add_ptr(root, member, proc->label);
          }
          else {
               tuple_member_add_ptr(subtdata, member, proc->label);
          }
     }
     else if (!wsdata_check_label(member, proc->label)) {
          tuple_add_member_label(subtdata, member, proc->label);
     }

     return 1;
}

static int proc_process_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     //search for specified tuple member
     tuple_nested_search(input_data, &proc->nest,
                         proc_nest_match_callback,
                         proc, input_data);


     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
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

