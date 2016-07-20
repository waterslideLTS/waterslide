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
//removed members from tuple..
#define PROC_NAME "removefromtuple"
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
char *proc_tags[] = {"Stream Manipulation", NULL};
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "tupledelete", "tupleremove", NULL};
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Creates a new tuple, removing specified members";
char *proc_synopsis[] = { "removefromtuple <LIST_OF_LABELS> [-L <label>]", NULL };
char proc_description[] = "Creates a new tuple, and removes specified labeled members of the tuple.  Optionally, adds a container label to the tuple.";
char proc_requires[] = "";

proc_example_t proc_examples[] = {
     {"... | removefromtuple SPACE PERIOD | ...", "Remove the SPACE and PERIOD members from the tuples in the stream."},
     {"... | removefromtuple CONTENT -L CONTENTLESS | ...", "Remove the CONTENT member from a tuple, add the container label, CONTENTLESS, to the tuple"},
     {NULL,""}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'L',"","label",
     "output tuple label",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "list of LABELS";
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
     wslabel_set_t lset;
     wsdata_t * rmembers[WSDT_TUPLE_MAX];
     int rmember_len;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "L:")) != EOF) {
          switch (op) {
          case 'L':
               proc->label_out = wsregister_label(type_table, optarg);
               tool_print("adding label %s to output tuple", optarg);
               break;
          default:
               return 0;
          }
     }

     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset, argv[optind]);
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
          if (!proc->outtype_tuple) {
               proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, proc->label_out);
          }
          return process_tuple;
     }
     return NULL;
}

static inline void add_rmember(proc_instance_t * proc, wsdata_t * rmember) {
     int i;
     for (i = 0; i < proc->rmember_len; i++) {
          if (rmember == proc->rmembers[i]) {
               return;
          }
     }
     proc->rmembers[proc->rmember_len] = rmember;
     proc->rmember_len++;
}

static inline int not_removed(proc_instance_t * proc, wsdata_t * member) {
     int i;
     for (i = 0; i < proc->rmember_len; i++) {
          if (member == proc->rmembers[i]) {
               return 0;
          }
     }
     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     dprint("removefromtuple process_tuple start");
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     proc->rmember_len = 0;
     wsdata_t * newtupledata = ws_get_outdata(proc->outtype_tuple);

     if (!newtupledata) {
          return 0;
     }
     wsdata_duplicate_labels(input_data, newtupledata);

     int member_cnt = 0;
     wsdata_t ** mset;
     int mset_len;

     int i;
     int j;
     for (i = 0; i < proc->lset.len; i++) {
          if (tuple_find_label(input_data, proc->lset.labels[i], &mset_len,
                               &mset)) {
               for (j = 0; j < mset_len; j++ ) {
                    add_rmember(proc, mset[j]); 
               }
          }
     }
     dprint("removefromtuple walk tuple");

     wsdt_tuple_t * tuple = (wsdt_tuple_t*)input_data->data;
     for (i = 0; i < tuple->len; i++) {
          if (not_removed(proc, tuple->member[i])) {
               dprint("walk tuple list %d", i);
               add_tuple_member(newtupledata, tuple->member[i]);
               member_cnt++;
          }
     }
    
     // this is new data.. pass as output
     if (member_cnt == 0) {
          //delete empty tuple
          wsdata_delete(newtupledata);
          return 0;
     }
     ws_set_outdata(newtupledata, proc->outtype_tuple, dout);
     proc->outcnt++;
     dprint("removefromtuple process_tuple end");
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

