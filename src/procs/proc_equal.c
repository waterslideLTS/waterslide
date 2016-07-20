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

#define PROC_NAME "equal"
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
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "forwards tuple if specified elements are equal";

char *proc_synopsis[] = { "equal <LABELS> [-L <LABEL>]", NULL};
char *proc_tags[] = {"Equals", "Filters", "Matching", NULL};
char proc_description[] = {"Looks for tuples whose labels are equal to each other. "
	"This kid takes in a list of labels whose values will be compared. If all "
	"of the values are equal to each other, the tuple is passed on. If not, "
	"the tuple is dropped. If you don't want to drop the tuples whose labels "
	"aren't equal, you can use port TAGASMEMBER, which will append a member "
	"with value 0 if the labels don't equal each other and 1 if they do. This "
	"added member is labeled as EQUALRESULT by default and can be changed to a "
	"different label name by using option -L. If port TAGASMEMBER is not "
	"specified, the -L is option is ignored and non-equal items are dropped. "
	"Note that values must be exactly equal; for substring matching, " 
     "refer to the match kid."};
proc_example_t proc_examples[] = {
          {"... | equal LABEL1 LABEL2 | ...", "look for tuples that have equal values in LABEL1 and LABEL2 (LABEL1=LABEL2)"},
		{"... | equal LABEL1 LABEL2 LABEL3 | ...", "look for tuples that have equal values for LABEL1, LABEL2, and LABEL3 (LABEL1=LABEL2=LABEL3)"},
		{"... | equal LABEL1 LABEl2 | ...", "mark tuples whose LABEL1=LABEL2 with label EQUALRESULT of value 1, and items whose LABEL1 does not equal LABEL2 with label EQUALRESULT of value 0"},
		{"... | equal LABEL1 LABEl2 -L MYOTHERLABEL | ...", "does the same thing as previous example except uses label MYOTHERLABEL instead of EQUALRESULT to store 0/1 for non-equality/equality"},
          {NULL,""}
};
proc_port_t proc_input_ports[] = {
     {"none","check if labels are equal, drop if not equal"},
     {"TAGASMEMBER","create tuple member with result of equality"},
     {NULL, NULL}
};
char proc_requires[] = "none";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char *proc_tuple_member_labels[] = {"EQUALRESULT", NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_container_labels[] = {NULL};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'L',"","label",
     "output tuple label",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]   = "list of LABELS";




#define MAX_LABELS 32

//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int process_tuple_tagasmember(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_t * label_out;
     wslabel_t * label_tag;
     uint64_t hashes[WSDT_TUPLE_MAX];
     wslabel_set_t lset;
     //wslabel_t * labels[MAX_LABELS];
     //uint32_t numlabels;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "L:")) != EOF) {
          switch (op) {
          case 'L':
               proc->label_out = wsregister_label(type_table, optarg);
               proc->label_tag = proc->label_out;
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

     proc->label_tag = wsregister_label(type_table, "EQUALRESULT");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     if (!proc->lset.len) {
          error_print("need to specify tuple labels to search");
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
          if (wslabel_match(type_table, port, "TAGASMEMBER")) {
               proc->outtype_tuple = ws_add_outtype(olist, meta_type, NULL);
               return process_tuple_tagasmember;
          }
          proc->outtype_tuple = ws_add_outtype(olist, meta_type, proc->label_out);
          return process_tuple;
     }
     return NULL;
}

static inline uint64_t local_gethash(wsdata_t * wsd) {
     ws_hashloc_t* hashloc = wsd->dtype->hash_func(wsd);

     if (hashloc && hashloc->len) {
          return evahash64(hashloc->offset, hashloc->len, 0x12345678);
     }
     return 0;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int cmp = 0;

     wsdata_t ** mset;
     int mset_len;

     int i;
     int j;
     for (i = 0; i < proc->lset.len; i++) {
          if (tuple_find_label(input_data, proc->lset.labels[i], &mset_len, &mset)) {
               for (j = 0; j < mset_len; j++ ) {
                    if (cmp < WSDT_TUPLE_MAX) {
                         proc->hashes[cmp] = local_gethash(mset[j]);
                         for (i = 0; i < cmp; i++) {
                              if (proc->hashes[cmp] != proc->hashes[i]) {
                                   return 0;
                              }
                         } 
                         cmp++;
                    }
               }
          }
     }

     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;
     return 1;
}

static int process_tuple_tagasmember(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int cmp = 0;

     wsdata_t ** mset;
     int mset_len;

     int hasmatch = 1;

     int i;
     int j;
     for (i = 0; i < proc->lset.len; i++) {
          if (tuple_find_label(input_data, proc->lset.labels[i], &mset_len, &mset)) {
               for (j = 0; j < mset_len; j++ ) {
                    if (cmp < WSDT_TUPLE_MAX) {
                         proc->hashes[cmp] = local_gethash(mset[j]);
                         for (i = 0; i < cmp; i++) {
                              if (proc->hashes[cmp] != proc->hashes[i]) {
                                   hasmatch = 0;  //not equal
                              }
                         } 
                         cmp++;
                    }
               }
          }
     }

     tuple_member_create_uint(input_data, hasmatch, proc->label_tag);

     ws_set_outdata(input_data, proc->outtype_tuple, dout);
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

