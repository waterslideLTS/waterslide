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
#define PROC_NAME "subtuple"
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
char *proc_alias[]     = { "stuple", "subtup", "tupsub", NULL};
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Creates a new tuple containing a subset of data from an existing tuple.";
char proc_description[] = "The subtuple module is used to create a new tuple "
     "containing a subset of items in an existing tuple.  This is extremely "
     "useful for manipulating the stream to reduce the number of items carried "
     "by the tuple and can be used to simplify the stream for viewing and output. "
     "Options exist for generating labels when values don't exist, subsetting to "
     "a single label when multiple identical labels exist, labeling the tuple "
     "container, or dropping extra labels for a value.  Note that without adding "
     "the '-R' option, subtuple will always strip container labels from the new tuple.";
char *proc_synopsis[] = {"subtuple [<LABELS_IN_SUBSET>][-NqR1][-L <TUPLE_LABEL>]", NULL};
proc_example_t proc_examples[] = {
     {"... | subtuple NOUN VERB | ..." , "create a new tuple containing "
          "only NOUN and VERB labels"},
     {"... | subtuple -q SUBJECT VERB | ...", "create a new tuple containing "
          "only SUBJECT and VERB labels; drop any additional labels "
               "associated with the SUBJECT and VERB labels"},
     {"... | subtuple DATETIME RECORD -R | ...", "create a new tuple containing "
          "DATETIME and RECORD labels; also carry forward existing tuple "
               "container labels"},
     {"... | subtuple MATCH -1 -N -L MAX | ...", "create a new tuple with the "
          "label MAX containing the first MATCH label; if MATCH does not exist, "
               "create a MATCH label with a NULL value"},
     {NULL, ""}};
char proc_requires[] = "";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'1',"","",
     "add only one data element per label",0,0},
     {'N',"","",
     "add null string if output does not exist",0,0},
     {'R',"","",
     "keep existing tuple container labels",0,0},
     {'L',"","TUPLE_LABEL",
     "output tuple label as container label",0,0},
     {'q',"","",
     "only include labels chosen by user (drop extra labels)",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABELS_IN_SUBSET";
char *proc_input_types[]    = {"tuple", "monitor", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[] = {{NULL, NULL}};
char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};

//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int process_dupe(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_monitor(void *, wsdata_t *, ws_doutput_t *, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_t * label_out;
     wslabel_t * label_process_cnt;
     wslabel_t * label_subtuple;
     wslabel_nested_set_t nest;
     wsdata_t * wsd_nullstr;
     int add_nullstr;
     int apply_container_labels;
     int only_one;
     int reset_label;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "q1RNL:")) != EOF) {
          switch (op) {
          case 'q':
               proc->reset_label = 1;
               break;
          case '1':
               proc->only_one = 1;
               break;
          case 'R':
               proc->apply_container_labels = 1;
               tool_print("applying tuple container labels");
               break;
          case 'N':
               proc->add_nullstr = 1;
               tool_print("adding null string to replace missing data");
               break;
          case 'L':
               proc->label_out = wsregister_label(type_table, optarg);
               tool_print("adding label %s to output tuple", optarg);
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

     char * nstr = "NULL";
     wsdata_t * wsnull = wsdata_create_string(nstr, strlen(nstr));

     if (wsnull) {
          proc->wsd_nullstr = wsnull;
          wsdata_add_reference(wsnull);
     }

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
  
     //monitor_type labels 
     proc->label_process_cnt = wsregister_label(type_table, "PROCESS_CNT");
     proc->label_subtuple = wsregister_label(type_table, "SUBTUPLE");
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
          if (proc->nest.cnt == 0) {
               return process_dupe;
          }
          return process_tuple;
     }

     //callback for receiving and responding to monitor requests
     if (wsdatatype_match(type_table, meta_type, "MONITOR_TYPE") )
     {
	return proc_monitor;
     }

     return NULL;
}

static inline int add_nullstring(proc_instance_t * proc, wsdata_t * tdata,
                                 wslabel_t * label) {
     wsdata_t * wsd = tuple_member_add_ptr(tdata, proc->wsd_nullstr, label);
     return (wsd != NULL);
}

static inline int add_searchlabels(proc_instance_t * proc,
                                   wslabel_nested_set_t * nest,
                                   wsdata_t * input_data, 
                                   wsdata_t * newtupledata, wslabel_set_t * lset) {
     int i, j;
     int member_cnt = 0;
     wsdata_t ** mset;
     int mset_len;

     if (proc->only_one) {
          for (i = 0; i < lset->len; i++) {
               if (tuple_find_label(input_data,
                                    lset->labels[i], &mset_len,
                                    &mset)) {
                    if (lset->id[i]) {
                         for (j = 0; j < mset_len; j++ ) {
                              dprint("subtuple i %u j %u, %s", i, j,
                                     lset->labels[i]->name);
                              //search for sublabels
                              member_cnt +=
                                   add_searchlabels(proc, nest, mset[j],
                                                 newtupledata,
                                                 &nest->lset[lset->id[i]]); 
                         }
                    }
                    else {
                         if (proc->reset_label) {
                              tuple_member_add_ptr(newtupledata, mset[0],
                                                   lset->labels[i]);
                         }
                         else {
                              add_tuple_member(newtupledata, mset[0]);
                         }
                         member_cnt++;
                    }
               }
               else if (proc->add_nullstr) {
                    member_cnt += add_nullstring(proc, newtupledata,
                                                 lset->labels[i]);
               }

          }
     }
     else {
          for (i = 0; i < lset->len; i++) {
               if (tuple_find_label(input_data,
                                    lset->labels[i], &mset_len,
                                    &mset)) {
                    if (lset->id[i]) {
                         for (j = 0; j < mset_len; j++ ) {
                              dprint("subtuple i %u j %u, %s", i, j,
                                     lset->labels[i]->name);
                              //search for sublabels
                              member_cnt +=
                                   add_searchlabels(proc, nest, mset[j],
                                                 newtupledata,
                                                 &nest->lset[lset->id[i]]); 
                         }
                    }
                    else {
                         for (j = 0; j < mset_len; j++ ) {
                              dprint("subtuple i %u j %u, %s", i, j,
                                     lset->labels[i]->name);
                              if (proc->reset_label) {
                                   tuple_member_add_ptr(newtupledata, mset[j],
                                                        lset->labels[i]);
                              }
                              else {
                                   add_tuple_member(newtupledata, mset[j]);
                              }
                              member_cnt++;
                         }
                    }
               }
               else if (proc->add_nullstr) {
                    member_cnt += add_nullstring(proc, newtupledata,
                                                 lset->labels[i]);
               }
          }
     }

     return member_cnt;
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

     if (proc->apply_container_labels) {
          wsdata_duplicate_labels(input_data, newtupledata);
     }
     int member_cnt;
     member_cnt = add_searchlabels(proc, &proc->nest,
                                   input_data,
                                   newtupledata,
                                   &proc->nest.lset[0]); 

    
     // this is new data.. pass as output
     if (member_cnt == 0) {
          //delete empty tuple
          wsdata_delete(newtupledata);
          return 0;
     }
     ws_set_outdata(newtupledata, proc->outtype_tuple, dout);
     proc->outcnt++;
     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
static int process_dupe(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     wsdata_t * newtupledata = ws_get_outdata(proc->outtype_tuple);

     if (!newtupledata) {
          return 0;
     }

     if (proc->apply_container_labels) {
          wsdata_duplicate_labels(input_data, newtupledata);
     }

     wsdt_tuple_t * tup = (wsdt_tuple_t *)input_data->data;
     int i;
     for (i = 0; i < tup->len; i++) {
          add_tuple_member(newtupledata, tup->member[i]);
     }

     ws_set_outdata(newtupledata, proc->outtype_tuple, dout);
     proc->outcnt++;
     return 1;
}

// this function is polled by a monitor kid to get periodic health and status 
static int proc_monitor(void *vinstance, wsdata_t *input_data, 
			ws_doutput_t *dout, int type_index) 
{
     proc_instance_t *proc = (proc_instance_t *)vinstance;

     //you need to get a tuple from the monitor to write your output
     wsdata_t *mtdata = wsdt_monitor_get_tuple(input_data);

     //allocate a subtuple for organizing a kid's specific output
     wsdata_t *tdata = tuple_member_create_wsdata(mtdata, dtype_tuple,
                                                  proc->label_subtuple);

     //ok, now print all the stats this proc has collected to our kid's tuple
     if (tdata) 
     {
          if (proc->meta_process_cnt) 
          {
		tuple_member_create_uint64(tdata, proc->meta_process_cnt,
						proc->label_process_cnt); 
	  }
     }

     //the following function must be called before exiting this callback;
     // signals that  kid has been visited and data has been appended
     wsdt_monitor_set_visit(input_data);
     return 0;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     wsdata_delete(proc->wsd_nullstr);

     //free dynamic allocations
     free(proc);

     return 1;
}

