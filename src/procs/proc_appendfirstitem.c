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

#define PROC_NAME "appendfirstitem"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "stringhash5.h"
#include "procloader.h"

char proc_name[]	= PROC_NAME;
char *proc_tags[]	= { "annotation", NULL };
char proc_purpose[]	= "Given a key A to index on and a label B to track, "
			  "stores the value B for the first occurrence of each "
			  "key A, and appends that value to each subsequent occurrence of key A.";
char *proc_synopsis[]	= { "appendfirstitem <LABEL.A> ... -V <LABEL.B> [-N <records>]", NULL };
char proc_description[]	= "Given the label of a key A to index on and a label B to track, stores the "
			  "value B for the first occurrence of each key A. Upon each "
			  "subsequent occurrence of the same key A, the following changes are made "
			  "to the tuple:\n"
			  "\t(1) The label 'NEXT' is added to label B.\n"
			  "\t(2) A new member is appended to the tuple with the label B, the label "
			  "'FIRST', and the value B that was stored on the first occurrence of key A.\n"
			  "Composite keys cannot be used for indexing; if multiple labels are given, "
			  "the last will be used for the index. Also, "
			  "only one label B is accepted; any additional labels occurring after label B are "
			  "considered when determining key A.\n"
			  "\n"
			  "A maximum buffer size can be suggested by the user (with the -N option) to determine the number of "
			  "unique keys that will be tracked. The appendfirstitem kid employs a stringhash5 "
			  "table, which requires a minimum buffer size of 64. Given a suggested maximum number "
			  "of records to track (x), the maximum buffer size is set to 2^(\\ceiling \\log2(x)). A "
			  "user-suggested buffer size less than 64 will result in a warning and an automatic "
			  "resize. As the buffer is filled, keys are expired in order of those that are approximately "
			  "least recently seen.\n"
			  "\n"
			  "Default behavior only passes records that are not the first occurrence of key A. The TAG "
			  "port is used to pass all records, but only append a tuple member to those that are not "
			  "the first occurrence of key A. The ADD port is used to store the key-value pair for the "
			  "first occurrence of key A, but no records are passed. The ADD and TAG ports can be used "
			  "in conjunction to combine buffers into the same tuple data structure (e.g., for use in "
			  "request/response decoding.)"
;
proc_example_t proc_examples[]	= {
				   {"... | appendfirstitem KEY -V VALUE | ...", 
				    "Store VALUE value for first occurrence of each KEY value. On each next occurrence of KEY value, add label 'NEXT' to VALUE and append a tuple member with the labels 'VALUE:FIRST' and the value of VALUE from the first record that contained same KEY value."},
				   {"... | appendfirstitem KEY -V VALUE -N 100 | ...", 
				    "Track the 128 most recently seen KEY values."},
				   {NULL,""}
				  };
char *proc_alias[]	= { "appendfirst", NULL };
char proc_version[]	= "1.5";
char proc_requires[]	= "";
char *proc_input_types[]	= {"flush", "tuple", NULL};
char *proc_output_types[]	= {"tuple", NULL};
proc_port_t proc_input_ports[]	= {
				   {"none","if key not found, store value and label FIRST, but do not pass; if key found, add NEXT label to value, append FIRST tuple member and pass"},
				   //{"DELETE","remove key from list of seen items"}, // not implemented
				   //{"REMOVE","remove key from list of seen items"}, // not implemented
				   {"TAG","if key not found, store value and label FIRST; if key found, add NEXT label to value, append FIRST tuple member; pass all"},
				   {"ADD","if key not found, add FIRST label to value and store; if key found, do nothing; pass nothing"},
				   {NULL, NULL}
				  };
char *proc_tuple_container_labels[]	= {NULL};
char *proc_tuple_conditional_container_labels[]	= {NULL};
char *proc_tuple_member_labels[]	= {"FIRST", "NEXT", NULL};
proc_option_t proc_opts[]	= {
				   /* 'option character', "long option string", "option argument",
				      "option description", <allow multiple>, <required> */
                                   {'J',"","sharename",
                                    "shared table with other kids",0,0},
				   {'V',"","LABEL.B",
				    "label of value to store if first occurrence of key A and append if subsequent occurrence of key A", 0, 1},
				   {'N',"","records",
				    "maximum number of unique keys to store",0,0},
				   //the following must be left as-is to signify the end of the array
				   {' ',"","",
				    "",0,0}
				  };
char proc_nonswitch_opts[]	= "LABEL.A of key to index on";

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_tuple_tag(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_tuple_add(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     stringhash5_t * last_table;
     uint32_t buflen;
     wslabel_set_t lset;
     ws_outtype_t * outtype_tuple;
     wslabel_t * label_first;
     wslabel_t * label_next;
     wslabel_t * label_value;
     wslabel_t * label_key;

     char * sharelabel;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:V:N:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'V':
               proc->label_value = wssearch_label(type_table, optarg);
               tool_print("using value %s", optarg);
               break;
          case 'N':
               proc->buflen = atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          proc->label_key = wssearch_label(type_table, argv[optind]);
          tool_print("using key %s", argv[optind]);
          optind++;
     }
     
     return 1;
}

static void last_destroy(void * vdata, void * vproc) {
     //proc_instance_t * proc = (proc_instance_t *)vproc;
     wsdata_t ** wsdp = (wsdata_t **)vdata;
     if (wsdp && (*wsdp)) {
          wsdata_t * wsd = *wsdp;
          wsdata_delete(wsd);
          *wsdp = NULL;
     }
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

     ws_default_statestore(&proc->buflen);

     proc->label_first = wsregister_label(type_table, "FIRST");
     proc->label_next = wsregister_label(type_table, "NEXT");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->label_key || !proc->label_value) {
          error_print("need to specify a key and value");
          return 0;
     }

     //other init - init the stringhash table
     if (proc->sharelabel) {
          stringhash5_sh_opts_t * sh5_sh_opts;
          int ret;

          //calloc shared sh5 option struct
          stringhash5_sh_opts_alloc(&sh5_sh_opts);

          //set shared sh5 option fields
          sh5_sh_opts->sh_callback = last_destroy;
          sh5_sh_opts->proc = proc; 

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->last_table, 
                                              proc->sharelabel, proc->buflen, 
                                              sizeof(wsdata_t *), NULL, sh5_sh_opts); 

          //free shared sh5 option struct
          stringhash5_sh_opts_free(sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          proc->last_table = stringhash5_create(0, proc->buflen, sizeof(wsdata_t *));
          if (!proc->last_table) {
               return 0;
          }
          stringhash5_set_callback(proc->last_table, last_destroy, proc);
     }

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->last_table->max_records;

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
          return proc_flush;
     }
     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          if (!proc->outtype_tuple) {
               proc->outtype_tuple = ws_add_outtype(olist, meta_type, NULL);
          }
          if (wslabel_match(type_table, port, "TAG")) {
               return proc_tuple_tag;
          }
          if (wslabel_match(type_table, port, "ADD")) {
               return proc_tuple_add;
          }

          return proc_tuple;

     }

     return NULL; // a function pointer
}

static inline int check_append(proc_instance_t * proc, wsdata_t * tdata,
                               wsdata_t * key, wsdata_t * value) {
     wsdata_t ** lastdata;
     lastdata = (wsdata_t **)
          stringhash5_find_attach_wsdata(proc->last_table, key);
     if (lastdata) {
          if (*lastdata) {
               //append
               dprint("adding tuple member - first");
               dprint("dtype %s", (*lastdata)->dtype->name);
               add_tuple_member(tdata, *lastdata);
               dprint("adding value label");
               if (value) {
                    tuple_add_member_label(tdata, value, proc->label_next);
               }
               stringhash5_unlock(proc->last_table);
               return 1;
          }
          else {
               if (value) {
                    *lastdata = value;
                    wsdata_add_reference(value);
                    wsdata_add_label(value, proc->label_first);
               }
               stringhash5_unlock(proc->last_table);
          }
     }
     return 0;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     wsdata_t * key = NULL;
     wsdata_t * value = NULL;

     dprint("proc_tuple");
     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          key = mset[0];
          dprint("found key");
     }
     if (!key) {
          return 0;
     }
     if (tuple_find_label(input_data, proc->label_value,
                          &mset_len, &mset)) {
          value = mset[0];
          dprint("found value");
     }

     dprint("found key");
     if (check_append(proc, input_data, key, value)) {
          dprint("setting out data");
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     dprint("proc_tuple done");
     //always return 1 since we don't know if table will flush old data
     return 1;
}

static int proc_tuple_tag(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     wsdata_t * key = NULL;
     wsdata_t * value = NULL;

     dprint("proc_tuple");
     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          key = mset[0];
          dprint("found key");
     }
     if (!key) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
          return 0;
     }
     if (tuple_find_label(input_data, proc->label_value,
                          &mset_len, &mset)) {
          value = mset[0];
          dprint("found value");
     }

     dprint("found key");
     if (check_append(proc, input_data, key, value)) {
          dprint("appended data");
     }
     dprint("setting out data");
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;

     dprint("proc_tuple done");
     //always return 1 since we don't know if table will flush old data
     return 1;
}

static int proc_tuple_add(void * vinstance, wsdata_t* input_data,
                          ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     wsdata_t * key = NULL;
     wsdata_t * value = NULL;

     dprint("proc_tuple");
     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          key = mset[0];
          dprint("found key");
     }
     if (!key) {
          return 0;
     }
     if (tuple_find_label(input_data, proc->label_value,
                          &mset_len, &mset)) {
          value = mset[0];
          dprint("found value");
     }

     dprint("found key");
     if (check_append(proc, input_data, key, value)) {
          dprint("appended data");
     }

     dprint("proc_tuple done");

     return 0;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     stringhash5_scour_and_flush(proc->last_table, last_destroy, proc);

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     stringhash5_destroy(proc->last_table);

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     free(proc);

     return 1;
}

