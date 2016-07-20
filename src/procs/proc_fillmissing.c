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
#define PROC_NAME "fillmissing"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "waterslide_io.h"
#include "procloader.h"
#include "wstypes.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_fixedstring.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"Annotation", "Stream manipulation", NULL};
char *proc_alias[]             =  {NULL};
char proc_purpose[]            =  "Appends a new member with user-specified value if existing label does not exist";
char proc_description[] = "Checks each tuple for the specified labels and, if missing, adds these labels using the specified value (-V option) or replicates the value from the specified buffer (-R option).  If the value specified is not a string value, the '-S' option can be used to cast the new value to a string.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'V',"","value",
     "new tuple member value - a string",0,0},
     {'R',"","member",
     "tuple member to replicate",0,0},
     {'S',"","",
     "replicate as string",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]     =  "LABEL to check if it exists";
char *proc_input_types[]       =  {"tuple", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  {"fillmissing <LABEL> [-V <value>] [-R <label>] [-S]", NULL};
proc_example_t proc_examples[] =  {
	{"... | fillmissing WORD -R FIRSTWORD -S | ...", "If the WORD is missing from a tuple, then add a WORD label with the value found in the FIRSTWORD buffer (converted to a string value)."},
	{"... | fillmissing -V 0 WORD SPACE VALUE | ...", "If the WORD, SPACE and/or VALUE labels are missing, then add these labels to the tuple and give them a value of 0."},
	{NULL, NULL}
};


//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wsdata_t * wsd_newdata;
     char * new_value;
     wslabel_set_t lset;
     wslabel_t * label_replicate;
     int as_string;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "SR:V:")) != EOF) {
          switch (op) {
          case 'S':
               proc->as_string = 1;
               break;
          case 'R':
               proc->label_replicate = wssearch_label(type_table, optarg);
               break;
          case 'V':
               proc->new_value=optarg;
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset, argv[optind]);
          wsregister_label(type_table, argv[optind]);
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
     if (!proc->new_value) {
          proc->new_value = "NULL";
     }
     char * nstr = proc->new_value;
     wsdata_t * wsnewdata = dtype_detect_strtype(nstr, strlen(nstr));
     if (!wsnewdata) {
          wsnewdata = wsdata_create_string(nstr, strlen(nstr));
     }

     if (wsnewdata) {
          proc->wsd_newdata = wsnewdata;
          wsdata_add_reference(wsnewdata);
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

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     wsdata_t ** mset;
     int mset_len;

     int found;
     int i;
     for (i = 0; i < proc->lset.len; i++) {
          found = 0;
          if (tuple_find_label(input_data, proc->lset.labels[i],
                               &mset_len, &mset)) {
               found = 1;
          }
          if (!found) {
               if (proc->label_replicate) {
                    wsdata_t ** submset;
                    int submset_len;
                    if (tuple_find_label(input_data, proc->label_replicate,
                                         &submset_len, &submset) && submset_len) {
                         if (proc->as_string) {
                              char * buf = NULL;
                              int buflen = 0;
                              if (dtype_string_buffer(submset[0], &buf,
                                                      &buflen)) {
                                   wsdt_string_t * str =
                                        tuple_member_create_wdep(input_data,
                                                                 dtype_string,
                                                                 proc->lset.labels[i],
                                                                 submset[0]);
                                   if (str) {
                                        str->buf = buf;
                                        str->len = buflen;
                                   }
                              }
                         }
                         else {
                              tuple_member_add_ptr(input_data, submset[0], proc->lset.labels[i]);
                         }
                    }

               }
               else {
                    tuple_member_add_ptr(input_data, proc->wsd_newdata, proc->lset.labels[i]);
               }
          }
     }

     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt  %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     wsdata_delete(proc->wsd_newdata);

     //free dynamic allocations
     free(proc);

     return 1;
}

