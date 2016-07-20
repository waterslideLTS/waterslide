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

#define PROC_NAME "addlabelmember"
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

char proc_name[]	= PROC_NAME;
char *proc_tags[]      	= { "annotation", NULL };
char proc_purpose[]     = "Appends a new member to the tuple with a user-specified label and value.";
char *proc_synopsis[]   = { "addlabelmember <LABEL> [-E <ENV> | -V <value>]", NULL };
char proc_description[] = "Appends a new member to the tuple with a user-specified label and value. "
			  "Only one label can be added at a time. The label is assigned by the "
	                  "user; if no label is provided, the default is an empty string. "
	                  "The value of the new member is either (1) set to 'UNKNOWN'; (2) assigned by the user; or "
 	                  "(3) extracted from an environment variable. New tuple members are "
	                  "always added at the end of the tuple.";
proc_example_t proc_examples[]	= {
     {"... | addlabelmember NEWLABEL | ...", "append a tuple member with the label 'NEWLABEL' and the value 'UNKNOWN'"},
     {"... | addlabelmember WORD -E VERB | ...","append a tuple member with the label 'WORD' and the value of the 'VERB' environment variable"},
     {"... | addlabelmember SEEN -V 5 | ...","append a tuple member with the label 'SEEN' and the value '5'"},
     {"... | addlabelmember 'LOOKS BAD?' -V 'if you squint' | ...","append a tuple member with the label 'LOOKS BAD?' and the value 'if you squint'"},
     {NULL,""}
};
char *proc_alias[]	= { "alm", "addlabelm", NULL};
char proc_version[]     = "1.5";
char proc_requires[] 	= "";
char *proc_input_types[]	= {"tuple", NULL};
char *proc_output_types[]   	= {"tuple", NULL};
proc_port_t proc_input_ports[]	= {{NULL, NULL}};
char *proc_tuple_container_labels[]	= {NULL};
char *proc_tuple_conditional_container_labels[]	= {NULL};
char *proc_tuple_member_labels[]	= {NULL};

proc_option_t proc_opts[]	= {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'E',"","ENV",
     "set value of new tuple member to that of ENV (use 'env' to list available environment variables)",0,0},
     {'V',"","value",
     "set value of new tuple member to <value> specified by user",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]	= "LABEL of new tuple member";


//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_t * label_out;
     wsdata_t * wsd_newdata;
     char * new_value;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     int got_my_one_value=0;
     char * my_one_value=NULL;
     while ((op = getopt(argc, argv, "E:V:")) != EOF) {
          switch (op) {
          case 'E':
               if (got_my_one_value) {
                    error_print("only one value can be provided\n");
                    return 0;
               }
               my_one_value = getenv(optarg);
               got_my_one_value=1;
               break;
          case 'V':
               if (got_my_one_value) {
                    error_print("only one value can be provided\n");
                    return 0;
               }
               my_one_value=optarg;
               got_my_one_value=1;
               tool_print("got value %s ", my_one_value);
               break;
          default:
               return 0;
          }
     }
     int got_one=0;
     while (optind < argc) {
          if (got_one++ == 1) {
               error_print("only one new member at a time allowed");
               return 0;
          }
          proc->label_out = wsregister_label(type_table, argv[optind]);
          tool_print("registered new LABEL : %s", argv[optind]);
          optind++;
     }
     proc->new_value = my_one_value;

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
          proc->new_value = "UNKNOWN";
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
// return 1 if ok
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

static inline int add_newdata(proc_instance_t * proc, wsdata_t * tdata,
                                 wslabel_t * label) {
     dprint("adding tuple member data ptr");
     wsdata_t * wsd = tuple_member_add_ptr(tdata, proc->wsd_newdata, label);
     return (wsd != NULL);
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     wsdata_t * newtupledata = input_data;

     if (!newtupledata) {
          return 0;
     }

     add_newdata(proc, newtupledata,proc->label_out);
     ws_set_outdata(newtupledata, proc->outtype_tuple, dout);
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

