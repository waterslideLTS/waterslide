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
#define PROC_NAME "labelmultiple"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "datatypes/wsdt_tuple.h"

char proc_version[]     = "1.1";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { NULL};
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "adds label to data with multiple entries";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'L',"","LABEL",
     "prefix of label to add to members",0,0},
     {'p',"","",
     "create a pointer to member with just new label",0,0},
     {'M',"","cnt",
     "maximum index to apply",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABEL of element to look for";
char *proc_input_types[]    = {"any", NULL};
char *proc_output_types[]    = {"any", NULL};

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_nested_set_t nest;
     wslabel_t ** label_set;
     char * prefix;
     int maxindex;
     int relabel;
     int lcnt;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "M:pL:")) != EOF) {
          switch (op) {
          case 'L':
               free(proc->prefix);
               proc->prefix = strdup(optarg);
               break;
          case 'p':
               proc->relabel = 1;
               break;
          case 'M':
               proc->maxindex = atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          if (wslabel_nested_search_build(type_table, &proc->nest,
                                          argv[optind])) {
               tool_print("searching for label %s", argv[optind]);
          }
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

     proc->maxindex = 256;
     proc->prefix = strdup("MULTI_");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
    
     if (!proc->nest.cnt) {
          error_print("need to specify label");
          return 0;
     } 
     if (!proc->maxindex) {
          error_print("need to have an index count");
          return 0;
     }
     proc->label_set = (wslabel_t **)calloc(proc->maxindex,
                                            sizeof(wslabel_t *));
     if (!proc->label_set) {
          error_print("failed calloc of proc->label_set");
          return 0;
     }
     char buf[100];
     int i;
     for (i = 0; i < proc->maxindex; i++) {
          snprintf(buf, 100, "%s%d", proc->prefix, i);
          proc->label_set[i] = wsregister_label(type_table, buf);
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
               proc->outtype_tuple = ws_add_outtype(olist, meta_type, NULL);
          }
          return proc_tuple;
     }

     return NULL;
}

//callback when searching nested tuple
static int proc_nest_match_callback(void * vproc, void * vtdata,
                                    wsdata_t * subtdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     if (proc->lcnt >= proc->maxindex) {
          return 0;
     }

     if (proc->relabel) {
          tuple_member_add_ptr(subtdata, member,
                               proc->label_set[proc->lcnt]);
     }
     else {
          tuple_add_member_label(subtdata, member,
                                 proc->label_set[proc->lcnt]);
     }
     proc->lcnt++;

     return 1;
}

static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     proc->lcnt = 0;

     //search for specified tuple member
     tuple_nested_search(input_data, &proc->nest,
                         proc_nest_match_callback,
                         proc, input_data);


     // this is new data.. pass as output
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
     free(proc->prefix);
     free(proc->label_set);
     free(proc);

     return 1;
}

