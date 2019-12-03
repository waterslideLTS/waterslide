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
#define PROC_NAME "mknest"
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

char proc_version[]     = "1.1";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "renest", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "place selected elements into a new subtuple";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'A',"","tuple LABEL",
     "append items (except tuples) into existing labeled tuple",0,0},
     {'L',"","LABEL",
     "apply label to denested elements",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "list of elements to add into subtuple";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};

//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     wslabel_nested_set_t appendset;
     int append;
     ws_outtype_t * outtype_tuple;
     wslabel_nested_set_t nest;
     wslabel_t * label_apply;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "a:A:L:")) != EOF) {
          switch (op) {
          case 'a':
          case 'A':
               proc->append = 1;
               wslabel_nested_search_build(type_table, &proc->appendset, optarg);
               tool_print("using %s tuple for appending", optarg);
               break;
          case 'L':
               proc->label_apply = wsregister_label(type_table, optarg);
               tool_print("creating new tuple %s", optarg);
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

     proc->label_apply = wsregister_label(type_table, "NEST");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
   
     if (proc->nest.cnt == 0) {
          tool_print("must specify tuple members to nest");
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

static int proc_nest_find_append(void * vproc, void * vnewtup,
                                 wsdata_t * tdata, wsdata_t * member) {

     //proc_instance_t * proc = (proc_instance_t *)vproc;
     wsdata_t ** newtup = (wsdata_t **)vnewtup;
     dprint("proc_nest_find_append");

     if (member->dtype != dtype_tuple) {
          dprint("member not a tuple");
          dprint("member is a %s type", member->dtype->name);
          return 0;
     }

     dprint("member is a tuple");
     *newtup = member;

     return 1;
}


static int proc_nest_tuple(void * vproc, void * vnewtup,
                         wsdata_t * tdata, wsdata_t * member) {

     proc_instance_t * proc = (proc_instance_t *)vproc;
     wsdata_t * newtup = (wsdata_t *)vnewtup;

     dprint("proc_nest_tuple");
     if (proc->append) {
          //tuples cannot be appended - to avoid nested dependency recusion
          if (member->dtype == dtype_tuple) {
               dprint("member is a tuple - cannot append");
               return 0;
          }
          dprint("member not a tuple - can append");
     }
     add_tuple_member(newtup, member);

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     dprint("--process_tuple--------");

     proc->meta_process_cnt++;

     wsdata_t * newtupledata = NULL;

     if (proc->append) {
          dprint("doing append");
          tuple_nested_search(input_data, &proc->appendset,
                              proc_nest_find_append,
                              proc, &newtupledata);
          dprint("done searching for tuple to append");
          if (!newtupledata) {
               dprint("appending tuple not found");
               ws_set_outdata(input_data, proc->outtype_tuple, dout);
               proc->outcnt++;
               return 0;
          }
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

     //find out how many elements in new tuple
     if (!proc->append) {
          wsdt_tuple_t * tup = (wsdt_tuple_t *)newtupledata->data;
          if (!tup->len) {
               wsdata_delete(newtupledata);
          }
          else {
               wsdata_add_label(newtupledata, proc->label_apply);
               add_tuple_member(input_data, newtupledata);
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
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free dynamic allocations
     free(proc);

     return 1;
}

