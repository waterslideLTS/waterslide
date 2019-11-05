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
#define PROC_NAME "workreceive"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "shared/mwmr_queue.h"
#include "shared/kidshare.h"

#define LOCAL_MAX_TYPES (25)

char proc_version[]     = "1.1";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "wreceive", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "implements receiving end of work (work-based queue)";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharequeuename",
     "shared queue with other kids",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char *proc_input_types[]    = {"any", NULL};

//function prototypes for local functions
int proc_process(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_share_t {
     int cnt;
     char * prefix;
     mwmr_queue_t * sq;
} proc_share_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];

     proc_share_t * sharedata;
     char * sharelabel;
     void * v_type_table;

     int my_shared_id;
     ws_sourcev_t * mysv;
     uint8_t got_myshid;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc) {
     int op;

     while ((op = getopt(argc, argv, "J:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          default:
               return 0;
          }
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
     if (!proc_cmd_options(argc, argv, proc)) {
          return 0;
     }

     //other init
     proc->v_type_table = type_table;
     if(!proc->sharelabel) {
          error_print("must specify a sharelabel with -J option");
          return 0;
     }

     proc->mysv = (ws_sourcev_t *)calloc(1, sizeof(ws_sourcev_t));
     if (!proc->mysv) {
          error_print("failed calloc of proc->mysv");
          return 0;
     }
     memcpy(proc->mysv, sv, sizeof(ws_sourcev_t));

     return 1; 
}

// proc_init_finish is called after all proc_inits have been
// called by WS driver code
int proc_init_finish(void *vinstance)
{
     // the sharedata should have been created by a workbalance
     // kid; if not, we notify the user
     //
     proc_instance_t * proc = (proc_instance_t *)vinstance;
     proc->sharedata = ws_kidshare_get(proc->v_type_table, proc->sharelabel);

     if(!proc->sharedata) {
          error_print("no label of '%s' found for a workbalance kid",
                      proc->sharelabel);
          return 0;
     }

     proc->sharedata->cnt++;

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
          tool_print("does not process flusher type");
          return NULL;
     }
     //register output..
     // pass the input type to the output..
     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type, NULL);

     return proc_process; // a function pointer
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
int proc_process(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     if(!proc->got_myshid) {
          sscanf(proc->mysv->proc_instance->srclabel_name + strlen(proc->sharedata->prefix), 
                 "%d", &proc->my_shared_id);

          // this corrects for our inability to add 0 as (intptr_t) in our
          //shared queue, since we have an assert of data not being NULL
          proc->my_shared_id++; 

          proc->got_myshid = 1;
     }

     // what comes in, goes out
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     proc->outcnt++;

     // add self back to work-queue...keep blocking until successful in enqueuing id
     while(!mwmr_queue_add(proc->sharedata->sq, (void*)(intptr_t)proc->my_shared_id, NULL)) {}

     return 0;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free shared queue
     // use ws_kidshare_unshare to ensure the last arrival does this!
     // Note that last arrival could be one of the workreceive kids!
     int ret = ws_kidshare_unshare(proc->v_type_table, proc->sharelabel);
     if (!ret) {
          mwmr_queue_exit(proc->sharedata->sq);
          free(proc->sharedata);
     }

     //free dynamic allocations
     free(proc->mysv);
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     free(proc);

     return 1;
}

