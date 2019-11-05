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

/* this generates container labels based on a work
 * event queue subscribed to by work receivers (wreceive)
   */
#define PROC_NAME "workbalance"
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "procloader.h"
#include "shared/mwmr_queue.h"
#include "shared/kidshare.h"

char proc_version[]     = "1.1";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "wbalance", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "labels data for work balancing";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharequeuename",
     "shared queue with other kids",0,0},
     {'N',"","count",
     "number of output channels",0,0},
     {'L',"","prefix",
     "Label prefix",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of tuple key";
char *proc_input_types[]    = {"tuple", "any", NULL};
char *proc_output_types[]    = {"any", NULL};

#define LOCAL_MAX_TYPES (25)

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
     wslabel_t ** lb_labels;
     char * prefix;
     int channels;

     proc_share_t * sharedata;
     char * sharelabel;
     void * v_type_table;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:N:L:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'N':
               proc->channels = atoi(optarg);
               break;
          case 'L':
               free(proc->prefix);
               proc->prefix = strdup(optarg);
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

     proc->prefix = strdup("LB");
     proc->channels = 4;


     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     //register label buffer
     proc->lb_labels = (wslabel_t**)calloc(proc->channels, sizeof(wslabel_t *));
     if (!proc->lb_labels) {
          error_print("failed calloc of proc->lb_labels");
          return 0;
     }
     int i;
     char lbuf[256];
     for (i = 0; i < proc->channels; i++) {
          snprintf(lbuf, 256, "%s%d", proc->prefix, i);
          proc->lb_labels[i] = wsregister_label(type_table, lbuf);
     }

     //other init
     if (!proc->sharelabel) {
          tool_print("this kid is not sharing queue, please specify share with -J option");
          return 0;
     }
     //see if structure is already available at label
     proc->sharedata = ws_kidshare_get(type_table, proc->sharelabel);
     proc->v_type_table = type_table;

     //no sharing at label yet..
     if (proc->sharedata) {
          status_print("more than one %s kid with the same label '%s' detected",
                      PROC_NAME, proc->sharelabel);
          // ... and we now have a reference to the shared multiple writer, multiple reader queue
     }
     else {
          tool_print("this kid is shared at label %s", proc->sharelabel);

          proc->sharedata = (proc_share_t *)calloc(1,sizeof(proc_share_t));
          if (!proc->sharedata) {
               error_print("failed calloc of proc->sharedata");
               return 0;
          }

          // we need to have at least as many entries in our event queue as
          // there are channels; otherwise, we will deadlock on the kick start
          // below (i.e., the  mwmr_queue_adds)
          proc->sharedata->sq = sized_mwmr_queue_init(proc->channels * 2); 
          if (!proc->sharedata->sq) {
               tool_print("unable to initialize sharedq");
               return 0;
          }

          // Kick start event queue
          for (i = 0; i < proc->channels; i++) {
               // enqueue the workers in round-robin fashion (initially)
               // the (i+1) below is intentional (don't use (i))
               while(!mwmr_queue_add(proc->sharedata->sq, (void*)(intptr_t)(i+1), NULL)) {}
          }

          proc->sharedata->prefix = proc->prefix;
          proc->sharedata->cnt++;

          //actually share structure
          ws_kidshare_put(type_table, proc->sharelabel, proc->sharedata);
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
          tool_print("does not process flusher type");
          return NULL;
     }
     //register output..
     // pass the input type to the output..
     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type, NULL);

     // we are happy.. now set the processor function
     return proc_process; // a function pointer
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
int proc_process(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     void *dummy;
#if __GNUC__
  #if __x86_64__ || __ppc64__
     int64_t id;
  #else // 32-bit architecture (pointers are 32-bit wide)
     int32_t id;
  #endif
#else
  #warning "non-GNU compiler detected; assuming a 64-bit architecture"
     int64_t id;
#endif // __GNUC__
     mwmr_queue_remove(proc->sharedata->sq, (void**)(intptr_t*)&id, (void**)(intptr_t*)&dummy);
     id--; // corrects for our inability to add 0 to queue in mwmr_queue_add
//fprintf(stderr, "WB: proc->id_found = %d\n", id);

     wsdata_add_label(input_data, proc->lb_labels[id]);

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

     //free shared queue
     // use ws_kidshare_unshare to ensure the last arrival does this!
     // Note that last arrival could be one of the workreceive kids!
     int ret = ws_kidshare_unshare(proc->v_type_table, proc->sharelabel);
     if (!ret) {
          mwmr_queue_exit(proc->sharedata->sq);
          free(proc->sharedata);
     }

     //free dynamic allocations
     free(proc->prefix);
     free(proc->lb_labels);
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     free(proc);

     return 1;
}

