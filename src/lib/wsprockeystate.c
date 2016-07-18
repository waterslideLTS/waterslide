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

//NOTE: both serial and parallel versions of kids are built from this source file
//#define DEBUG 1
//#define PROC_NAME ""
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <netinet/in.h>
#include <time.h>
#include <assert.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "stringhash5.h"
#include "wsprockeystate.h"
#include "datatypes/wsdt_tuple.h"
#include "wstypes.h"

#define LOCAL_OPTIONS "J:V:M:"

typedef struct _wsprockeystate_inst_t {
     stringhash5_t * state_table;
     ws_outtype_t * outtype_tuple;
     wslabel_t * label_key;
     wslabel_t * label_value;
     void * kproc;
     char * option_str;
     wsprockeystate_kid_t * kid;
     uint32_t buflen;
     ws_doutput_t * dout;
     uint64_t rec;

     char * sharelabel;
} wsprockeystate_inst_t;

//function prototypes for local functions
static int wsprockeystate_process_key(void *, wsdata_t*, ws_doutput_t*, int);
static int wsprockeystate_process_keyvalue(void *, wsdata_t*, ws_doutput_t*, int);
static int wsprockeystate_delete_key(void *, wsdata_t*, ws_doutput_t*, int);
static int wsprockeystate_process_flush(void *, wsdata_t*, ws_doutput_t*, int);


static void wsprockeystate_expire(void * vstate, void * vinstance) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;
     proc->kid->expire_func(proc->kproc, vstate, proc->dout, proc->outtype_tuple);
}

static int wsprockeystate_cmd_options(int argc, char * const * argv, 
                                    wsprockeystate_inst_t * proc,
                                    void * type_table) {
     int op;

     while ((op = getopt(argc, argv, proc->option_str)) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'V':
               proc->label_value = wssearch_label(type_table, optarg);
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          default:
               if (proc->kid->option_func) {
                    if (!proc->kid->option_func(proc->kproc, type_table, op, optarg)) {
                         return 0;
                    }
               }
          }
     }
     while (optind < argc) {
          proc->label_key = wssearch_label(type_table, argv[optind]);
          tool_print("%s using key with label %s",
                     proc->kid->name, argv[optind]);
          return 1;
     }
     return 1;
}

// the following is a function to take in command arguments and initalize
// this processor's instance..
//  also register as a source here..
// return 1 if ok
// return 0 if fail
int wsprockeystate_init(int argc, char * const * argv, void ** vinstance,
                      void * type_table,
                      wsprockeystate_kid_t * kid) {
    
     //allocate proc instance of this processor
     wsprockeystate_inst_t * proc = 
          (wsprockeystate_inst_t*)calloc(1, sizeof(wsprockeystate_inst_t));
     if (!proc) {
          error_print("wsprockeystate_init failed in calloc of proc");
          return 0;
     }

     if (kid && kid->option_str) {
          int olen = strlen(kid->option_str);
          int len = olen + strlen(LOCAL_OPTIONS) + 1;
          proc->option_str = calloc(1, len);
          if (!proc->option_str) {
               error_print("wsprockeystate_init failed in calloc of proc->option_str");
               return 0;
          }
          memcpy(proc->option_str, kid->option_str, olen); 
          memcpy(proc->option_str+olen, LOCAL_OPTIONS, strlen(LOCAL_OPTIONS)); 
     }
     else {
          proc->option_str = LOCAL_OPTIONS;
     }

     *vinstance = proc;

     proc->kid = kid;
     if (kid->instance_len) {
          proc->kproc = (void *) calloc(1, kid->instance_len);
          if (!proc->kproc) {
               error_print("wsprockeystate_init failed in calloc of proc->kproc");
               return 0;
          }
     }
     
     if (kid->labeloffset) {
          wsinit_labeloffset(kid->labeloffset, proc->kproc, type_table);
     }

     //read in command options
     if (!wsprockeystate_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->label_key) {
          tool_print("%s no key specified with label", proc->kid->name);
          return 0;
     }

     if (!kid->state_len) {
          tool_print("%s no state specified", proc->kid->name);
          return 0;
     }

     if (kid->update_value_func && !kid->update_func && !proc->label_value) {
          tool_print("%s no value specified with label", proc->kid->name);
          return 0;
     }

     if (kid->init_func) {
         if (!kid->init_func(proc->kproc, type_table, proc->label_value ? 1 : 0)) {
              return 0;
         }
     }

     if (!proc->buflen) {
          ws_default_statestore(&proc->buflen);
     }

     //other init - init the stringhash table

     //calloc shared sh5 option struct
     stringhash5_sh_opts_t * sh5_sh_opts;
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     if (proc->sharelabel) {
          if (kid->expire_func) {

               //set shared sh5 option fields
               sh5_sh_opts->sh_callback = wsprockeystate_expire;
               sh5_sh_opts->proc = proc; 
               if (!stringhash5_create_shared_sht(type_table, (void **)&proc->state_table,
                                                  proc->sharelabel, proc->buflen,
                                                  kid->state_len, NULL, sh5_sh_opts)) {
                    return 0;
               }
          }
          else {
               if (!stringhash5_create_shared_sht(type_table, (void **)&proc->state_table,
                                                  proc->sharelabel, proc->buflen,
                                                  kid->state_len, NULL, sh5_sh_opts)) {
                    return 0;
               }
          }
     }
     else {
          proc->state_table = stringhash5_create(0, proc->buflen, kid->state_len);
          if (kid->expire_func) {
               stringhash5_set_callback(proc->state_table, wsprockeystate_expire, proc);
          }
     }

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->state_table->max_records;

     return 1; 
}


// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
//return 1 if ok
// return 0 if problem
proc_process_t wsprockeystate_input_set(void * vinstance, wsdatatype_t * input_type,
                                      wslabel_t * port,
                                      ws_outlist_t* olist, int type_index,
                                      void * type_table) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;

     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
     }

     if (input_type == dtype_flush) {
          return wsprockeystate_process_flush;
     }

     if (input_type != dtype_tuple) {
          return NULL;  // not matching expected type
     }

     if (wslabel_match(type_table, port, "REMOVE") ||
         wslabel_match(type_table, port, "DELETE")) {
          return wsprockeystate_delete_key; // a function pointer
     }

     if (proc->label_value) {
          return wsprockeystate_process_keyvalue; // a function pointer
     }
     else {
          return wsprockeystate_process_key; // a function pointer
     }
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 2 if not output

// Commented out code represents in-place copy, but uses more memory
// The current version deflates to a persistent buffer and copies out (more computation)

static int wsprockeystate_process_key(void * vinstance, wsdata_t* input_data,
                                      ws_doutput_t * dout, int type_index) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;
     proc->rec++;

     proc->dout = dout;
     //proc->meta_process_cnt++;
    
     int rtn = 0; 
     //search for items in tuples
     wsdata_t ** mset;
     int mset_len;
     int j;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          for (j = 0; j < mset_len; j++ ) {
               int found = 0;
               void * sdata = stringhash5_find_attach_wsdata(proc->state_table, mset[j]);
               if (sdata) {
                    found = 1;
               }
               rtn += proc->kid->update_func(proc->kproc, sdata, input_data, mset[j]);
               if (proc->kid->force_expire_func && proc->kid->expire_func) {
                    if (proc->kid->force_expire_func(proc->kproc, sdata,
                                                     input_data, mset[j])) {
                         proc->kid->expire_func(proc->kproc, sdata, proc->dout, proc->outtype_tuple);
                         memset(sdata, 0, proc->kid->state_len);
                    }
               }
               if (found) {
                    stringhash5_unlock(proc->state_table);
               }
          }
     }
    
     if (rtn) { 
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
     }

     return 1;
}

static int wsprockeystate_process_keyvalue(void * vinstance, wsdata_t* input_data,
                                      ws_doutput_t * dout, int type_index) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;
     proc->rec++;

     proc->dout = dout;
     //proc->meta_process_cnt++;

     wsdata_t * key = NULL;
    
     int rtn = 0; 
     //search for items in tuples
     wsdata_t ** mset;
     int mset_len;
     int j;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          key = mset[0];
     }

     if (!key) {
          return 0;
     }

     int found = 0;
     void * sdata = stringhash5_find_attach_wsdata(proc->state_table, key);
     if (sdata) {
          found = 1;
     }

     if (tuple_find_label(input_data, proc->label_value,
                          &mset_len, &mset)) {
          for (j = 0; j < mset_len; j++ ) {
               rtn += proc->kid->update_value_func(proc->kproc,
                                                   sdata,
                                                   input_data,
                                                   key, mset[j]);
               if (proc->kid->force_expire_func && proc->kid->expire_func) {
                    if (proc->kid->force_expire_func(proc->kproc, sdata,
                                                     input_data, mset[j])) {
                         proc->kid->expire_func(proc->kproc, sdata, proc->dout, proc->outtype_tuple);
                         memset(sdata, 0, proc->kid->state_len);
                    }
               }
          }
     }
     if (found) {
          stringhash5_unlock(proc->state_table);
     }
    
     if (rtn) { 
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
     }

     return 1;
}

static int wsprockeystate_process_flush(void * vinstance, wsdata_t* input_data,
                                      ws_doutput_t * dout, int type_index) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;
     proc->dout = dout;

     if (proc->kid->expire_func) {
          stringhash5_scour_and_flush(proc->state_table, wsprockeystate_expire, proc);
     }
     else {
          stringhash5_flush(proc->state_table);
     }

     if (proc->kid->flush_func) {
          proc->kid->flush_func(proc->kproc);
     }

     return 1;
}

static int wsprockeystate_delete_key(void * vinstance, wsdata_t* input_data,
                                     ws_doutput_t * dout, int type_index) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;

     //search for items in tuples
     wsdata_t ** mset;
     int mset_len;
     int j;
     if (tuple_find_label(input_data, proc->label_key,
                          &mset_len, &mset)) {
          for (j = 0; j < mset_len; j++ ) {
               int found = 0;
               void * sdata = NULL;
               if (proc->kid->expire_func) {
                    sdata = stringhash5_find_wsdata(proc->state_table, mset[j]);
                    if (sdata) {
                         found = 1;
                    }
                    if (sdata) {
                         proc->kid->expire_func(proc->kproc, sdata, dout,
                                                proc->outtype_tuple);
                    }
               }
               stringhash5_delete_wsdata(proc->state_table, mset[j]);
               if (found) {
                    stringhash5_unlock(proc->state_table);
               }
          }
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int wsprockeystate_destroy(void * vinstance) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;
     tool_print("%s processed %" PRIu64 " tuples", proc->kid->name, proc->rec);

     //destroy table
     stringhash5_destroy(proc->state_table);

     //free dynamic allocations
     if (proc->kid->destroy_func) {
          proc->kid->destroy_func(proc->kproc);
     }
     if (proc->option_str) {
          free(proc->option_str);
     }
     if (proc->kproc) {
          free(proc->kproc);
     }
     free(proc->sharelabel);
     free(proc);

     return 1;
}

