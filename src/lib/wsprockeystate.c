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

     uint32_t expire_generation;
     uint32_t loop_generation;
     int loop_started;
     int loop_target;
     stringhash5_walker_t * expire_walker;

     int multivalue;
     wslabel_nested_set_ext_t nest_mvalue;
     wslabel_t ** label_values;
     int mvalue_cnt;
     wslabel_nested_set_t nest_key;
     int core_len;
     wsdata_t * current_key;
     wsdata_t * current_tuple;
} wsprockeystate_inst_t;

//function prototypes for local functions
static int wsprockeystate_process_key(void *, wsdata_t*, ws_doutput_t*, int);
static int wsprockeystate_process_keyvalue(void *, wsdata_t*, ws_doutput_t*, int);
static int wsprockeystate_process_multivalue(void *, wsdata_t*, ws_doutput_t*, int);
static int wsprockeystate_delete_key(void *, wsdata_t*, ws_doutput_t*, int);
static int wsprockeystate_mvdelete_key(void *, wsdata_t*, ws_doutput_t*, int);
static int wsprockeystate_expire_port(void *, wsdata_t*, ws_doutput_t*, int);
static int wsprockeystate_process_flush(void *, wsdata_t*, ws_doutput_t*, int);


static void wsprockeystate_expire(void * vstate, void * vinstance) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;
     if (proc->multivalue) {
          if (proc->kid->expire_multi_func) {
               uint8_t * vptr = (uint8_t*)vstate + proc->core_len;
               proc->kid->expire_multi_func(proc->kproc, vstate, proc->dout,
                                            proc->outtype_tuple,
                                            proc->mvalue_cnt, (void *)vptr);
          }
     }
     else {
          proc->kid->expire_func(proc->kproc, vstate, proc->dout, proc->outtype_tuple);
     }
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
               if (proc->multivalue) {
                    wslabel_nested_search_build_ext(type_table,
                                                    &proc->nest_mvalue,
                                                    optarg, proc->mvalue_cnt);

                    if (proc->label_values) {
                         proc->label_values =
                              (wslabel_t **)realloc(proc->label_values,
                                                    (proc->mvalue_cnt + 1) * sizeof(wslabel_t*));
                    }
                    else {
                         proc->label_values =
                              (wslabel_t**)calloc(proc->mvalue_cnt + 1, sizeof(wslabel_t*));
                    }
                    if (!proc->label_values) {
                         error_print("unable to allocate memory for values");
                         return 0;
                    }
                    proc->label_values[proc->mvalue_cnt] = wssearch_label(type_table, optarg);
                    proc->mvalue_cnt++;
                    tool_print("%s using value with label %s", proc->kid->name, optarg);
                    
               }
               else {
                    proc->label_value = wssearch_label(type_table, optarg);
               }
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
          if (!proc->multivalue) {
               proc->label_key = wssearch_label(type_table, argv[optind]);
               tool_print("%s using key with label %s",
                          proc->kid->name, argv[optind]);
               return 1;
          }

          //handle nested variant key
          wslabel_nested_search_build(type_table, &proc->nest_key, argv[optind]);
          tool_print("%s using key with label %s", proc->kid->name, argv[optind]);
          optind++;
     }
     return 1;
}

static inline uint32_t * wsprockeystate_get_generation_ptr(wsprockeystate_inst_t * proc,
                                                           void * vdata) {
     if (!proc->kid->gradual_expire) {
          return NULL;
     }
     uint8_t * bdata = (uint8_t *)vdata;
     uint8_t * offset = bdata + proc->kid->state_len;

     return (uint32_t*)offset;
}

//if callback = 0, delete record
static int wsprockeystate_check_expiration(void * vstate, void * vproc) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vproc;
     dprint("checking expiration");
     
     uint32_t * pgeneration = wsprockeystate_get_generation_ptr(proc, vstate);
     if (!pgeneration) {
          return 0; //delete data
     }
     uint32_t generation = *pgeneration;

     dprint("checking expiration %u %u %u", generation, proc->expire_generation,
            proc->loop_generation);

     if ((generation != proc->expire_generation) &&
         (generation != proc->loop_generation)) {
          dprint("expiring with generation %u", generation);
          if (proc->kid->expire_func || proc->kid->expire_multi_func) {
               wsprockeystate_expire(vstate, vproc);
          }
          return 0; //delete data
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
     proc->multivalue = kid->multivalue;

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

     if (!proc->label_key && !proc->nest_key.cnt) {
          tool_print("%s no key specified with label", proc->kid->name);
          return 0;
     }

     if (!kid->state_len && !kid->value_size) {
          tool_print("%s no state specified", proc->kid->name);
          return 0;
     }

     if (!proc->multivalue &&
         kid->update_value_func && !kid->update_func && !proc->label_value) {
          tool_print("%s no value specified with label", proc->kid->name);
          return 0;
     }

     if (kid->init_func) {
          if (proc->multivalue) {
               if (!kid->init_func(proc->kproc, type_table, proc->mvalue_cnt)) {
                    return 0;
               }
          }
          if (!kid->init_func(proc->kproc, type_table, (proc->label_value) ? 1 : 0)) {
               return 0;
          }
     }
     if (kid->init_mvalue_func && proc->multivalue) {
          if (!kid->init_mvalue_func(proc->kproc, type_table, proc->mvalue_cnt,
                                     proc->label_values)) {
               return 0;
          }
     }

     if (!proc->buflen) {
          ws_default_statestore(&proc->buflen);
     }

     //other init - init the stringhash table
     int state_len = kid->state_len;
     if (kid->gradual_expire) {
          state_len += sizeof(uint32_t); //add extra state to track expiration

     }
     proc->core_len = state_len;

     if (proc->multivalue) {
          state_len += kid->value_size * proc->mvalue_cnt; 
     }

     //calloc shared sh5 option struct
     stringhash5_sh_opts_t * sh5_sh_opts;
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     if (proc->sharelabel) {
          if (kid->expire_func || kid->expire_multi_func) {

               //set shared sh5 option fields
               sh5_sh_opts->sh_callback = wsprockeystate_expire;
               sh5_sh_opts->proc = proc; 
               if (!stringhash5_create_shared_sht(type_table, (void **)&proc->state_table,
                                                  proc->sharelabel, proc->buflen,
                                                  state_len, NULL, sh5_sh_opts)) {
                    return 0;
               }
          }
          else {
               if (!stringhash5_create_shared_sht(type_table, (void **)&proc->state_table,
                                                  proc->sharelabel, proc->buflen,
                                                  state_len, NULL, sh5_sh_opts)) {
                    return 0;
               }
          }
     }
     else {
          proc->state_table = stringhash5_create(0, proc->buflen, state_len);
          if (!proc->state_table) {
               return 0;
          }
          if (kid->expire_func || kid->expire_multi_func) {
               stringhash5_set_callback(proc->state_table, wsprockeystate_expire, proc);
          }
     }

     if (kid->gradual_expire && proc->state_table) {
          proc->expire_walker =
               stringhash5_walker_init(proc->state_table,
                                       wsprockeystate_check_expiration, proc);
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

     if (wslabel_match(type_table, port, "EXPIRE")) {
          proc->expire_generation = 1;
          return wsprockeystate_expire_port;
     }
     if (wslabel_match(type_table, port, "REMOVE") ||
         wslabel_match(type_table, port, "DELETE")) {
          if (proc->multivalue) {
               return wsprockeystate_mvdelete_key; // a function pointer
          }
          else {
               return wsprockeystate_delete_key; // a function pointer
          }
     }

     if (proc->multivalue) {
          return wsprockeystate_process_multivalue; // a function pointer
     }
     if (proc->label_value) {
          return wsprockeystate_process_keyvalue; // a function pointer
     }
     else {
          return wsprockeystate_process_key; // a function pointer
     }
}


static inline void wspks_update_generation(wsprockeystate_inst_t * proc,
                                           void * vstate) {
     if (!proc->expire_generation || !vstate) {
          return;
     }

     uint32_t * pgeneration = wsprockeystate_get_generation_ptr(proc, vstate);
     if (pgeneration) {
          *pgeneration = proc->expire_generation;
     }
}

static inline void wspks_check_expire_loop(wsprockeystate_inst_t * proc) {
     if (proc->loop_started) {
          dprint("loop_started- check expire_loop");
          stringhash5_walker_next(proc->expire_walker);

          if (proc->expire_walker->loop == proc->loop_target) {
               dprint("loop ended");
               proc->loop_started = 0;
          }
     }
}

//only select first key found as key to use
static int wspks_nest_search_key(void * vproc, void * vkey,
                           wsdata_t * tdata, wsdata_t * member) {
     dprint("nest_search_key");
     //wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vproc;
     wsdata_t ** pkey = (wsdata_t **)vkey;
     wsdata_t * key = *pkey;
     if (!key) {
          *pkey = member;
          return 1;
     }
     return 0;
}

static int wspks_nest_search_value(void * vproc, void * vbase,
                                   wsdata_t * tdata, wsdata_t * member,
                                   wslabel_t * label, int offset) {

     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vproc;

     if (offset >= proc->mvalue_cnt) {
          return 0; //invalid offset
     }
     uint8_t * vptr = (uint8_t*)vbase + (offset * proc->kid->value_size);

     return proc->kid->update_value_func(proc->kproc, vptr, proc->current_tuple,
                                         proc->current_key, member);

}


static int wsprockeystate_process_multivalue(void * vinstance, wsdata_t* input_data,
                                             ws_doutput_t * dout, int type_index) {

     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;
     proc->rec++;

     proc->dout = dout;
     int rtn = 0; 

     wsdata_t * key = NULL;

     //return the first key found
     tuple_nested_search(input_data, &proc->nest_key,
                         wspks_nest_search_key,
                         proc, &key);

     if (!key) {
          return 0;
     }

     void * sdata = stringhash5_find_attach_wsdata(proc->state_table, key);
     if (!sdata) {
          return 0;
     }
     wspks_update_generation(proc, sdata);

     if (proc->kid->update_func) {
          rtn += proc->kid->update_func(proc->kproc, sdata, input_data, key);
     }

     //get values
     uint8_t * vptr = (uint8_t*)sdata + proc->core_len;
     if (proc->kid->update_value_func) {
          proc->current_key = key;
          proc->current_tuple = input_data;
          rtn += tuple_nested_search_ext(input_data, &proc->nest_mvalue,
                                         wspks_nest_search_value,
                                         proc, vptr);
     }
     if (proc->kid->post_update_mvalue_func) {
          rtn += proc->kid->post_update_mvalue_func(proc->kproc, sdata,
                                                    input_data, key,
                                                    proc->mvalue_cnt, vptr);
     }

     int do_expire = 0;
     if (proc->kid->force_expire_func) { 
          if (proc->kid->force_expire_func(proc->kproc, sdata,
                                           input_data, key)) {
               wsprockeystate_expire(sdata, proc);
               do_expire = 1;
          }
     }

     if (do_expire) {
          stringhash5_delete_wsdata(proc->state_table, key);
     }

     stringhash5_unlock(proc->state_table);

     //see if records need to be expired
     wspks_check_expire_loop(proc);

     if (rtn) { 
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
     }

     return 1;

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
               wspks_update_generation(proc, sdata);
               rtn += proc->kid->update_func(proc->kproc, sdata, input_data, mset[j]);
               if (proc->kid->force_expire_func) {
                    if (proc->kid->force_expire_func(proc->kproc, sdata,
                                                     input_data, mset[j])) {
                         if (proc->kid->expire_func) {
                              proc->kid->expire_func(proc->kproc, sdata, proc->dout, proc->outtype_tuple);
                         }
                         memset(sdata, 0, proc->kid->state_len);
                    }
               }
               if (found) {
                    stringhash5_unlock(proc->state_table);
               }
          }
     }
    
     wspks_check_expire_loop(proc);

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

     wspks_update_generation(proc, sdata);

     if (tuple_find_label(input_data, proc->label_value,
                          &mset_len, &mset)) {
          for (j = 0; j < mset_len; j++ ) {
               rtn += proc->kid->update_value_func(proc->kproc,
                                                   sdata,
                                                   input_data,
                                                   key, mset[j]);
               if (proc->kid->force_expire_func) {
                    if (proc->kid->force_expire_func(proc->kproc, sdata,
                                                     input_data, mset[j])) {
                         if (proc->kid->expire_func) {
                              proc->kid->expire_func(proc->kproc, sdata, proc->dout, proc->outtype_tuple);
                         }
                         memset(sdata, 0, proc->kid->state_len);
                    }
               }
          }
     }
     if (found) {
          stringhash5_unlock(proc->state_table);

     }
     wspks_check_expire_loop(proc);
    
     if (rtn) { 
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
     }

     return 1;
}

//triggered when any tuple is set ot the expire port
static int wsprockeystate_expire_port(void * vinstance, wsdata_t* input_data,
                                      ws_doutput_t * dout, int type_index) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;
     dprint("%s expire port called", proc->kid->name);
     if (!proc->kid->gradual_expire) {
          if (proc->kid->expire_func || proc->kid->expire_multi_func) {
               stringhash5_scour_and_flush(proc->state_table, wsprockeystate_expire, proc);
          }
          else {
               stringhash5_flush(proc->state_table);
          }
          return 1;
     }

     //Do gradulal flush
     if (proc->loop_started) {
          dprint("%s expire already started", proc->kid->name);
          return 1;
     }

     proc->dout = dout;
     proc->loop_started = 1;
     proc->loop_target = proc->expire_walker->loop + 1;
     proc->loop_generation = proc->expire_generation;
     wspks_check_expire_loop(proc);

     proc->expire_generation++;
     if (proc->expire_generation == 0) {
          proc->expire_generation = 1;
     }
     dprint("%s expire generation %d", proc->kid->name, proc->expire_generation);

     return 1;
}

static int wsprockeystate_process_flush(void * vinstance, wsdata_t* input_data,
                                      ws_doutput_t * dout, int type_index) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;
     proc->dout = dout;

     if (proc->kid->expire_func || proc->kid->expire_multi_func) {
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

static int wsprockeystate_mvdelete_key(void * vinstance, wsdata_t* input_data,
                                     ws_doutput_t * dout, int type_index) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;
     proc->dout = dout;

     wsdata_t * key;
     tuple_nested_search(input_data, &proc->nest_key,
                         wspks_nest_search_key,
                         proc, &key);

     if (!key) {
          return 0;
     }

     if (proc->kid->expire_multi_func) {
          void * sdata = NULL;
          sdata = stringhash5_find_wsdata(proc->state_table, key);
          if (sdata) {
               wsprockeystate_expire(sdata, proc);
               stringhash5_unlock(proc->state_table);
          }
     }

     stringhash5_delete_wsdata(proc->state_table, key);

     return 1;
}

static int wsprockeystate_delete_key(void * vinstance, wsdata_t* input_data,
                                     ws_doutput_t * dout, int type_index) {
     wsprockeystate_inst_t * proc = (wsprockeystate_inst_t*)vinstance;
     proc->dout = dout;

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

