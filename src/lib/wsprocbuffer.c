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
#include "wsprocbuffer.h"
#include "datatypes/wsdt_tuple.h"
#include "wstypes.h"

typedef struct _wsprocbuffer_inst_t {
     ws_outtype_t * outtype_tuple;
     wslabel_nested_set_t nest;
     void * kproc;
     wsprocbuffer_kid_t * kid;
     uint64_t recv;
     uint64_t pass;
     wssubelement_t * sube_date;
     wssubelement_t * sube_srcip;
     wssubelement_t * sube_srcport;
     wssubelement_t * sube_dstip;
     wssubelement_t * sube_dstport;
     wssubelement_t * sube_nflowhash;
     wslabel_t * label_serverport;
     wslabel_t * label_serverip;
     wslabel_t * label_clientport;
     wslabel_t * label_clientip;
} wsprocbuffer_inst_t;

//function prototypes for local functions
static int wsprocbuffer_process_label(void *, wsdata_t*, ws_doutput_t*, int);
static int wsprocbuffer_process_unlabeled(void *, wsdata_t*, ws_doutput_t*, int);
static int wsprocbuffer_process_el_label(void *, wsdata_t*, ws_doutput_t*, int);
static int wsprocbuffer_process_el_unlabeled(void *, wsdata_t*, ws_doutput_t*, int);


static int wsprocbuffer_cmd_options(int argc, char * const * argv, 
                                    wsprocbuffer_inst_t * proc,
                                    void * type_table) {
     int op;

     while ((op = getopt(argc, argv, proc->kid->option_str)) != EOF) {
          switch (op) {
          default:
               if (proc->kid->option_func) {
                    if (!proc->kid->option_func(proc->kproc, type_table, op, optarg)) {
                         return 0;
                    }
               }
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build(type_table, &proc->nest, argv[optind]);
          tool_print("%s searching for string with label %s",
                     proc->kid->name, argv[optind]);
          optind++;
     }
     return 1;
}

// the following is a function to take in command arguments and initalize
// this processor's instance..
//  also register as a source here..
// return 1 if ok
// return 0 if fail
int wsprocbuffer_init(int argc, char * const * argv, void ** vinstance,
                      void * type_table,
                      wsprocbuffer_kid_t * kid) {
    
     //allocate proc instance of this processor
     wsprocbuffer_inst_t * proc = 
          (wsprocbuffer_inst_t*)calloc(1, sizeof(wsprocbuffer_inst_t));
     if (!proc) {
          error_print("wsprocbuffer_init failed in calloc of proc");
          return 0;
     }

     *vinstance = proc;

     proc->kid = kid;
     if (kid->instance_len) {
          proc->kproc = (void *)calloc(1, kid->instance_len);
          if (!proc->kproc) {
               error_print("wsprocbuffer_init failed in calloc of proc->kproc");
               return 0;
          }
     }
     
     if (kid->labeloffset) {
          wsinit_labeloffset(kid->labeloffset, proc->kproc, type_table);
     }

     //read in command options
     if (!wsprocbuffer_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (kid->init_func) {
         return kid->init_func(proc->kproc, type_table); 
     }

     return 1; 
}


// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
//return 1 if ok
// return 0 if problem
proc_process_t wsprocbuffer_input_set(void * vinstance, wsdatatype_t * input_type,
                                      wslabel_t * port,
                                      ws_outlist_t* olist, int type_index,
                                      void * type_table) {
     wsprocbuffer_inst_t * proc = (wsprocbuffer_inst_t*)vinstance;

     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
     }

     if (input_type == dtype_tuple) {
          if (proc->kid->element_func) {
               if (proc->nest.cnt) {
                    return wsprocbuffer_process_el_label; // a function pointer
               }
               else {
                    return wsprocbuffer_process_el_unlabeled; // a function pointer
               }
          }
          else if (proc->kid->decode_func) {

               if (proc->nest.cnt) {
                    return wsprocbuffer_process_label; // a function pointer
               }
               else {
                    return wsprocbuffer_process_unlabeled; // a function pointer

               }
          }
     }
     return NULL;
}

static inline int wsprocbuffer_nested_search(wsprocbuffer_inst_t * proc,
                                             wsdata_t * tdata, int sid, int * found) {
     int i, j;
     int cnt = 0;
     wsdata_t ** mset;
     int mset_len;

     wslabel_set_t * lset = &proc->nest.lset[sid];

     for (i = 0; i < lset->len; i++) {
          dprint("search for %s", lset->labels[i]->name);
          if (tuple_find_label(tdata,
                               lset->labels[i], &mset_len,
                               &mset)) {
               dprint("found %s", lset->labels[i]->name);
               if (lset->id[i]) {
                    for (j = 0; j < mset_len; j++ ) {
                         dprint("subtuple i %u j %u, %s", i, j,
                                lset->labels[i]->name);
                         //search for sublabels
                         if (mset[j]->dtype == dtype_tuple) {
                              cnt += wsprocbuffer_nested_search(proc, mset[j],
                                                                lset->id[i],
                                                                found);
                         }
                    }
               }
               else {
                    for (j = 0; j < mset_len; j++ ) {
                         char * buf;
                         int blen;
                         if (dtype_string_buffer(mset[j], &buf, &blen)) {
                              *found = 1;
                              cnt += proc->kid->decode_func(proc->kproc, tdata, mset[j],
                                                            (uint8_t*)buf, blen);
                         }
                    }
               }
          }
     }
     return cnt;
}

// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 2 if not output
static int wsprocbuffer_process_label(void * vinstance, wsdata_t* input_data,
                                      ws_doutput_t * dout, int type_index) {
     wsprocbuffer_inst_t * proc = (wsprocbuffer_inst_t*)vinstance;

     proc->recv++;

     int found = 0;
     if (wsprocbuffer_nested_search(proc, input_data, 0, &found) ||
         (!found && proc->kid->pass_not_found)) {
          proc->pass++;
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
     }

     return 1;
}

static int wsprocbuffer_process_unlabeled(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     wsprocbuffer_inst_t * proc = (wsprocbuffer_inst_t*)vinstance;

     proc->recv++;
     wsdt_tuple_t * tuple = input_data->data;
     int i;
     wsdata_t * member;
     int tlen = tuple->len;
     int rtn = 0;

     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          char * buf;
          int blen;
          if (dtype_string_buffer(member, &buf, &blen)) {
               rtn += proc->kid->decode_func(proc->kproc, input_data, member,
                                             (uint8_t*)buf, blen);
          }
     }
    
     if (rtn) { 
          proc->pass++;
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
     }

     return 1;
}

static inline int wsprocbuffer_nested_el_search(wsprocbuffer_inst_t * proc,
                                             wsdata_t * tdata, int sid,
                                             int * found) {
     int i, j;
     int cnt = 0;
     wsdata_t ** mset;
     int mset_len;

     wslabel_set_t * lset = &proc->nest.lset[sid];

     for (i = 0; i < lset->len; i++) {
          dprint("search for %s", lset->labels[i]->name);
          if (tuple_find_label(tdata,
                               lset->labels[i], &mset_len,
                               &mset)) {
               dprint("found %s", lset->labels[i]->name);
               if (lset->id[i]) {
                    for (j = 0; j < mset_len; j++ ) {
                         dprint("subtuple i %u j %u, %s", i, j,
                                lset->labels[i]->name);
                         //search for sublabels
                         if (mset[j]->dtype == dtype_tuple) {
                              cnt += wsprocbuffer_nested_el_search(proc, mset[j],
                                                                   lset->id[i],
                                                                   found);
                         }
                    }
               }
               else {
                    for (j = 0; j < mset_len; j++ ) {
                         *found = 1;
                         cnt += proc->kid->element_func(proc->kproc, tdata,
                                                        mset[j]);
                    }
               }
          }
     }
     return cnt;
}

static int wsprocbuffer_process_el_label(void * vinstance, wsdata_t* input_data,
                                      ws_doutput_t * dout, int type_index) {
     wsprocbuffer_inst_t * proc = (wsprocbuffer_inst_t*)vinstance;

     proc->recv++;
     int found = 0;
     if (wsprocbuffer_nested_el_search(proc, input_data, 0, &found) ||
         (!found && proc->kid->pass_not_found)) {

          proc->pass++;
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
     }

     return 1;
}

static int wsprocbuffer_process_el_unlabeled(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     wsprocbuffer_inst_t * proc = (wsprocbuffer_inst_t*)vinstance;

     proc->recv++;

     wsdt_tuple_t * tuple = input_data->data;
     int i;
     wsdata_t * member;
     int tlen = tuple->len;
     int rtn = 0;

     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          rtn += proc->kid->element_func(proc->kproc, input_data, member);
     }
    
     if (rtn) { 
          proc->pass++;
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int wsprocbuffer_destroy(void * vinstance) {
     wsprocbuffer_inst_t * proc = (wsprocbuffer_inst_t*)vinstance;
     tool_print("%s received %"PRIu64,
                proc->kid->name, proc->recv);
     tool_print("%s passed %"PRIu64,
                proc->kid->name, proc->pass);

     //free dynamic allocations
     if (proc->kid->destroy_func) {
         proc->kid->destroy_func(proc->kproc); 
     }
     if (proc->kproc) {
          free(proc->kproc);
     }
     free(proc);

     return 1;
}

