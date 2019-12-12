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
//keeps last wsdata tuple container based on key

#define PROC_NAME "keeplast"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "stringhash5.h"
#include "procloader.h"

char proc_version[]     = "1.1";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "lastvalue", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Keep last wsdata from key";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'V',"","label to store",
     "label of item to store at key",0,0},
     {'N',"","records",
     "maximum table size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABEL of key to index on";
char *proc_input_types[]    = {"any","tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[] = {
     {"none","Check and save last data at key"},
     {"QUERYAPPEND","lookup key, append value to tuple"},
     {"ADD","add key and value to state"},
     {NULL, NULL}
};


//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_tuple_query(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t replaced;
     uint64_t outcnt;

     stringhash5_t * last_table;
     uint32_t buflen;
     wslabel_set_t lset;
     ws_outtype_t * outtype_tuple;
     ws_doutput_t * dout;
     int do_output;
     wslabel_t * label_value;

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
               break;
          case 'N':
               proc->buflen = atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset, argv[optind]);
          tool_print("using key %s", argv[optind]);
          optind++;
     }
     
     return 1;
}

static void last_destroy(void * vdata, void * vproc) {
     proc_instance_t * proc = (proc_instance_t *)vproc;
     wsdata_t ** wsdp = (wsdata_t **)vdata;
     if (wsdp && (*wsdp)) {
          dprint("output wsdata 2");
          wsdata_t * wsd = *wsdp;          
          if (proc->do_output && proc->dout) {
               if (wsd->dtype != dtype_tuple) {
                    wsdata_t * tdata = wsdata_alloc(dtype_tuple);              
                    if (tdata) {
                         add_tuple_member(tdata, wsd);
                         ws_set_outdata(tdata, proc->outtype_tuple, proc->dout);
                         proc->outcnt++;
                    }
               }
               else {
                    ws_set_outdata(wsd, proc->outtype_tuple, proc->dout);
                    proc->outcnt++;
               }
          }
          wsdata_delete(wsd);
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

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
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

     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
     }
     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          return proc_flush;
     }
     if (wslabel_match(type_table, port, "QUERYAPPEND")) {
          if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               return proc_tuple_query;
          }
     }
     if (wslabel_match(type_table, port, "ADD")) {
          if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               return proc_tuple;
          }
     }

     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          proc->do_output = 1;
          return proc_tuple;
     }

     return NULL; // a function pointer
}

static inline void add_member(proc_instance_t * proc, wsdata_t * tdata, wsdata_t * member) {
     wsdata_t ** lastdata = NULL;
     ws_hashloc_t * hashloc = member->dtype->hash_func(member);
     if (hashloc && hashloc->len) {
          lastdata = (wsdata_t **) stringhash5_find_attach(proc->last_table,
                                                           (uint8_t*)hashloc->offset,
                                                           hashloc->len);
     }
     if (lastdata) {
          if (*lastdata) {
               //delete old data to be replaced by new data
               wsdata_delete(*lastdata);
               proc->replaced++;
          }
          *lastdata = tdata;
          wsdata_add_reference(tdata);
          stringhash5_unlock(proc->last_table);
     }
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     proc->meta_process_cnt++;

     int id;
     wsdata_t * member;
     wslabel_t * label;
     tuple_labelset_iter_t iter;
     tuple_init_labelset_iter(&iter, input_data,
                              &proc->lset);

     wsdata_t * value = input_data;

     if (proc->label_value) {
          wsdata_t ** mset;
          int mset_len;
          if (tuple_find_label(input_data, proc->label_value,
                               &mset_len, &mset)) {
               value = mset[0];
          }
          else {
               return 0;
          }
     }
     while (tuple_search_labelset(&iter, &member, &label, &id)) {
          add_member(proc, value, member);
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}

static inline void query_append_key(proc_instance_t * proc, wsdata_t * query, wsdata_t * member) {
     wsdata_t ** lastdata = NULL;
     ws_hashloc_t * hashloc = member->dtype->hash_func(member);
     if (hashloc && hashloc->len) {
          lastdata = (wsdata_t **) stringhash5_find(proc->last_table,
                                                    (uint8_t*)hashloc->offset,
                                                    hashloc->len);
     }
     if (lastdata && *lastdata) {
          wsdata_t * wsd = *lastdata;
          if (wsd->dtype == dtype_tuple) {
               wsdt_tuple_t * tuple = wsd->data; 
               int i;
               for (i = 0; i < tuple->len; i++) {
                    add_tuple_member(query, tuple->member[i]); 
               }
          }
          else {
               add_tuple_member(query, wsd); 
          }
     }
     if (lastdata) {
          stringhash5_unlock(proc->last_table);
     }
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple_query(void * vinstance, wsdata_t* input_data,
                            ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     proc->meta_process_cnt++;

     int id;
     wsdata_t * member;
     wslabel_t * label;
     tuple_labelset_iter_t iter;
     tuple_init_labelset_iter(&iter, input_data,
                              &proc->lset);

     while (tuple_search_labelset(&iter, &member,
                                  &label, &id)) {
          query_append_key(proc, input_data, member);
     }

     ws_set_outdata(input_data, proc->outtype_tuple, proc->dout);
     proc->outcnt++;
     //always return 1 since we don't know if table will flush old data
     return 1;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;

     stringhash5_scour_and_flush(proc->last_table, last_destroy, proc);

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("replaced %" PRIu64, proc->replaced);
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

