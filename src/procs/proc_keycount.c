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
//keeps count of keys.. keep a representative tuple for each key..

#define PROC_NAME "keycount"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_double.h"
#include "stringhash5.h"
#include "procloader.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"Statistics", "Math", "Profiling", NULL};
char *proc_alias[]             =  { "countkey", NULL };
char proc_purpose[]            =  "keep counts based on keys";
char proc_description[]  = "The keycount kid counts the number of times that a key "
     "occurs in the dataset. Default behavior flushes table at end of count, and "
     "therefore, each key is output only once (like uniq) with a total count.  However, using the QUERYAPPEND port, the table and current values can be accessed within a live stream.  The QUERYAPPEND port will query the keycount state table at the specified key and append the count data to the tuple used in the query.  Additional options will calculate the percentage of the total keys each key represents (-P), change the default label (COUNT) to a user specified label (-L), change the size of the internal state table (-M).  The '-R' option will drop all labels except the specified key label and the COUNT label from the output tuple.  You might also consider using the cntquery kid which has similar functionality.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'P',"","",
     "calculate percentage",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'L',"","label",
     "record label as label",0,0},
     {'R',"","",
      "keep only the member that matches, not the whole tuple",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]     =  "LABEL of key to count";
char *proc_input_types[]       =  {"tuple", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
char proc_requires[]           =  "";
// Ports: QUERYAPPEND
proc_port_t proc_input_ports[] =  {
     {"none","count item at key"},
     {"QUERYAPPEND","queries current value of key in the table and appends the results to the query tuple"},
     {NULL, NULL}
};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {"COUNT", "PCT", NULL};
char *proc_synopsis[]          =  {"keycount <LABEL> [-P] [-M <records>] [-L <NEWLABEL>] [-R]", NULL};
proc_example_t proc_examples[] =  {
	{"... | keycount KEY | ...", "Buffers a table containing the COUNTS of the KEYs seen.  Table will be dumped at flush event."},
	{"... | $stream1, $stream2:QUERYAPPEND | keycount KEY | ...", "KEYs in $stream1 are set in the keycount state table and counted; KEYs in $stream2 are queried against the keycount state table and tuples that hit are emitted with the current count data appended to the query tuple."},
	{"... | keycount KEY -P -L NEWCOUNT | ...", "Buffers a table containing counts of KEY and percentage of total KEYs seen in the stream.  Table will be dumped at a flush event with the count data in the NEWCOUNT label."},
	{NULL, NULL}
};

typedef struct _key_data_t {
     wsdata_t * wsd;
     uint32_t cnt;
} key_data_t;

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_tuple_query(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t totalcnt;
     uint64_t outcnt;

     stringhash5_t * key_table;
     uint32_t buflen;
     wslabel_set_t lset;
     ws_tuplemember_t * tmember_cnt;
     ws_tuplemember_t * tmember_pct;
     ws_outtype_t * outtype_tuple;
     ws_doutput_t * dout;
     char * strlabel;
     int do_pct;
     int keepOnlyMember;

     char * sharelabel;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:RPl:L:M:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'P':
               proc->do_pct = 1;
               tool_print("do percentage");
               break;
          case 'l':
          case 'L':
               free(proc->strlabel);
               proc->strlabel = strdup(optarg);
               tool_print("setting output label as %s", proc->strlabel);
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          case 'R':
               proc->keepOnlyMember=1;
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
     key_data_t * kd = (key_data_t *)vdata;
     if (kd && kd->wsd) {
          dprint("output wsdata 2");

          wsdata_t * outTuple = NULL;
          if (proc->keepOnlyMember) {
               outTuple = wsdata_alloc(dtype_tuple);  // no reference counter
               if (outTuple) {
                    add_tuple_member(outTuple, kd->wsd);
                    wsdata_delete(kd->wsd);
                    kd->wsd = NULL;
               }
               else {
                    wsdata_delete(kd->wsd);
                    kd->wsd = NULL;
                    kd->cnt = 0;
                    return;
               }
               //Do we need to call wsdata_add_reference here?  I'm assuming
               //that add_tuple member takes care of it
          }
          else {
               outTuple = kd->wsd;
          }
           
          wsdt_uint_t * cntp = tuple_member_alloc(outTuple, proc->tmember_cnt);
          if (cntp) {
               *cntp = kd->cnt;
          }
          if (proc->do_pct) {
               wsdt_double_t * pct = tuple_member_alloc(outTuple,
                                                        proc->tmember_pct);
               if (pct) {
                    *pct = (double)kd->cnt/(double)proc->totalcnt;
               }
          }

          proc->outcnt++;

          ws_set_outdata(outTuple, proc->outtype_tuple, proc->dout);

          if (!proc->keepOnlyMember) {
               wsdata_delete(outTuple);
          }
     }
     kd->wsd = NULL;
     kd->cnt = 0;
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

     proc->strlabel = strdup("COUNT");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     proc->tmember_cnt = register_tuple_member_type(type_table,
                                                    "UINT_TYPE",
                                                    proc->strlabel);
     proc->tmember_pct = register_tuple_member_type(type_table,
                                                    "DOUBLE_TYPE",
                                                    "PCT");

     //other init - init the stringhash table
     if (proc->sharelabel) {
          stringhash5_sh_opts_t * sh5_sh_opts;
          int ret;

          //calloc shared sh5 option struct
          stringhash5_sh_opts_alloc(&sh5_sh_opts);

          //set shared sh5 option fields
          sh5_sh_opts->sh_callback = last_destroy;
          sh5_sh_opts->proc = proc; 

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->key_table, 
                                              proc->sharelabel, proc->buflen, 
                                              sizeof(key_data_t), NULL, sh5_sh_opts);

          //free shared sh5 option struct
          stringhash5_sh_opts_free(sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          proc->key_table = stringhash5_create(0, proc->buflen, sizeof(key_data_t));
          if (!proc->key_table) {
               return 0;
          }
          stringhash5_set_callback(proc->key_table, last_destroy, proc);
     }

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->key_table->max_records;

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
     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          return proc_tuple;
     }

     return NULL; // a function pointer
}

static inline void add_member(proc_instance_t * proc, wsdata_t * tdata, wsdata_t * member) {
     key_data_t * kdata;
     kdata = (key_data_t*)stringhash5_find_attach_wsdata(proc->key_table, member);

     if (kdata) {
          if (!kdata->wsd) {
               if (proc->keepOnlyMember) {
                    kdata->wsd = member;
                    wsdata_add_reference(member);
               } else {
                    kdata->wsd = tdata;
                    wsdata_add_reference(tdata);
               }
          }
          kdata->cnt++;
          stringhash5_unlock(proc->key_table);
          proc->totalcnt++;
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

     //search for items in tuples
     wsdata_t ** mset;
     int mset_len;
     int i, j;
     for (i = 0; i < proc->lset.len; i++) {
          if (tuple_find_label(input_data, proc->lset.labels[i],
                               &mset_len, &mset)) {
               for (j = 0; j < mset_len; j++ ) {
                    add_member(proc, input_data, mset[j]);
               }
          }
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}

static inline int query_append_key(proc_instance_t * proc, wsdata_t * query, wsdata_t * member) {
     key_data_t * kdata = NULL;
     ws_hashloc_t * hashloc = member->dtype->hash_func(member);
     if (hashloc && hashloc->len) {
          kdata = (key_data_t *) stringhash5_find(proc->key_table,
                                                  (uint8_t*)hashloc->offset,
                                                  hashloc->len);
     }
     if (kdata) {
          wsdt_uint_t * cntp = tuple_member_alloc(query, proc->tmember_cnt);
          if (cntp) {
               *cntp = kdata->cnt;
          }
          stringhash5_unlock(proc->key_table);
          return 1;
     }
     return 0;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple_query(void * vinstance, wsdata_t* input_data,
                            ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     proc->meta_process_cnt++;

     //search key
     wsdata_t ** mset;
     int mset_len;
     int i, j;
     int found = 0;
     for (i = 0; i < proc->lset.len; i++) {
          if (tuple_find_label(input_data, proc->lset.labels[i], &mset_len,
                               &mset)) {
               for (j = 0; j < mset_len; j++ ) {
                    if (query_append_key(proc, input_data, mset[j])) {
                         found = 1;
                    }
               }
          }
     }
 
     if (found) {
          ws_set_outdata(input_data, proc->outtype_tuple, proc->dout);
          proc->outcnt++;
     }

     return 1;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;

     stringhash5_scour_and_flush(proc->key_table, last_destroy, proc);

     proc->totalcnt = 0;

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     stringhash5_destroy(proc->key_table);

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     if (proc->strlabel) {
          free(proc->strlabel);
     }
     free(proc);

     return 1;
}


