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
#define PROC_NAME "haslabel"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "waterslide_io.h"
#include "procloader.h"
#include "datatypes/wsdt_tuple.h"

char proc_version[]     = "1.5";
char *proc_tags[] = {"Filtering", NULL};
char *proc_alias[]     = { "order", NULL};
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Provides filtering of events based on label existence";
char *proc_synopsis[] = {"haslabel <LABEL1> [<LABEL2> [<...>]] [-A] [-N] [-m <value>]", NULL};
char proc_description[] = 
	"The 'haslabel' kid is used for filtering events from a stream by "
	"checking for the existence of a label or set of labels in a tuple's members or on the tuple container. "
	"It is also possible to check for the existence of one (using the '-A' "
	" option) or many labels given a list of labels, multiple instances "
	"of a label in a tuple (using the '-m' option), as well as inverse "
	"filtering (using the '-N' option) on a specific label.";
proc_example_t proc_examples[] = {
	{"... | haslabel FAMILY | ...","Pass events that have the FAMILY label"},
	{"... | haslabel -A NOUN VERB | ...","Pass events that have either the NOUN or a VERB label"},
	{"... | haslabel -m 5 PEAR | ...","Pass events that have 5 PEAR labels"},
	{"... | NOT:haslabel INVALID | ...","Only pass events that do not have the INVALID label"},
	{"... | haslabel -N INVALID | ...","Only pass events that do not have the INVALID label"},
	{"... | haslabel NOT INVALID | ...","Only pass events that do not have the INVALID label"},
     {NULL,""}
};

char proc_requires[] = "";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'A',"","",
     "pass if any label matches",0,0},
     {'N',"","",
     "pass if labels do NOT match",0,0},
     {'m',"","count",
     "pass only if label appears m times in a tuple",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABELs to match or keyword NOT";
char *proc_input_types[]    = {"tuple", "flush", "monitor", "any", NULL};
char *proc_output_types[]    = {"tuple", "any", NULL};
proc_port_t proc_input_ports[] = {
	{"none","normal operation"},
	{"NOT","Check if the label is NOT in the tuple"},
	{NULL,NULL}
};
char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};

//function prototypes for local functions
static int proc_monitor(void *, wsdata_t *, ws_doutput_t *, int);
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int process_tuple_multicnt(void *, wsdata_t*, ws_doutput_t*, int);
static int process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int process_meta_not(void *, wsdata_t*, ws_doutput_t*, int);
static int process_not(void *, wsdata_t*, ws_doutput_t*, int);

#define LOCAL_MAX_TYPES 64
typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     int any_match;
     int do_not;
     wslabel_nested_set_t nest;
     wslabel_t * label_process_cnt;
     wslabel_t * label_process_rate;
     wslabel_t * label_outcnt;
     wslabel_t * label_haslabel;
     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];
     int multicnt;
     struct timeval real_end_time;
     struct timeval real_start_time;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "m:AN")) != EOF) {
          switch (op) {
          case 'A':
               proc->any_match = 1;
               tool_print("matching any labels");
               break;
          case 'N':
               proc->do_not = 1;
               break;
          case 'm':
               proc->multicnt = atoi(optarg);
               break;
          default:
               return 0;
          }
     }

     while (optind < argc) {
          if (strcasecmp("NOT", argv[optind]) == 0) {
               proc->do_not = 1;
               tool_print("inverting search.. ");
          }
          else {
               if (wslabel_nested_search_build(type_table, &proc->nest,
                                           argv[optind])) {
                    tool_print("searching for label %s", argv[optind]);
               }
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

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     //register labels for returning monitoring data
     proc->label_process_cnt = wsregister_label(type_table, "PROCESS_CNT");
     proc->label_outcnt = wsregister_label(type_table, "OUTPUT_CNT");
     proc->label_process_rate = wsregister_label(type_table, "PROCESS_RATE");
     proc->label_haslabel = wsregister_label(type_table, "HASLABEL");

     if (!proc->nest.cnt) {
          error_print("need labels");
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

     if (wsdatatype_match(type_table, meta_type, "MONITOR_TYPE") )
     {
          return proc_monitor;
     }

     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          return NULL;
     }
     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          proc->outtype_tuple = ws_add_outtype(olist, meta_type, NULL);
          if (proc->multicnt) {
               return process_tuple_multicnt;
          }
          if (proc->do_not || wslabel_match(type_table, port, "NOT")) {
               return process_not;
          }
          else {
               return process_tuple;
          }
     }
     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type, NULL);
     if (proc->do_not || wslabel_match(type_table, port, "NOT")) {
          return process_meta_not;
     }
     return process_meta;
}

static inline int container_search(proc_instance_t * proc,
                                   wsdata_t * tdata) {
     int i, j;
     int cnt = 0;

     for (i = 0; i < tdata->label_len; i++) {
          for (j = 0; j < proc->nest.lset[0].len; j++) {
               if (!proc->nest.lset[0].id[j] &&
                   tdata->labels[i] == proc->nest.lset[0].labels[j]) {
                    cnt++;
                    break; // out of inner for loop
               }
          }
     }
     return cnt;
}

static inline int local_nested_search(proc_instance_t * proc,
                                             wsdata_t * tdata, int sid) {
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
               if (lset->id[i]) {
                    for (j = 0; j < mset_len; j++ ) {
                         //search for sublabels
                         if (mset[j]->dtype == dtype_tuple) {
                              int add;

                              add = local_nested_search(proc, mset[j],
                                                        lset->id[i]);
                              if (add) {
                                   cnt += add;
                                   break; 
                              }
                         }
                    }
               }
               else {
                    cnt++;
               }
          }
     }
     return cnt;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     dprint("proc_haslabel");

     proc->meta_process_cnt++;

     int uniq_labels;

     uniq_labels = local_nested_search(proc, input_data, 0);

     if (uniq_labels < proc->nest.cnt) {
          uniq_labels += container_search(proc, input_data);
     }
    
     if ((proc->any_match && uniq_labels) || (uniq_labels >= proc->nest.cnt)) { 
          proc->outcnt++;
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          return 1;
     }
     return 0;
}

static inline int local_nested_multicnt_search(proc_instance_t * proc,
                                               wsdata_t * tdata, int sid) {
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
               if (lset->id[i]) {
                    for (j = 0; j < mset_len; j++ ) {
                         //search for sublabels
                         if (mset[j]->dtype == dtype_tuple) {
                              int add;
                              add = local_nested_multicnt_search(proc, mset[j],
                                                                 lset->id[i]);
                              if (add) {
                                   cnt += add;
                                   break;
                              }
                         }
                    }
               }
               else {
                    if (mset_len >= proc->multicnt) {
                         cnt++;
                    }
               }
          }
     }
     return cnt;
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_tuple_multicnt(void * vinstance, wsdata_t* input_data,
                                  ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     dprint("proc_haslabel");

     proc->meta_process_cnt++;

     int uniq_labels;

     uniq_labels = local_nested_multicnt_search(proc, input_data, 0);
    
     if ((proc->any_match && uniq_labels) || (uniq_labels >= proc->nest.cnt)) { 
          proc->outcnt++;
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          return 1;
     }
     return 0;
}


static int process_meta(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     dprint("proc_haslabel");

     proc->meta_process_cnt++;

     int uniq_labels = 0;

     uniq_labels += container_search(proc, input_data);
    
     if ((proc->any_match && uniq_labels) || (uniq_labels >= proc->nest.cnt)) { 
          proc->outcnt++;
          ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
          return 1;
     }
     return 0;
}

static int process_meta_not(void * vinstance, wsdata_t* input_data,
                                                    ws_doutput_t * dout,
                                                    int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     dprint("proc_haslabel");

     proc->meta_process_cnt++;

     if (!container_search(proc, input_data)) {
          proc->outcnt++;
          ws_set_outdata(input_data,
                         proc->outtype_meta[type_index],
                         dout);
          return 1;
     }
     return 0;
}


static inline int local_nested_not_search(proc_instance_t * proc,
                                               wsdata_t * tdata, int sid) {
     int i, j;
     wsdata_t ** mset;
     int mset_len;

     wslabel_set_t * lset = &proc->nest.lset[sid];

     for (i = 0; i < lset->len; i++) {
          dprint("search for %s", lset->labels[i]->name);
          if (tuple_find_label(tdata,
                               lset->labels[i], &mset_len,
                               &mset)) {
               if (lset->id[i]) {
                    for (j = 0; j < mset_len; j++ ) {
                         //search for sublabels
                         if (mset[j]->dtype == dtype_tuple) {
                              if (local_nested_not_search(proc, mset[j],
                                                          lset->id[i])) {
                                   return 1;
                              }
                         }
                    }
               }
               else {
                    return 1;
               }
          }
     }
     return 0;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_not(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     if (container_search(proc, input_data)) {
          return 0;
     }

     if (local_nested_not_search(proc, input_data, 0)) {
          return 0; 
     }
    
     proc->outcnt++;
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     return 1;
}

//used for making monitoring re-fire at each t-interval
//this makes little sense to me but it's how bandwidth was doing it so we'll try doing
//it here too and see what happens...
static void print_waterslide_stats_tuple(proc_instance_t *proc, wsdata_t *tdata)
{
     double timediff;

     double t1, t2;

     //convert integer times to decimal seconds
     t1 =  (double)proc->real_start_time.tv_usec/1000000. +
           (double)proc->real_start_time.tv_sec;
     t2 =  (double)proc->real_end_time.tv_usec/1000000. +
           (double)proc->real_end_time.tv_sec;

     timediff = t2 - t1;

     tuple_member_create_uint64(tdata, proc->meta_process_cnt, proc->label_process_cnt);

     if (timediff)
     {
          tuple_member_create_double( tdata,
                                      (double)proc->meta_process_cnt / timediff,
                                      proc->label_process_rate);
     }
}


// this function is polled by a monitor kid to get periodic health and status
static int proc_monitor(void *vinstance, wsdata_t *input_data,
                        ws_doutput_t *dout, int type_index)
{
     proc_instance_t *proc = (proc_instance_t *)vinstance;

     //you need to get a tuple from the monitor to write your output
     wsdata_t *mtdata = wsdt_monitor_get_tuple(input_data);

     //allocate a subtuple for organizing a kid's specific output
     wsdata_t *tdata = tuple_member_create_wsdata(mtdata, dtype_tuple,
                                                  proc->label_haslabel);

     //print the stats this proc has collected to date to our kid's labelled tuple,
     //registering individual stat labels as searchable as we go
     if (tdata)
     {
          if (proc->meta_process_cnt) {
                print_waterslide_stats_tuple(proc, tdata);
          }

          if (proc->outcnt) {
                tuple_member_create_uint64(tdata, proc->outcnt,
                                           proc->label_outcnt);
          }

     }
     //the following function must be called before exiting this callback;
     // signals that  kid has been visited and data has been appended
     wsdt_monitor_set_visit(input_data);
     return 0;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("input cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free dynamic allocations
     free(proc);

     return 1;
}

