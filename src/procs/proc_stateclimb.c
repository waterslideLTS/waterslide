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
// look for conditions.. climb to value at key to output

// run as ...   stateclimb -T 20 KEY +2 COND1 COND2 ~4 COND3 COND4
// which will add +2 for COND1 and COND2
// it will subtract 4 for COND3 and COND4

#define PROC_NAME "stateclimb"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_ts.h"
#include "stringhash5.h"
#include "procloader.h"
#include "sysutil.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"Filtering", "Detection", "Profiling", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "track state at key, climb to condition";
char proc_description[] = "The goal of the stateclimb kid is to track *weighted* counts of unique values (labels) associated with a single key, emitting the tuple once a threshold has been reached.  The threshold value which determines when a tuple will be emitted is specified with the '-T' option (the default threshold is 10).  Each time an event is seen for the given key, the condition-labels are evaluated and the score for this key is incremented or decremented based on the parameters specified on the command line.  Scores for each condition label are specified with the '~' (tilde) operator for negative values, and the '+' operator for positive values.  For example, '~10 GOOD' would decrement the score at the key by 10 each time the GOOD label is seen, while '+10 BAD' would increment the score each time the BAD label is seen.  To prevent an event from exceeding the scoring ceiling (or floor), the -C and -c options can be specified (the '-C' option controls the maximum ceiling value (default is 1000000); the '-c' option controls the minimum floor value (default is -1000000)).  If the ceiling or floor is encountered, the score for this key will be set to the ceiling or floor value; if the '-R' option is specified, then when a score exceeds the ceiling or floor value, the score will be reset to 0.  Multiple labels within a tuple containing the specified condition label will cause the score to be adjusted for each label found. The '-s' option will pull conditions from container labels rather than from member labels.  The TAG port can be utilized if it is desireable for all tuples to be emitted; in this case, if the threshold is exceeded then the STATECLIMB container label is added to the tuple, otherwise, tuples are emitted unmodified.  Note that the threshold must be *exceeded* for the tuple to be emitted.  The '-M' option specifies how many different keys can be stored in the state-tracking table.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'T',"","threshold",
     "threshold to reach (default is 10)",0,0},
     {'C',"","max",
     "maximum ceiling condition",0,0},
     {'c',"","min",
     "minimum floor condition (negative value)",0,0},
     {'R',"","",
     "reset to zero when reached threshold",0,0},
     {'s',"","",
     "use container labels for updating score",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     {'F',"","filename",
     "load existing database (key table) from file",0,0},
     {'O',"","filename",
     "write database (key table) to file",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]     =  "LABEL of key to track";
char *proc_input_types[]       =  {"tuple", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
char proc_requires[]           =  "";
// Ports: TAG
proc_port_t proc_input_ports[] =  {
     {"none","output only when threshold reached"},
     {"TAG","pass all tuples, tag if threshold reached"},
     {NULL, NULL}
};

char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {"STATECLIMB", NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  {"stateclimb <LABEL> [-T <threshold>] [-C <max>] [-c <min>] [-R] [-s] [-M <records>] [[~|+]<value> <CONDITION_LABEL>|...]", NULL};
proc_example_t proc_examples[] =  {
	{"... | stateclimb -R -T 5 KEY ~1 GOOD +2 BAD +5 UGLY | ...", "For each unique KEY, keep running score of events as they are seen, decrementing the score by 1 if the tuple has a member label GOOD, and incrementing by 2 and 5, respectively, if the BAD or UGLY labels exist in the tuple.  Emit the tuple when the score exceeds the threshold of 5.  If the score exceeds the ceiling or floor values, reset the score to 0."},
	{"... | stateclimb -C 10 -T 5 KEY ~1 GOOD +2 BAD +5 UGLY | ...", "For each unique KEY, keep running score of events as they are seen, decrementing the score by 1 if the tuple has a member label GOOD, and incrementing by 2 and 5, respectively, if the BAD or UGLY labels exist in the tuple.  Emit the tuple when the score exceeds the threshold of 5.  If the score exceeds 10, reset the score to 10."},
	{"... | TAG:stateclimb -T 5 KEY ~1 GOOD +2 BAD +5 UGLY | ...", "For each unique KEY, keep running score of events as they are seen, decrementing the score by 1 if the tuple has a member label GOOD, and incrementing by 2 and 5, respectively, if the BAD or UGLY labels exist in the tuple.  Emit all tuples, but tag tuples exceeding the threshold value with the container label, STATECLIMB."},
        {"... | stateclimb -s -T 5 KEY ~1 GOOD +2 BAD +5 UGLY | ...", "For each unique KEY, keep running score of events as they are seen, decrementing the score by 1 if the tuple has a *container* label GOOD, and incrementing by 2 and 5, respectively, if the BAD or UGLY *container* labels exist on the tuple.  Emit tuples if the score exceeds the threshold value."},
	{NULL, NULL}
};

typedef struct _key_data_t {
     int value;
} key_data_t;

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_tuple_tag(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     stringhash5_t * key_table;
     wslabel_t * label_key;
     wslabel_t * label_climb;
     wslabel_set_t lset;
     ws_outtype_t * outtype_tuple;
     int threshold;
     uint32_t buflen;
     int max;
     int min;
     int reset;
     int tuple_container_search;
     char * outfile;
     char * open_table;

     char * sharelabel;
     int sharer_id;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:sRC:c:T:M:F:O:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'R':
               proc->reset = 1;
               break;
          case 'T':
               proc->threshold = atoi(optarg);
               tool_print("threshold set to %d", proc->threshold);
               break;
          case 'C':
               proc->max = atoi(optarg);
               tool_print("max set to %d", proc->max);
               break;
          case 'c':
               proc->min = atoi(optarg);
               tool_print("min set to %d", proc->min);
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          case 's':
               proc->tuple_container_search = 1;
               break;
          case 'F':
               proc->open_table = strdup(optarg);
               // loading of the table is postponed to the stringhash5_create_shared_sht
               // call in proc_init
               break;
          case 'O':
               proc->outfile = strdup(optarg);
               break;
          default:
               return 0;
          }
     }
     int incr = 1;
     while (optind < argc) {
          char * lbl = argv[optind];
          switch (lbl[0]) {
          case '0':
               break;
          case '+':
               incr = atoi(lbl+1);
               break;
          case '-':
          case '~':
               incr = -atoi(lbl+1);
               break;
          default:
               if (!proc->label_key) {
                    proc->label_key = wssearch_label(type_table, lbl);
                    tool_print("using key %s", lbl);
               }
               else {
                    wslabel_set_add_id(type_table, &proc->lset,
                                       lbl, incr);
                    tool_print("using condition %s, %d", lbl, incr);
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
     
     ws_default_statestore(&proc->buflen);

     proc->threshold = 10;
     proc->max = 1000000;
     proc->min = -1000000;
     
     proc->label_climb = wsregister_label(type_table, "STATECLIMB");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     //other init - init the stringhash table

     //calloc shared sh5 option struct
     stringhash5_sh_opts_t * sh5_sh_opts;
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     //set shared sh5 option fields
     sh5_sh_opts->open_table = proc->open_table;

     if (proc->sharelabel) {
          int ret;

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->key_table, 
                                              proc->sharelabel, proc->buflen, 
                                              sizeof(key_data_t), &proc->sharer_id, 
                                              sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          // read the stringhash5 table from the open_table file
          uint32_t ret = 0;
          if (proc->open_table) {
               ret = stringhash5_open_sht_table(&proc->key_table, proc, proc->buflen, 
                                                sizeof(key_data_t), sh5_sh_opts);
          }
          // create the stringhash5 table from scratch
          if (!ret) {
               proc->key_table = stringhash5_create(0, proc->buflen, 
                                                    sizeof(key_data_t));
               if (!proc->key_table) {
                    return 0;
               }
          }
     }

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->key_table->max_records;

     free(proc->open_table);

     if (proc->lset.len == 0) {
          tool_print("must specify a key to track");
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

     if (meta_type == dtype_tuple) {
          if (!proc->outtype_tuple) {
               proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple,
                                                    NULL);
          }
          if (wslabel_match(type_table, port, "TAG")) {
               return proc_tuple_tag;
          }

          return proc_tuple;
     }

     return NULL; // a function pointer
}

static inline int lookup_tuple_cond(proc_instance_t * proc, wsdata_t * tdata) {
     key_data_t * kdata = NULL;
     int rtn = 0;

     wsdata_t ** mset;
     int mset_len;
     int i;
     if (tuple_find_label(tdata, proc->label_key,
                          &mset_len, &mset)) {
          kdata = (key_data_t *) stringhash5_find_attach_wsdata(proc->key_table,
                                                                mset[0]);
     }

     if (!kdata) {
          return rtn;
     }

     if (proc->tuple_container_search) {
          for (i = 0; i < proc->lset.len; i++) {
               if (wsdata_check_label(tdata, proc->lset.labels[i])) {
                    kdata->value += proc->lset.id[i];  
               }
          }
     }
     else {
          //search for condition labels in tuples
          for (i = 0; i < proc->lset.len; i++) {
               if (tuple_find_label(tdata, proc->lset.labels[i],
                                    &mset_len, &mset)) {
                    kdata->value += proc->lset.id[i];  
               }
          }
     }
     if (kdata->value > proc->threshold) {
          //always pass thru..
          wsdata_add_label(tdata, proc->label_climb);
          rtn = 1;
          if (proc->reset) {
               kdata->value = 0;
          }
     }
     if (kdata->value > proc->max) {
          kdata->value = proc->max;
     }
     else if (kdata->value < proc->min) {
          kdata->value = proc->min;
     }
     stringhash5_unlock(proc->key_table);

     return rtn;
}

static int proc_tuple(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     if (lookup_tuple_cond(proc, input_data)) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }
     return 1;
}

static int proc_tuple_tag(void * vinstance, wsdata_t* input_data,
                          ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     lookup_tuple_cond(proc, input_data);
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;

     return 1;
}

static void serialize_table(proc_instance_t * proc) {
     if (proc->outfile && (!proc->sharelabel || !proc->sharer_id)) {
	  tool_print("Writing data table to %s", proc->outfile);
          FILE * fp = fopen(proc->outfile, "w");
	  if (fp) {
	       stringhash5_dump(proc->key_table, fp);
	       fclose(fp);
	  }
          else {
               perror("failed writing data table");
               tool_print("unable to write to file %s", proc->outfile);
          }
     }
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     serialize_table(proc);
     stringhash5_destroy(proc->key_table);

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     if (proc->outfile) {
          free(proc->outfile);
     }
     free(proc);

     return 1;
}

