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
//keeps track of when keys are seen, labels keys based on NEW, RECENT or OLD

#define PROC_NAME "keytime"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_double.h"
#include "stringhash5.h"
#include "stringhash9a.h"
#include "procloader.h"
#include "sysutil.h"

char proc_version[]     = "1.1";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "keyoccur", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "labels repeated key based on occurance near in time/sequence";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'j',"","sharename5",
     "shared table with other sh5 kids",0,0},
     {'o',"","seconds",
     "seconds before something is old",0,0},
     {'r',"","seconds",
     "seconds before something is recent",0,0},
     {'O',"","file",
     "write existence table to file",0,0},
     {'L',"","file",
     "load existence table from file",0,0},
     {'M',"","records",
     "maximum data table size",0,0},
     {'N',"","records",
     "maximum existence table size",0,0},
     {'F',"","filename",
     "load existing database (key table) from file",0,0},
     {'P',"","filename",
     "write database (key table) to file",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of key to count";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[] = {
     {"none","count item at key"},
     {NULL, NULL}
};

#define LOCAL_MAX_SH9A_TABLE 1000000

typedef struct _key_data_t {
     wsdt_uint64_t key;
     time_t first_time;
} key_data_t;

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     stringhash5_t * key_table;
     stringhash9a_t * old_table;
     uint32_t buflen;
     uint32_t max_table;
     wslabel_set_t lset;
     wslabel_t * label_date;
     wslabel_t * label_new;
     wslabel_t * label_recent;
     wslabel_t * label_old;
     ws_outtype_t * outtype_tuple;
     ws_doutput_t * dout;
     time_t old;
     time_t recent;
     char * open_table;
     char * outfile;
     char * outfile5;
     char * open_table5;

     char * sharelabel;
     char * sharelabel5;
     int sharer_id;
     int sharer_id5;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:j:L:O:r:o:M:N:F:P:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'j':
               proc->sharelabel5 = strdup(optarg);
               break;
          case 'L':
               proc->open_table = strdup(optarg);
               // loading of the table is postponed to the stringhash9a_create
               // call in proc_init
               break;
          case 'O':
               proc->outfile = strdup(optarg);
               break;
          case 'o':
               proc->old = atoi(optarg);
               tool_print("old set to %d", (int)proc->old);
               break;
          case 'r':
               proc->recent = atoi(optarg);
               tool_print("recent set to %d", (int)proc->old);
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          case 'N':
               proc->max_table = atoi(optarg);
               break;
          case 'F':
               proc->open_table5 = strdup(optarg);
               // loading of the table is postponed to the stringhash5_create_shared_sht
               // call in proc_init
               break;
          case 'P':
               proc->outfile5 = strdup(optarg);
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

     //append key to old list
     if (kd) {
          stringhash9a_set(proc->old_table, &kd->key, sizeof(wsdt_uint64_t));
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
     proc->max_table = LOCAL_MAX_SH9A_TABLE;

     proc->old = 3600;
     proc->recent = 600;
     proc->label_date = wssearch_label(type_table, "DATETIME");
     proc->label_new = wsregister_label(type_table, "NEW");
     proc->label_recent = wsregister_label(type_table, "RECENT");
     proc->label_old = wsregister_label(type_table, "OLD");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     //other init - init the stringhash tables

     //init the first hash table

     //calloc shared sh5 option struct
     stringhash5_sh_opts_t * sh5_sh_opts;
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     //set shared sh5 option fields
     sh5_sh_opts->sh_callback = last_destroy;
     sh5_sh_opts->proc = proc; 
     sh5_sh_opts->open_table = proc->open_table5;

     if (proc->sharelabel5) {
          int ret;

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->key_table, 
                                              proc->sharelabel5, proc->buflen, 
                                              sizeof(key_data_t), &proc->sharer_id5, 
                                              sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          // read the stringhash5 table from the open_table file
          uint32_t ret = 0;
          if (proc->open_table5) {
               ret = stringhash5_open_sht_table(&proc->key_table, proc, proc->buflen, 
                                                sizeof(key_data_t), sh5_sh_opts);
          }
          // create the stringhash5 table from scratch
          if (!ret) {
               proc->key_table = stringhash5_create(0, proc->buflen, sizeof(key_data_t));
               if (!proc->key_table) {
                    return 0;
               }
          }
          stringhash5_set_callback(proc->key_table, last_destroy, proc);
     }

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //use the stringhash5-adjusted value of max_records to reset buflen
     proc->buflen = proc->key_table->max_records;

     free(proc->open_table5);

     //init the second hash table
     stringhash9a_sh_opts_t * sh9a_sh_opts;

     //calloc shared sh9a option struct
     stringhash9a_sh_opts_alloc(&sh9a_sh_opts);

     //set shared sh9a option fields
     sh9a_sh_opts->open_table = proc->open_table;

     if (proc->sharelabel) {
          if (!stringhash9a_create_shared_sht(type_table, (void **)&proc->old_table, 
                                          proc->sharelabel, proc->max_table, 
                                          &proc->sharer_id, sh9a_sh_opts)) {
               return 0;
          }
     }
     else {
          // read the stringhash9a table from the open_table file
          uint32_t ret = 0;
          if (proc->open_table) {
               ret = stringhash9a_open_sht_table(&proc->old_table, 
                                                 proc->max_table, sh9a_sh_opts);
          }
          // create the stringhash9a table from scratch
          if (!ret) {
               proc->old_table = stringhash9a_create(0, proc->max_table);
               if (NULL == proc->old_table) {
                    tool_print("unable to create a proper stringhash9a table");
                    return 0;
               }
          }
     }

     //free shared sh9a option struct
     stringhash9a_sh_opts_free(sh9a_sh_opts);

     //use the stringhash9a-adjusted value of max_records to reset max_table
     proc->max_table = proc->old_table->max_records;

     free(proc->open_table);

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
          proc->outtype_tuple = ws_add_outtype(olist, wsdatatype_get(type_table,
                                                                     "TUPLE_TYPE"), NULL);
          return proc_flush;
     }
     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          proc->outtype_tuple = ws_add_outtype(olist, meta_type, NULL);
          return proc_tuple;
     }

     return NULL; // a function pointer
}

static inline int add_member(proc_instance_t * proc, wsdata_t * tdata,
                             wsdata_t * member, wsdt_ts_t * ts) {
     key_data_t * kdata = NULL;
     wsdt_uint64_t * tag;
     wsdt_uint64_t local_tag;
     if (member->dtype != dtype_uint64) {
          ws_hashloc_t * hashloc;
          hashloc = member->dtype->hash_func(member);
          local_tag = evahash64(hashloc->offset, hashloc->len, 0xF32FEE11);
          tag = &local_tag;
     }
     else {
          tag = member->data;
     }

     if (stringhash9a_check(proc->old_table, tag, sizeof(wsdt_uint64_t))) {
          //label as old
          tuple_add_member_label(tdata, member, proc->label_old);
          return 2;
     }

     kdata = (key_data_t *) stringhash5_find_attach(proc->key_table,
                                                    (uint8_t*)tag,
                                                    sizeof(wsdt_uint64_t));
     if (kdata) {
          if (!kdata->key) {
               kdata->key = *tag;
               if (ts) {
                    kdata->first_time = ts->sec;
               }
               stringhash5_unlock(proc->key_table);
               tuple_add_member_label(tdata, member, proc->label_new);
               return 1;
          }
          else {
               if (ts) {
		    time_t diff;
		    if (ts->sec < kdata->first_time) {
			 diff = 0;
		    } 
		    else {
			 diff = ts->sec - kdata->first_time;
		    }
                    if (diff <= proc->recent) {
                         tuple_add_member_label(tdata, member, proc->label_new);
                         stringhash5_unlock(proc->key_table);
                         return 1;
                    }
                    if (diff > proc->old) {
                         stringhash9a_set(proc->old_table, tag,
                                          sizeof(wsdt_uint64_t));
                         stringhash5_delete(proc->key_table, tag,
                                            sizeof(wsdt_uint64_t));
                         stringhash5_unlock(proc->key_table);
                         tuple_add_member_label(tdata, member, proc->label_old);
                         return 2;
                    }
               }
               stringhash5_unlock(proc->key_table);
               tuple_add_member_label(tdata, member, proc->label_recent);
               return 1;
          }
          stringhash5_unlock(proc->key_table);
     }

     return 0;
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
     wsdt_ts_t * ts = NULL;
     wsdata_t ** mset;
     int mset_len;
     int i, j;
     if (tuple_find_label(input_data, proc->label_date,
                          &mset_len, &mset)) {
          for (i = 0; i < mset_len; i++) {
               if (mset[i]->dtype == dtype_ts) {
                    ts = (wsdt_ts_t*)mset[i]->data;
               }
          }
     }
     for (i = 0; i < proc->lset.len; i++) {
          if (tuple_find_label(input_data, proc->lset.labels[i],
                               &mset_len, &mset)) {
               for (j = 0; j < mset_len; j++ ) {
                    add_member(proc, input_data, mset[j], ts);
               }
          }
     }

     //always pass thru..
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;
     return 1;
}

static inline void proc_dump_existence_table(proc_instance_t * proc) {
     if (proc->outfile && (!proc->sharelabel || !proc->sharer_id)) {
          tool_print("dumping existence table to file %s", proc->outfile);
          FILE * fp = fopen(proc->outfile, "w");
          if (fp) {
               stringhash9a_dump(proc->old_table, fp);
               fclose(fp);
          }
          else {
               perror("failed writing existence table");
               tool_print("unable to write to file %s", proc->outfile);
          }
     }
}

static void serialize_table(proc_instance_t * proc) {
     if (proc->outfile5 && (!proc->sharelabel5 || !proc->sharer_id5)) {
	  tool_print("Writing data table to %s", proc->outfile5);
          FILE * fp = fopen(proc->outfile5, "w");
	  if (fp) {
	       stringhash5_dump(proc->key_table, fp);
	       fclose(fp);
	  }
          else {
               perror("failed writing data table");
               tool_print("unable to write to file %s", proc->outfile5);
          }
     }
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;

     serialize_table(proc);
     stringhash5_scour_and_flush(proc->key_table, last_destroy, proc);

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy tables
     stringhash5_destroy(proc->key_table);

     proc_dump_existence_table(proc);
     stringhash9a_destroy(proc->old_table);

     //free dynamic allocations
     if (proc->sharelabel5) {
          free(proc->sharelabel5);
     }
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     if (proc->outfile5) {
          free(proc->outfile5);
     }
     if (proc->outfile) {
          free(proc->outfile);
     }
     free(proc);

     return 1;
}

