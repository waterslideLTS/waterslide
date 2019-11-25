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

//a timed-expiration version of uniq

#define PROC_NAME "uniqexpire"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint64.h"
#include "stringhash5.h"
#include "procloader.h"
#include "sysutil.h"
#include "wstypes.h"

char proc_version[]     = "1.1";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "expireuniq", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "finds uniq records based on hash, expire based on timeout";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'F',"","filename",
     "load existing database (uniq table) from file (expire timers are not reset)",0,0},
     {'O',"","filename",
     "write database (uniq table) to file",0,0},
     {'t',"","duration",
     "expire time in seconds (or use m for minutes, h for hours)",0,0},
     {'L',"","label",
     "use labeled tuple hash",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABEL of tuple subitem";
char *proc_input_types[]    = {"tuple", "any", NULL};
char *proc_output_types[]    = {"any", NULL};
proc_port_t proc_input_ports[] = {
     {"none","Check & pass if unique"},
     {"DELETE","remove key from list of seen items"},
     {"REMOVE","remove key from list of seen items"},
     {NULL, NULL}
};

#define LOCAL_MAX_SH5_TABLE 100000
#define LOCAL_MAX_TYPES 25

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int process_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int removefrom_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int removefrom_meta(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _uniq_data_t {
     time_t sec;
} uniq_data_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t output_cnt;

     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];
     stringhash5_t * uniq_table;
     uint32_t max_uniq_table;
     wslabel_t * hash_label;
     time_t expire_time;
     char * outfile;
     char * open_table;

     char * sharelabel;
     int sharer_id;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:O:F:t:L:M:")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'O':
               proc->outfile = strdup(optarg);
               break;
          case 'F':
               proc->open_table = strdup(optarg);
               // loading of the table is postponed to the stringhash5_create
               // call in proc_init
               break;
          case 't':
               proc->expire_time = sysutil_get_duration_ts(optarg);
               tool_print("expire every %d seconds", (int)proc->expire_time);
               break;
          case 'L':
               proc->hash_label = wssearch_label(type_table, optarg); 
               break;
          case 'M':
               proc->max_uniq_table = atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          proc->hash_label = wssearch_label(type_table, argv[optind]);
          tool_print("filtering on %s", argv[optind]);
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

     proc->max_uniq_table = LOCAL_MAX_SH5_TABLE;
     proc->expire_time = 600;  //10 minutes

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

          ret = stringhash5_create_shared_sht(type_table, (void **)&proc->uniq_table, 
                                              proc->sharelabel, proc->max_uniq_table, 
                                              sizeof(uniq_data_t), &proc->sharer_id, 
                                              sh5_sh_opts);

          if (!ret) return 0;
     }
     else {
          // read the stringhash5 table from the open_table file
          uint32_t ret = 0;
          if (proc->open_table) {
               ret = stringhash5_open_sht_table(&proc->uniq_table, proc, 
                                                proc->max_uniq_table, sizeof(uniq_data_t), 
                                                sh5_sh_opts); 
          }
          // create the stringhash5 table from scratch
          if (!ret) {
               proc->uniq_table = stringhash5_create(0, proc->max_uniq_table, 
                                                     sizeof(uniq_data_t));
               if (!proc->uniq_table) {
                    return 0;
               }
          }
     }

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //use the stringhash5-adjusted value of max_records to reset max_uniq_table
     proc->max_uniq_table = proc->uniq_table->max_records;

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
          tool_print("trying to register flusher type");
          return NULL;
     }
     //register output..
     // pass the input type to the output..
     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     if (wslabel_match(type_table, port, "DELETE") ||
         wslabel_match(type_table, port, "REMOVE")) {
          if (proc->hash_label &&
              wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               return removefrom_labeled_tuple; 
          }
          return removefrom_meta;
     }

     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type, NULL);

     if (proc->hash_label &&
         wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
         return process_labeled_tuple; 
     }

     // we are happy.. now set the processor function
     return proc_process_meta; // a function pointer
}

static void serialize_table(proc_instance_t* proc) {
     if (proc->outfile && (!proc->sharelabel || !proc->sharer_id)) {
          tool_print("Writing uniq table to %s", proc->outfile);
          FILE * fp = fopen(proc->outfile, "w");
	  if (fp) {
	       stringhash5_dump(proc->uniq_table, fp);
	       fclose(fp);
	  }
          else {
               perror("failed writing uniq table");
               tool_print("unable to write to file %s", proc->outfile);
          }
     }
}

static inline int check_hash(proc_instance_t * proc, uint8_t * key, int keylen,
                             time_t sec) {
    if (keylen == 0) return 0;

    uniq_data_t * ud = stringhash5_find_attach(proc->uniq_table, key, keylen);
    if (!ud->sec || (ud->sec < sec)) {
         ud->sec = sec + proc->expire_time;
         stringhash5_unlock(proc->uniq_table);
         return 1;
    }
    stringhash5_unlock(proc->uniq_table);

    return 0;
}

static inline void remove_hash(proc_instance_t * proc,
                              uint8_t * key, int keylen) {
    if (keylen >= 0) {
         stringhash5_delete(proc->uniq_table,
                         key, keylen);
    }
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     ws_hashloc_t* hashloc = input_data->dtype->hash_func(input_data);
     
     if (!check_hash(proc,
                     (uint8_t*)hashloc->offset,
                     hashloc->len,
                     time(NULL))) {
          return 0;
     }

     // this is new data.. pass as output
     proc->output_cnt++;
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_labeled_tuple(void * vinstance, wsdata_t* input_data,
                                 ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     wsdt_uint64_t * hashtag = NULL;
     wsdt_ts_t * ts = NULL;
     wsdt_tuple_t * tuple = input_data->data;
     int i;

     for (i = 0; i < tuple->len; i++) {
          if (tuple->member[i]->dtype == dtype_ts) {
               ts = tuple->member[i]->data;
               break;
          }
     }

     //search backwards in tuple for hashlabel
     wsdata_t * tag_member = NULL;
     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->hash_label,
                          &mset_len, &mset)) {
          tag_member = mset[0];
     }
     if (!tag_member) {
          return 0;
     }
     if (tag_member->dtype == dtype_uint64) {
          hashtag = (wsdt_uint64_t*)tag_member->data;

          if (!hashtag || !check_hash(proc, 
                                      (uint8_t*)hashtag,
                                      sizeof(wsdt_uint64_t),
                                      ts ? ts->sec : time(NULL))) {
               // we got a duplicate ... no output
               return 0;
          }
     }
     else {
          ws_hashloc_t* hashloc = tag_member->dtype->hash_func(tag_member);
          if (!hashloc || !check_hash(proc,
                                      (uint8_t*)hashloc->offset,
                                      hashloc->len,
                                      ts ? ts->sec : time(NULL))) {
               return 0;
          }
     }

     // this is new data.. pass as output
     proc->output_cnt++;
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
static int removefrom_meta(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     ws_hashloc_t* hashloc = input_data->dtype->hash_func(input_data);
     
     if (!hashloc) {
          remove_hash(proc,
                      (uint8_t*)hashloc->offset,
                      hashloc->len);
     }

     return 0;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int removefrom_labeled_tuple(void * vinstance, wsdata_t* input_data,
                                    ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     wsdt_uint64_t * hashtag = NULL;

     //search backwards in tuple for hashlabel
     wsdata_t * tag_member = NULL;
     wsdata_t ** mset;
     int mset_len;
     if (tuple_find_label(input_data, proc->hash_label,
                          &mset_len, &mset)) {
          tag_member = mset[0];
     }
     if (!tag_member) {
          return 0;
     }
     if (tag_member->dtype == dtype_uint64) {
          hashtag = (wsdt_uint64_t*)tag_member->data;

          if (!hashtag) {
               remove_hash(proc, 
                           (uint8_t*)hashtag,
                           sizeof(wsdt_uint64_t));
          }
     }
     else {
          ws_hashloc_t* hashloc = tag_member->dtype->hash_func(tag_member);
          if (!hashloc) {
               remove_hash(proc,
                           (uint8_t*)hashloc->offset,
                           hashloc->len);
          }
     }

     return 0;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->output_cnt);

     //destroy table
     serialize_table(proc);
     stringhash5_destroy(proc->uniq_table);

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

