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

#ifndef OWMR_TABLES
#define PROC_NAME "uniq"
#else
#define PROC_NAME "uniq_owmr"
#endif // OWMR_TABLES

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "stringhash9a.h"
#include "evahash64_data.h"
#include "procloader.h"
#include "sysutil.h"

char proc_version[]     = "1.5";
char *proc_menus[]     = { "Filters", NULL };
char *proc_tags[]     = { "Filtering", "State Tracking", "Detection", "Matching", NULL };
#ifndef OWMR_TABLES
char *proc_alias[]     = { "unique", "uniq_shared", "unique_shared", NULL };
#else
char *proc_alias[]     = { "uniq_owmr_shared", NULL };
#endif // OWMR_TABLES
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "provides access to a state table that can be used to filter for existence, uniqueness, and duplication. ";
char *proc_synopsis[] = {"uniq [-J <SHT9A LABEL>] <LIST_OF_LABELS> [-p <value>] [-M <records>] [-o] [-k <value>] [-i] [-O <filename>] [-L <filename>] [-T <LABEL>] [-S <string>]", NULL};
char proc_description[] = "The uniq kid stores a state table that can be used to "
     "filter stream events for existence, uniqueness, and duplication of records "
     "based on a given key or keys.\n"
     "\n"
     "A maximum table size can be suggested by the user (with the -M option) to "
     "determine the number of unique keys that will be tracked. The uniq kid employs "
     "a stringhash9a table. Given a suggested maximum number of records to track "
     "(x), the maximum table size is set to 2^(\\ceiling \\log2(x)). As the buffer "
     "is filled, keys are expred in order of those that are approximately least "
     "recently seen.";
proc_example_t proc_examples[] = {
     {"... | uniq -J SHT0 LABEL | ...", "Pass event only if a given value for LABEL has not been seen. The state table is shared among threads."},
     {"... | uniq LABEL1 LABEL2 | ...", "Pass event only if the given values for both LABEL1 and LABEL2 have not been seen."},
     {"... ; $stream1:SET, $stream2:INVQUERY | uniq LABEL | ...", "Pass stream2 events with LABEL that have already been seen in stream1"},
     {"... ; $stream1:SET, $stream2:QUERY | uniq LABEL | ...", "Pass stream2 events that have NOT been seen in stream1"},
     {"... | DUPES:uniq LABEL | ...", "Pass only events that have been seen in the stream before"},
     {"... | uniq LABEL -L <filename> -O <filename> | ...", "Load an existing uniq table from <filename>, process stream events using/amending table, then write out updates to filename upon exit"},
     {"... ; $stream1:TAG, $stream1:SET | uniq LABEL -T FOUND | ...", "Tag tuple containers with FOUND label for events with LABEL that have been seen in the stream previously"},
     {"... ; $stream1:TAG, $stream1:SET | uniq LABEL -T FOUND -S VALUE | ...", "Add a new FOUND label with value=VALUE to events with LABEL that have been seen in the stream previously."},
     {NULL, ""}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'J',"","sharename",
     "shared table with other kids",0,0},
     {'p',"","probability",
     "send duplicates every p times",0,0},
     {'M',"","records",
     "maximum number of unique keys to store (default: 5,000,000)",0,0},
     {'o',"","",
     "do ordered hashing",0,0},
     {'k',"","key",
     "hashkey",0,0},
     {'i',"","",
     "ignore flush",0,0},
     {'O',"","filename",
     "write out hashtable to file",0,0},
     {'L',"","filename",
     "load in hashtable from file",0,0},
     {'T',"","LABEL",
     "tag uniq members with label when using TAG port",0,0},
     {'S',"","string",
     "use with tag port; append string to element when found",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_requires[] = "";
char proc_nonswitch_opts[]    = "LABEL(S) to key for uniqueness";
char *proc_input_types[]    = {"tuple", "any", NULL};
char *proc_output_types[]    = {"any", NULL};
proc_port_t proc_input_ports[] = {
     {"none","check if item is in filter, set & pass if NOT already in filter (unique)"},
     {"DUPES","set item in filter, pass if already in filter (duplicate)"},
     {"TAG","pass all, tag tuple with tag specified in -T option"},
     {"QUERY","check & pass if item is NOT already in filter"},
     {"ISNOTSET", "check & pass if item is NOT already in filter"},
     {"INVQUERY","check & pass if item is already in filter"},
     {"ISSET", "check & pass if item is already in filter"},
     {"SET","set item in filter, pass no output data"},
     {"REMOVE","remove item from filter, pass no output data"},
     {NULL, NULL}
};

char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};

#define LOCAL_MAX_SH9A_TABLE 5000000
#define LOCAL_MAX_TYPES 25

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int process_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int query_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int invquery_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int dupes_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int tag_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int set_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int remove_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _hash_members_t {
     wsdata_t * member[WSDT_TUPLE_MAX];
     int id[WSDT_TUPLE_MAX];
     uint64_t hash;
} hash_members_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t iquery_cnt;
     uint64_t set_cnt;
     uint64_t outcnt;

     int ignore_flush;
     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];
     uint32_t table_size;
     stringhash9a_t * uniq_table;
     double heartbeat;
     int heartbeat_int;
     int do_heartbeat;
     wslabel_t * label_heartbeat;
     int ordered_hash;
     uint32_t hashkey;
     uint32_t count;
     hash_members_t hmembers;
     char * dump_file;
     wslabel_t * label_tag;
     wsdata_t * tstr;
     char * open_table;

     char * sharelabel;
     int sharer_id;
     
     wslabel_nested_set_ext_t nest;
     int label_cnt; 
     uint64_t * position_hash_additions;
     //wslabel_set_t lset;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "J:S:T:O:L:p:iM:k:o")) != EOF) {
          switch (op) {
          case 'J':
               proc->sharelabel = strdup(optarg);
               break;
          case 'i':
               proc->ignore_flush = 1;
               break;
          case 'O':
               proc->dump_file = strdup(optarg);
               break;
          case 'L':
               proc->open_table = strdup(optarg);
               // loading of the table is postponed to the stringhash9a_create
               // call in proc_init
               break;
          case 'o':
               proc->ordered_hash = 1;
               tool_print("ordered hashing");
               break;
          case 'k':
               proc->hashkey = atoi(optarg);
               tool_print("key set to %u", proc->hashkey);
               break;
          case 'p':
               proc->heartbeat = strtod(optarg, NULL);
               proc->heartbeat_int = (int)(proc->heartbeat * (double)RAND_MAX);
               proc->do_heartbeat = 1;
               tool_print("setting heartbeat %f %d", proc->heartbeat,
                          proc->heartbeat_int);
               break;
          case 'M':
               proc->table_size = atoi(optarg);
               tool_print("table size %u", proc->table_size);
               break;
          case 'T':
               proc->label_tag = wsregister_label(type_table, optarg);
               break;
          case 'S':
               proc->tstr = wsdata_create_string(optarg, strlen(optarg));
               if (proc->tstr) {
                    wsdata_add_reference(proc->tstr);
               }
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build_ext(type_table, &proc->nest, argv[optind],
                                          proc->label_cnt);
          tool_print("filtering on %s", argv[optind]);
          proc->label_cnt++;
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

     proc->table_size = LOCAL_MAX_SH9A_TABLE;
     proc->label_heartbeat = wsregister_label(type_table, "HEARTBEAT");
     proc->hashkey = 0x34552FE1;

     proc->label_tag = wsregister_label(type_table, "UNIQ");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     //preallocate hashing offsets for ordered hashing
     if (proc->ordered_hash && proc->label_cnt) {
          proc->position_hash_additions = calloc(proc->label_cnt,
                                                 sizeof(uint64_t));

          if (!proc->position_hash_additions) {
               tool_print("unable to allocate hash offsets");
               return 0;
          }

          int i;
          for (i = 0; i < proc->label_cnt; i++) {
               uint64_t ref = i + 0x55336677;
               proc->position_hash_additions[i] = evahash64((uint8_t*)&ref, sizeof(uint64_t),
                                                            proc->hashkey);
          }
     }
     if (proc->tstr && proc->label_tag) {
          wsdata_add_label(proc->tstr, proc->label_tag);
     }

     //other init - init the stringhash table
     stringhash9a_sh_opts_t * sh9a_sh_opts;

     //calloc shared sh9a option struct
     stringhash9a_sh_opts_alloc(&sh9a_sh_opts);

     //set shared sh9a option fields
     sh9a_sh_opts->open_table = proc->open_table;

     if (proc->sharelabel) {
          if (!stringhash9a_create_shared_sht(type_table, (void **)&proc->uniq_table, 
                                          proc->sharelabel, proc->table_size, 
                                          &proc->sharer_id, sh9a_sh_opts)) {
               return 0;
          }
     }
     else {
          // read the stringhash9a table from the open_table file
          uint32_t ret = 0;
          if (proc->open_table) {
               ret = stringhash9a_open_sht_table(&proc->uniq_table, 
                                                 proc->table_size, sh9a_sh_opts);
          }
          // create the stringhash9a table from scratch
          if (!ret) {
               proc->uniq_table = stringhash9a_create(0, proc->table_size);
               if (NULL == proc->uniq_table) {
                    tool_print("unable to create a proper stringhash9a table");
                    return 0;
               }
          }
     }

     //free shared sh9a option struct
     stringhash9a_sh_opts_free(sh9a_sh_opts);

     //use the stringhash9a-adjusted value of max_records to reset table_size
     proc->table_size = proc->uniq_table->max_records;

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
          return proc_flush;
     }
     //register output..
     // pass the input type to the output..
     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type, NULL);

     if (wslabel_match(type_table, port, "SET")) {
          if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               tool_print("doing set processing");
               return set_labeled_tuple; 
          }
          return NULL;
     }
     if (wslabel_match(type_table, port, "REMOVE")) {
          if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               return remove_labeled_tuple; 
          }
          return NULL;
     }

     if (wslabel_match(type_table, port, "QUERY")) {
          if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               return query_labeled_tuple; 
          }
          return NULL;
     }

     if (wslabel_match(type_table, port, "ISNOTSET")) {
          if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               return query_labeled_tuple; 
          }
          return NULL;
     }

     if (wslabel_match(type_table, port, "INVQUERY")) {
          if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               tool_print("doing invquery processing");
               return invquery_labeled_tuple; 
          }
          return NULL;
     }

     if (wslabel_match(type_table, port, "ISSET")) {
          if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               tool_print("doing invquery processing");
               return invquery_labeled_tuple; 
          }
          return NULL;
     }

     if (wslabel_match(type_table, port, "DUPES")) {
          if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               return dupes_labeled_tuple; 
          }
          return NULL;
     }


     if (wslabel_match(type_table, port, "TAG")) {
          if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               return tag_labeled_tuple; 
          }
          return NULL;
     }

     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          return process_labeled_tuple; 
     }

     // we are happy.. now set the processor function
     return proc_process_meta; // a function pointer
}

//return 0 if data should be dropped, return 1 if we have heartbeat
static inline int check_heartbeat(proc_instance_t * proc, wsdata_t * input_data) {
     if (proc->do_heartbeat && (rand() <= proc->heartbeat_int)) {
          wsdata_add_label(input_data, proc->label_heartbeat);
          return 1;
     }
     else {
          return 0;
     }
}

static inline int hash_exists(proc_instance_t * proc,
                              void * key, int keylen, wsdata_t * labelme) {

     if (keylen == 0) {
          return 0;
     }
     if (stringhash9a_set(proc->uniq_table,
                          (uint8_t*)key, keylen) == 1) { 
          if (!check_heartbeat(proc, labelme)) {
               // we got a duplicate ... no output
               return 1;
          }
     }
     return 0;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     ws_hashloc_t* hashloc = input_data->dtype->hash_func(input_data);
    
     if (hash_exists(proc, hashloc->offset, hashloc->len, input_data)) {
          return 0;
     } 

     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     proc->outcnt++;
     return 1;
}

//used to hash all members of a tuple
static uint64_t local_hash_tuple(proc_instance_t * proc, wsdata_t * tdata) {
     uint64_t hash = 0;
     wsdt_tuple_t * tuple = (wsdt_tuple_t *)tdata->data;

     int len = tuple->len;
     wsdata_t * member;
     int i;
     for (i = 0; i < len; i++) {
          member = tuple->member[i];
          if (member->dtype == dtype_tuple) {
               //recursive tuple traversal
               hash += local_hash_tuple(proc, member);
          }
          else {
               hash += evahash64_data(member, proc->hashkey);
          }
     }

     return hash;
}


static int proc_nest_hash_element(void * vinstance, void * vhash,
                                  wsdata_t * tdata, wsdata_t * attr,
                                  wslabel_t * label, int id) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     uint64_t * phash = (uint64_t *)vhash;

     uint64_t whash = 0;


     if (attr->dtype == dtype_tuple) {
          whash = local_hash_tuple(proc, attr);
     }
     else {
          whash = evahash64_data(attr, proc->hashkey);
     }

     if (proc->ordered_hash) {
          whash = whash ^ proc->position_hash_additions[id];
     }

     (*phash) = (*phash) + whash;
     return 1;
}


static inline int get_hashdata(proc_instance_t * proc, wsdata_t * tuple,
                               uint64_t * hash) {

     int found = tuple_nested_search_ext(tuple, &proc->nest,
                                         proc_nest_hash_element,
                                         proc, hash);

     if (found) {
          dprint("found %d elements, hash %"PRIu64, found, *hash);
     }
     return found;
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int process_labeled_tuple(void * vinstance, wsdata_t* input_data,
                                 ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     uint64_t hash = 0;

     if (!get_hashdata(proc, input_data, &hash) ||
         hash_exists(proc, &hash, sizeof(uint64_t), input_data)) {  
          return 0;
     }

     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     proc->outcnt++;
     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int set_labeled_tuple(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     uint64_t hash = 0;

     if (get_hashdata(proc, input_data, &hash)) {
          proc->set_cnt++;
          stringhash9a_set(proc->uniq_table, &hash, sizeof(uint64_t));
     }
     return 0;
}

static int remove_labeled_tuple(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     uint64_t hash = 0;

     if (get_hashdata(proc, input_data, &hash)) {
          stringhash9a_delete(proc->uniq_table, &hash, sizeof(uint64_t));
     }
     return 0;
}

static int query_labeled_tuple(void * vinstance, wsdata_t* input_data,
                                 ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     
     uint64_t hash = 0;

     if (!get_hashdata(proc, input_data, &hash)) {
          return 0;
     }

     if (stringhash9a_check(proc->uniq_table,
                           &hash, sizeof(uint64_t))) {
          return 0;
     }

     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     proc->outcnt++;
     return 1;
}


static int invquery_labeled_tuple(void * vinstance, wsdata_t* input_data,
                                 ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     uint64_t hash = 0;

     if (!get_hashdata(proc, input_data, &hash)) {
          return 0;
     }

     proc->iquery_cnt++;
     if (!stringhash9a_check(proc->uniq_table,
                             &hash, sizeof(uint64_t))) {
          return 0;
     }

     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     proc->outcnt++;
     return 1;
}

static int dupes_labeled_tuple(void * vinstance, wsdata_t* input_data,
                                 ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     
     uint64_t hash = 0;

     if (!get_hashdata(proc, input_data, &hash)) {
          return 0;
     }

     if (stringhash9a_set(proc->uniq_table,
                          &hash, sizeof(uint64_t))) {
          ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
          proc->outcnt++;
          return 1;
     }

     return 0;
}

static int tag_labeled_tuple(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;
     
     uint64_t hash = 0;

     if (get_hashdata(proc, input_data, &hash)) {

          if (stringhash9a_check(proc->uniq_table,
                                 &hash, sizeof(uint64_t))) {

               if (proc->tstr) {
                    add_tuple_member(input_data, proc->tstr);
               }
               else {
                    wsdata_add_label(input_data, proc->label_tag);
               }
          }
     }

     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     proc->outcnt++;

     return 0;
}

static inline void proc_dump_existence_table(proc_instance_t * proc) {
     if (proc->dump_file) {
          tool_print("Writing uniq table to %s", proc->dump_file);
          FILE * fp = fopen(proc->dump_file, "w");
          if (fp) {
               stringhash9a_dump(proc->uniq_table, fp);
               fclose(fp);
          }
          else {
               perror("failed writing uniq table");
               tool_print("unable to write to file %s", proc->dump_file);
          }
     }
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     // This flush is serialized for the stringhash9a_dump and stringhash9a_flush. 
     if (!proc->sharelabel || !proc->sharer_id) {
          //dump file if specified.
          dprint("flushing state table");
          if (proc->uniq_table->drops) {
               tool_print("state table drop cnt %" PRIu64, proc->uniq_table->drops);
               proc->uniq_table->drops = 0;
          }
          proc_dump_existence_table(proc);

          if (!dtype_is_exit_flush(input_data)) {
               if (!proc->ignore_flush) {
                    stringhash9a_flush(proc->uniq_table);
               }
          }
          else {
               dprint("flushing on exit");
               if (proc->dump_file) {
                    free(proc->dump_file);
               }
               proc->dump_file = NULL;
          }

          if (proc->meta_process_cnt) {
               tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
               tool_print("output cnt %" PRIu64, proc->outcnt);
               tool_print("output percentage %.2f%%",
                          (double)proc->outcnt * 100 /
                          (double)proc->meta_process_cnt);
               if (proc->iquery_cnt) {
                    tool_print("iquery cnt %" PRIu64, proc->iquery_cnt);
               }
               if (proc->set_cnt) {
                    tool_print("set cnt %" PRIu64, proc->set_cnt);
               }
          }
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     if (proc->tstr) {
          wsdata_delete(proc->tstr);
     }
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     if (proc->meta_process_cnt) {
          tool_print("output cnt %" PRIu64, proc->outcnt);
          tool_print("output percentage %.2f%%",
                     (double)proc->outcnt * 100 /
                     (double)proc->meta_process_cnt);
          if (proc->iquery_cnt) {
               tool_print("iquery cnt %" PRIu64, proc->iquery_cnt);
          }
          if (proc->set_cnt) {
               tool_print("set cnt %" PRIu64, proc->set_cnt);
          }
     }

     //destroy table
     stringhash9a_destroy(proc->uniq_table);

     //free dynamic allocations
     if (proc->sharelabel) {
          free(proc->sharelabel);
     }
     if (proc->dump_file) {
          free(proc->dump_file);
     }
     if (proc->position_hash_additions) {
          free(proc->position_hash_additions);
     }
     free(proc);

     return 1;
}

