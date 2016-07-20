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

#define PROC_NAME "bloom"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint64.h"
#include "bloomfilter.h"
#include "procloader.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
// Use to fix proc_tags: 
//char *proc_menus[]             =  { "Filters", NULL };
char *proc_tags[]              =  {"Filtering", "State tracking", "Detection", "Matching", NULL};
char *proc_alias[]             =  { "bloomuniq", NULL };
char proc_purpose[]            =  "Finds unique records using a bloom filter";
char proc_description[] = "Find unique records using a bloom filter (functionality is similar to 'uniq' kid).  Useful for filtering stream events based on existence, uniqueness, and duplication of records of a given key or keys.  Options are available for emitting unique values with a specified probability (-p), specifying the number of hash rounds used in the bloom filter (-R, default value is 7), loading or writing the bloom filter to a file (-F and -O), and the number of bits used in calculating existence within the bloom filter (-M; default size is 29).";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'p',"","probability",
     "send duplicates every p times",0,0},
     {'L',"","label",
     "use labeled tuple hash",0,0},
     {'M',"","bits",
     "bloom filter bit size",0,0},
     {'R',"","rounds",
     "bloom filter hash rounds",0,0},
     {'O',"","filename",
     "output bloom filter to file at exit",0,0},
     {'F',"","filename",
     "input bloom filter from file",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]     =  "LABEL(s) to key for uniqueness";
char *proc_input_types[]       =  {"tuple", "any", NULL};
// (Potential) Output types: meta[LOCAL_MAX_TYPES]
char *proc_output_types[]      =  {"any", NULL};
char proc_requires[]           =  "None";
// Ports: QUERY, INVQUERY
proc_port_t proc_input_ports[] =  {
     {"none","check and set item is in bloom filter, pass if not set"},
     {"QUERY","check and pass if item is already in bloom filter"},
     {"INVQUERY","check and do NOT pass if item is already in bloom filter"},
     {NULL, NULL}
};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[] =  {"bloom [-L] <LABEL> [-p <value>] [-M <bytes>] [-R <value>] [-O <filename>] [-F <filename>]", NULL};
proc_example_t proc_examples[] =  {
	{"... | bloom -L LABEL | ...", "Pass event only if a given value for LABEL has not been seen."},
	{"... | bloom -L LABEL -O filename.bloom -F filename.bloom | ...", "Pass event only if a given value for LABEL has not been seen; prior to run, load existing bloom filter from filename.bloom and write out updates to bloom filter to filename.bloom at exit."},
	{NULL, NULL}
};

#define LOCAL_BLOOM_BITS 29
#define LOCAL_BLOOM_ROUNDS 7
#define LOCAL_MAX_TYPES 25

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int process_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int query_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int invquery_labeled_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];
     bloomfilter_t * uniq_table;
     uint32_t bloom_bits;
     uint32_t bloom_rounds;
     wslabel_t * hash_label;
     double heartbeat;
     int heartbeat_int;
     int do_heartbeat;
     wslabel_t * label_heartbeat;
     char * outfilename;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "p:L:M:R:F:O:")) != EOF) {
          switch (op) {
          case 'p':
               proc->heartbeat = strtod(optarg, NULL);
               proc->heartbeat_int = (int)(proc->heartbeat * (double)RAND_MAX);
               proc->do_heartbeat = 1;
               tool_print("setting heartbeat %f %d", proc->heartbeat, proc->heartbeat_int);
               break;
          case 'L':
               proc->hash_label = wssearch_label(type_table, optarg); 
               break;
          case 'M':
               proc->bloom_bits = atoi(optarg);
               tool_print("bloom filter bits %u", proc->bloom_bits);
               break;
          case 'R':
               proc->bloom_rounds = atoi(optarg);
               tool_print("bloom filter rounds %u", proc->bloom_rounds);
               break;
          case 'F':
               proc->uniq_table = bloomfilter_import(optarg);
               if (!proc->uniq_table) {
                    error_print("unable to open bloom filter file");
                    return 0;
               }
               break;
          case 'O':
               proc->outfilename = strdup(optarg);
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
     proc->bloom_bits = LOCAL_BLOOM_BITS;
     proc->bloom_rounds = LOCAL_BLOOM_ROUNDS;
     proc->label_heartbeat = wsregister_label(type_table, "HEARTBEAT");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     
     //other init 
     if (!proc->uniq_table) {
          proc->uniq_table = bloomfilter_init(proc->bloom_rounds, proc->bloom_bits);
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

     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          tool_print("does not process flusher type");
          return NULL;
     }
     //register output..
     // pass the input type to the output..
     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type, NULL);

     if (wslabel_match(type_table, port, "QUERY")) {
          if (proc->hash_label &&
              wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               return query_labeled_tuple; 
          }
          return NULL;
     }

     if (wslabel_match(type_table, port, "INVQUERY")) {
          if (proc->hash_label &&
              wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               return invquery_labeled_tuple; 
          }
          return NULL;
     }


     if (proc->hash_label &&
         wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
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

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     ws_hashloc_t* hashloc = input_data->dtype->hash_func(input_data);
     
     if ((hashloc->len == 0) || 
         (bloomfilter_set(proc->uniq_table,
                          (uint8_t*)hashloc->offset,
                          hashloc->len) &&
          !check_heartbeat(proc, input_data))) {
          // we got a duplicate ... no output
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
static int process_labeled_tuple(void * vinstance, wsdata_t* input_data,
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

          if (!hashtag || 
              (bloomfilter_set(proc->uniq_table,
                               (uint8_t*)hashtag,
                               sizeof(wsdt_uint64_t)) &&
               !check_heartbeat(proc, input_data))) {
               // we got a duplicate ... no output
               return 0;
          }
     }
     else {
          ws_hashloc_t* hashloc = tag_member->dtype->hash_func(tag_member);
          if (!hashloc || (hashloc->len == 0) || 
              (bloomfilter_set(proc->uniq_table,
                               (uint8_t*)hashloc->offset,
                               hashloc->len) && 
               !check_heartbeat(proc, input_data))) {
               return 0;
          }
     }

     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     proc->outcnt++;
     return 1;
}


// true QUERY
static int query_labeled_tuple(void * vinstance, wsdata_t* input_data,
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

          if (!hashtag || 
              (!bloomfilter_query(proc->uniq_table,
                                 (uint8_t*)hashtag,
                                 sizeof(wsdt_uint64_t)) &&
               !check_heartbeat(proc, input_data))) {
               // we got a duplicate ... no output
               return 0;
          }
     }
     else {
          ws_hashloc_t* hashloc = tag_member->dtype->hash_func(tag_member);
          if (!hashloc || (hashloc->len == 0) || 
              (!bloomfilter_query(proc->uniq_table,
                                 (uint8_t*)hashloc->offset,
                                 hashloc->len) && 
               !check_heartbeat(proc, input_data))) {
               return 0;
          }
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

          if (!hashtag || 
              (bloomfilter_query(proc->uniq_table,
                                 (uint8_t*)hashtag,
                                 sizeof(wsdt_uint64_t)) &&
               !check_heartbeat(proc, input_data))) {
               // we got a duplicate ... no output
               return 0;
          }
     }
     else {
          ws_hashloc_t* hashloc = tag_member->dtype->hash_func(tag_member);
          if (!hashloc || (hashloc->len == 0) || 
              (bloomfilter_query(proc->uniq_table,
                                 (uint8_t*)hashloc->offset,
                                 hashloc->len) && 
               !check_heartbeat(proc, input_data))) {
               return 0;
          }
     }

     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     proc->outcnt++;
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     if (proc->outfilename) {
          bloomfilter_export(proc->uniq_table, proc->outfilename);
     }
     bloomfilter_destroy(proc->uniq_table);

     //free dynamic allocations
     free(proc->outfilename);
     free(proc);

     return 1;
}

