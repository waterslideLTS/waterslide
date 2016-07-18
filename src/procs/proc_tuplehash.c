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

#define PROC_NAME "tuplehash"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "wsbase64.h"
#include "wstypes.h"
#include "evahash64_data.h"
#include "datatypes/wsdt_tuple.h"

char proc_version[]     = "1.5";
char *proc_menus[]     = { "Filters", NULL };
char *proc_tags[]     = { "Filters", "Annotation", NULL };
char *proc_alias[]     = { "hashtuple", "tuplehasher", "metatuplehash", "thash", NULL};
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Create a new label containing a hash of the contents of other labels";
char *proc_synopsis[] = {"tuplehash <LABELS_TO_HASH> [-L <NEWLABEL>] [-o] [-k <key>] [-B] [-1]", NULL};
proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'o',"","",
     "compute ordered hash",0,0},
     {'L',"","label",
     "label for hash value",0,0},
     {'k',"","key",
      "key for keyed hash",0,0},
     {'B',"","",
      "output base64 key",0,0},
     {'I',"","",
      "output (long) integer key",0,0},
     {'1',"","",
     "only take the first element at any label",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_description[] = "Creates a hash from an input list of labels and appends the new hash value as a new label to the tuple.  The '-L' option is used to specify the new label that is appended to the tuple; if the '-L' option is not specified the label, TUPLEHASH, is used.  Other options are also available to order the hash (the '-o' option), to specify the key value to use when generating the hash (the '-k' option), to limit the number of labels included in the hash if multiple identical labels exist (the '-1' option), and the '-B' option can be used to generate hash values as base64 strings instead of numeric values.";

proc_example_t proc_examples[] = {
	{"... | tuplehash LABEL1 LABEL2 | ...", "append a tuple member with the label 'TUPLEHASH' containing the value of the hash of LABEL1 and LABEL2"},
	{"... | tuplehash LABEL1 LABEL2 LABEL3 -L NEWHASHLABEL | ...", "append a tuple member with the label 'NEWHASHLABEL' containing the value of the hash of LABEL1, LABEL2 and LABEL3"},
	{"... | tuplehash -B LABEL1 LABEL2 | ...", "append a tuple member with the label 'TUPLEHASH' containing the base64 value of the hash of LABEL1 and LABEL2"},
	{"... | tuplehash -o LABEL1 LABEL2 | ...", "append a tuple member with the label 'TUPLEHASH' containing the value of the ordered hash of LABEL1 and LABEL2"},
	{NULL, ""}
};

char proc_nonswitch_opts[]    = "list of LABELS to hash";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char proc_requires[] = "";
proc_port_t proc_input_ports[] = {{NULL, NULL}};
char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {"TUPLEHASH", NULL};

//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     int ordered_hash;
     uint32_t hash_key;
     wslabel_t * label_hash;
     int first_el;
     int base64key;
     int longkeyoutput;
     wslabel_nested_set_ext_t nest;
     int label_cnt;
     uint64_t bit_elements; //for first_el
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "BI1oL:k:x:")) != EOF) {
          switch (op) {
          case 'B':
               proc->base64key = 1;
               break;
          case 'I':
               proc->longkeyoutput = 1;
               break;
          case '1':
               proc->first_el = 1;
               break;
          case 'L':
               proc->label_hash = wsregister_label(type_table, optarg);
               tool_print("output hash label is %s", optarg);
               break;
          case 'o':
               tool_print("ordered hash");
               proc->ordered_hash = 1;
               break;
          case 'k':
               tool_print("keyed hash");
               proc->hash_key = atoi(optarg);
               if (proc->hash_key <= 0) {
                    error_print("please use a positive key");
                    return 0;
               }
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build_ext(type_table, &proc->nest, argv[optind],
                                          proc->label_cnt);
          tool_print("searching for string with label %s", argv[optind]);
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

     proc->hash_key = TUPLE_DEFAULT_HASHKEY;
     proc->label_hash = wsregister_label(type_table, "TUPLEHASH");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
   
     if (!proc->nest.cnt) {
          tool_print("must specify at least something to hash");
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
     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          proc->outtype_tuple = ws_add_outtype(olist, meta_type, NULL);
          return process_tuple;
     }
     return NULL;
}

static inline void set_base64_key(wsdata_t * tdata, uint64_t hash,
                                  wslabel_t * label) {
     wsdt_string_t * str = tuple_create_string(tdata, label, 12);

     if (!str) {
          return;
     }

     if (wsbase64_encode((uint8_t *)&hash, 8, (uint8_t *)str->buf, 12) != 12) {
          tool_print("error in base64 encoding");
     }
     str->len = 11;
}

static inline int not_first_el(proc_instance_t * proc, int id) {
     if (id >= 64) {
          return 0;
     }
     uint64_t mask = 0x01 << id;
     if (proc->bit_elements & mask) {
          return 1;
     }
     else {
          proc->bit_elements |= mask;
          return 0;
     }
} 

//used to hash all members of a tuple
static uint64_t local_hash_tuple(proc_instance_t * proc, wsdata_t * tdata, uint32_t offset) {
     uint64_t hash = 0;
     wsdt_tuple_t * tuple = (wsdt_tuple_t *)tdata->data;

     int len = tuple->len;
     wsdata_t * member;
     int i;
     for (i = 0; i < len; i++) {
          member = tuple->member[i];
          if (member->dtype == dtype_tuple) {
               //recursive tuple traversal
               hash += local_hash_tuple(proc, member, offset);
          }
          else {
               hash += evahash64_data(member, proc->hash_key + offset);
          }
     }
     
     return hash; 
}

static int proc_nest_hash_element(void * vinstance, void * vhash,
                                  wsdata_t * tdata, wsdata_t * attr,
                                  wslabel_t * label, int id) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     uint64_t * phash = (uint64_t *)vhash;

     //hash key with attribute (directional)
     // then look up in hashtable to see if pair exists

     uint32_t offset = 0;
     if (proc->ordered_hash) {
          offset = id;
     }
 
     if (attr->dtype == dtype_tuple) {
          if (proc->first_el && not_first_el(proc, id)) {
               return 0;
          }
          (*phash) += local_hash_tuple(proc, attr, offset);
          return 1;
     }

    
     if (proc->first_el && not_first_el(proc, id)) {
          return 0;
     }
     (*phash) += evahash64_data(attr, proc->hash_key + offset);

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     //allocate a hash variable..
     uint64_t hash = 0;

     proc->bit_elements = 0;

     //search for labels in tuple
     int found = tuple_nested_search_ext(input_data, &proc->nest,
                                         proc_nest_hash_element,
                                         proc, &hash);

     if (found) {
          if (proc->longkeyoutput) {
               tuple_member_create_uint64(input_data, hash, proc->label_hash);
          } else {
               if (proc->base64key) {
                    set_base64_key(input_data, hash, proc->label_hash);
               } else {
                    tuple_member_create_uint64(input_data, hash, proc->label_hash);
               }
          }
     }

     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free dynamic allocations
     free(proc);

     return 1;
}

