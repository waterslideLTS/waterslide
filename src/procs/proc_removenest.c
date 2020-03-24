/*

   proc_removenest.c - creates a deep copy of a tuple, removing specified
   elements in nested structure
   
Copyright 2019 Morgan Stanley

THIS SOFTWARE IS CONTRIBUTED SUBJECT TO THE TERMS OF YOU MAY OBTAIN A COPY OF
THE LICENSE AT https://www.apache.org/licenses/LICENSE-2.0.

THIS SOFTWARE IS LICENSED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE AND ANY
WARRANTY OF NON-INFRINGEMENT, ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE. THIS SOFTWARE MAY BE REDISTRIBUTED TO OTHERS ONLY BY
EFFECTIVELY USING THIS OR ANOTHER EQUIVALENT DISCLAIMER IN ADDITION TO ANY
OTHER REQUIRED LICENSE TERMS
*/
//removed members from tuple..
#define PROC_NAME "removenest"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "waterslide_io.h"
#include "procloader.h"
#include "evahash64.h"
#include "stringhash5.h"
#include "datatypes/wsdt_tuple.h"

char proc_version[]     = "1.5";
char *proc_tags[] = {"Stream Manipulation", NULL};
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { "nestdelete", "nestremove", NULL};
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Creates a new tuple, removing specified members";
char *proc_synopsis[] = { "removenest <LIST_OF_LABELS> [-L <label>]", NULL };
char proc_description[] = "Creates a deep copy of input tuple while removing specified labeled members of the structure.";
char proc_requires[] = "";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "list of LABELS";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[] = {{NULL, NULL}};
char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};

typedef struct _key_data_t {
     uint64_t generation;
     wsdata_t * value;
} key_data_t; 

//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;
     stringhash5_t * ignore_table;
     ws_outtype_t * outtype_tuple;
     wslabel_nested_set_t nest;
     uint64_t generation;
     uint64_t generation_hash;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc,
                            void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "H")) != EOF) {
          switch (op) {
          default:
               return 0;
          }
     }

     while (optind < argc) {
          wslabel_nested_search_build(type_table, &proc->nest, argv[optind]);
          optind++;
     }

     return 1;
}

#define IGNORE_TABLE_SIZE 50000

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

     proc->ignore_table = stringhash5_create(0, IGNORE_TABLE_SIZE,
                                             sizeof(key_data_t));
     if (!proc->ignore_table) {
          tool_print("unable to create a stringhash9a table");
          return 0;
     }

     if (proc->nest.cnt == 0) {
          tool_print("must specify tuple items to remove");
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
          if (!proc->outtype_tuple) {
               proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
          }
          return process_tuple;
     }
     return NULL;
}

static inline uint64_t member_pointer_hash(proc_instance_t * proc, wsdata_t * member) {
     uint64_t mhash = evahash64((uint8_t *)&member, sizeof(wsdata_t*), 0x1235aade);
     return (mhash ^ proc->generation_hash);   //xor hashes together
}

static int nest_ignore_element(void * vproc, void * vevent,
                               wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     //add pointer member address to hashtable
     uint64_t phash = member_pointer_hash(proc, member);
     dprint("found nest element to remove %"PRIu64, phash);
     key_data_t * kdata =
          stringhash5_find_attach(proc->ignore_table, &phash, sizeof(uint64_t));
     if (!kdata) {
          return 0;
     }
     kdata->generation = proc->generation;
     kdata->value = member;
     return 1;
}

static inline int is_removed(proc_instance_t * proc, wsdata_t * member) {
     //check if pointer to member is in remove list
     uint64_t phash = member_pointer_hash(proc, member);
     key_data_t * kdata = stringhash5_find(proc->ignore_table, &phash, sizeof(uint64_t));
     if (!kdata || (kdata->generation != proc->generation) || 
         (kdata->value != member)) {
          return 0;
     }
     //dprint("checking if element is to be removed %"PRIu64 " %s", phash, rtn ?  "yes":"no");
     return 1;
}

static int tuple_removal_deep_copy(proc_instance_t * proc, wsdata_t* src, wsdata_t* dst) {
     wsdt_tuple_t * src_tuple = (wsdt_tuple_t*)src->data;
     // duplicate container label (not search member labels) ... for hierarchical/nested tuples, these
     // container labels will wind up being the member search labels for the nested subtuples
     wsdata_duplicate_labels(src, dst);
     uint32_t i;
     int len = src_tuple->len;
     for (i = 0; i < len; i++) {
          if (!is_removed(proc, src_tuple->member[i])) {
               if(src_tuple->member[i]->dtype == dtype_tuple) {
                    wsdata_t * dst_sub_tuple = wsdata_alloc(dtype_tuple);
                    if(!dst_sub_tuple) {
                         return 0;
                    }
                    // recursively copy each subtuple
                    if(!tuple_removal_deep_copy(proc, src_tuple->member[i], dst_sub_tuple)) {
                         wsdata_delete(dst_sub_tuple);
                         return 0;
                    }
                    // don't assume that add tuple member will always succeed
                    if(!add_tuple_member(dst, dst_sub_tuple)) {
                         wsdata_delete(dst_sub_tuple);
                         return 0;
                    }
               }
               else {
                    if(!add_tuple_member(dst, src_tuple->member[i])) {
                         return 0;
                    }
               }
          }
     }
     return 1;
}


//// proc processing function assigned to a specific data type in proc_io_init
static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {

     dprint("removenest process_tuple start");
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     //make hashes unique per tuple
     proc->generation++;
     proc->generation_hash = evahash64((uint8_t*)&proc->generation, sizeof(uint64_t), 0x2123fdee);


     //check if anything should be removed
     int found = tuple_nested_search(input_data, &proc->nest,
                                     nest_ignore_element,
                                     proc, NULL);
     if (!found) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
          dprint("removenest nothing to remove");
          return 1;
     }

     //start deep copy
     wsdata_t * newtupledata = wsdata_alloc(dtype_tuple);
     if (!newtupledata) {
          return 1;
     }
     wsdata_add_reference(newtupledata);

     tuple_removal_deep_copy(proc, input_data, newtupledata);

     ws_set_outdata(newtupledata, proc->outtype_tuple, dout);
     wsdata_delete(newtupledata);

     proc->outcnt++;
     dprint("removenest process_tuple end");
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free dynamic allocations
     if (proc->ignore_table) {
          stringhash5_destroy(proc->ignore_table);
     }
     free(proc);

     return 1;
}

