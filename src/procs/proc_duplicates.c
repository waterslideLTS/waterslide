/*

proc_duplicates.c

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
#define PROC_NAME "duplicates"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "evahash64.h"
#include "evahash64_data.h"
#include "stringhash9a.h"
#include "datatypes/wsdt_tuple.h"

char proc_version[]     = "1.5";
char *proc_tags[]	= {"annotation", NULL};
char *proc_menus[]     = { "Annotate", NULL };
char *proc_alias[]     = { "dupes", "duplicate", "innerdupe", "innerdupes", "tupledupes", NULL};
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "Find and label duplicate tuple members";
char proc_description[] = "Scans specified tuple members and labels any duplicate values.";
char proc_requires[] = "none";


proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'L',"","LABEL",
     "label to apply to duplicate member",0,0},
     {'U',"","LABEL",
     "label to apply to unique member",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABEL of member to check";
char *proc_input_types[]    = {"any", NULL};
char *proc_output_types[]    = {"any", NULL};
proc_port_t proc_input_ports[] = {{NULL, NULL}};

char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {NULL};

#define LOCAL_MAX_TYPES 25

//function prototypes for local functions
static int proc_process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     stringhash9a_t * dupe_table;
     ws_outtype_t * outtype_tuple;
     wslabel_nested_set_t nest;
     wslabel_t * label_dupe;
     wslabel_t * label_uniq;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "D:L:U:u:")) != EOF) {
          switch (op) {
          case 'u':
          case 'U':
               proc->label_uniq = wsregister_label(type_table, optarg);
          break;
          case 'D':
          case 'L':
               proc->label_dupe = wsregister_label(type_table, optarg);
          break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build(type_table, &proc->nest, argv[optind]);
          tool_print("scanning element %s", argv[optind]);
          optind++;
     }

     return 1;
}
#define DUPE_TABLE_SIZE 50000

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

     proc->label_dupe = wsregister_label(type_table, "DUPLICATE");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     proc->dupe_table = stringhash9a_create(0, DUPE_TABLE_SIZE);
     if (!proc->dupe_table) {
          tool_print("unable to create a stringhash9a table");
          return 0;
     }

    
     if (!proc->nest.cnt) {
          error_print("need to specify label to seach for");
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
          return proc_process_tuple;
     }
     return NULL;
}

//callback when searching nested tuple
static int proc_nest_match_callback(void * vproc, void * vehash,
                                    wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     uint64_t * p_ehash = (uint64_t *)vehash;
     uint64_t ehash = *p_ehash;

     if (member->dtype == dtype_tuple) {
          return 0;
     }

     //xor data member hash with event hash - for unique hash per tuple
     uint64_t datahash = evahash64_data(member, 1141533479) ^ ehash;

     int found = stringhash9a_set(proc->dupe_table, &datahash, sizeof(uint64_t));

     if (found) {
          if (!wsdata_check_label(member, proc->label_dupe)) {
               tuple_add_member_label(tdata, member, proc->label_dupe);
          }
     }
     else if (proc->label_uniq) {
          if (!wsdata_check_label(member, proc->label_uniq)) {
               tuple_add_member_label(tdata, member, proc->label_uniq);
          }
     }

     return 1;
}

static int proc_process_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     //create a per-event hash
     uint64_t ehash = evahash64((uint8_t*)&proc->meta_process_cnt,
                                sizeof(uint64_t),
                                0x534fd);

     //search for specified tuple member
     tuple_nested_search(input_data, &proc->nest,
                         proc_nest_match_callback,
                         proc, &ehash);


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

     if (proc->dupe_table) {
          stringhash9a_destroy(proc->dupe_table);
     }

     //free dynamic allocations
     free(proc);

     return 1;
}

