/*
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
// Purpose: group events around a key for use in realtime event summarization
//   Expire data based on max events, changes in a specified session key, or
//   external trigger (such as a timed source)
//   When triggering from an external source, expiration is gradual through
//   incremental walking of the state table in order to reduce blocking when
//   expiration triggers.

#define PROC_NAME "groupevents"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <netinet/tcp.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "stringhash5.h"
#include "evahash64_data.h"
#include "procloader.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"Sessionization", "State tracking", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "group events about a key with a common data similarity";
char proc_description[] = "group events happening in sequence to create meta-events";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'n',"","count",
     "maximum number of events to store at key",0,0},
     {'C',"","label",
     "common value for grouping events for a key",0,0},
     {'V',"","label",
     "label of member to store at key (can be multiple)",0,0},
     {'L',"","label",
     "label of output tuple",0,0},
     {'1',"","label",
     "label of member to store once per group at key (singletons)",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]     =  "LABEL of key to index on";
char *proc_input_types[]       =  {"any","tuple", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {
     {"none","Store value at key"},
     {"EXPIRE","trigger expiration of all buffered state"},
     {"ENDSTATE","trigger expiration of a specific key, append values"},
     {"END","trigger expiration of a specific key, append values"},
     {"ENDSINGLE","trigger expiration of a specific key, append values and singletons"},
     {NULL, NULL}
};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  {"groupevents <LABEL of key> -C <LABEL of common value> -V <LABEL of event to store>", NULL};
proc_example_t proc_examples[] =  {
	{NULL, NULL}
};

typedef struct _key_data_t {
     uint16_t valuecnt;
     uint16_t keepone_cnt;
     uint32_t generation;
     uint64_t commonhash;  //detect changes
     wsdata_t * key; 
     wsdata_t * common; 
     wsdata_t * value[0]; //events stored at key
} key_data_t;

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_expire(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_endstate(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_endsingle(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     stringhash5_t * session_table;
     stringhash5_walker_t * session_walker;
     uint64_t loop_target; 
     int loop_started;
     uint32_t loop_generation;

     uint32_t buflen;
     wslabel_nested_set_t nest_key;
     wslabel_nested_set_t nest_common;
     wslabel_nested_set_t nest_value;
     wslabel_nested_set_ext_t nest_keepone;
     uint16_t keepone_cnt;
     uint16_t value_cnt; //number of values to store
     uint16_t maxvalues;

     wslabel_t * label_output;

     ws_outtype_t * outtype_tuple;
     ws_doutput_t * dout;

     uint16_t maxcnt;  //max number of events per key
     size_t key_struct_size;
     uint32_t generation;

     key_data_t * global_key; //when not key is specified, use a global key table

} proc_instance_t;


static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     dprint("proc_cmd_options");
     int op;

     while ((op = getopt(argc, argv, "1:L:v:V:c:C:N:n:M:")) != EOF) {
          switch (op) {
          case 'L':
               proc->label_output = wsregister_label(type_table, optarg);
               break;
          case 'V':
          case 'v':
               wslabel_nested_search_build(type_table, &proc->nest_value, optarg);
               proc->value_cnt++;
               break;
          case 'C':
          case 'c':
               wslabel_nested_search_build(type_table, &proc->nest_common,
                                           optarg);
               break;
          case '1':
               //have each specified key have an index offset into keepone list
               wslabel_nested_search_build_ext(type_table, &proc->nest_keepone,
                                           optarg, proc->keepone_cnt);
               proc->keepone_cnt++;
               break;
          case 'N':
          case 'n':
               proc->maxcnt = atoi(optarg);
               tool_print("storing %d objects at key", proc->maxcnt);
               if(proc->maxcnt <= 0) {
                    error_print("invalid count of objects ... specified %d",
                                proc->maxcnt);
                    return 0;
               }
               break;
          case 'M':
               proc->buflen = atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build(type_table, &proc->nest_key, argv[optind]);
          tool_print("using key %s", argv[optind]);
          optind++;
     }
     
     return 1;
}

//write buffered data onto tuple
static void emit_values_to_tuple(proc_instance_t * proc,
                                 key_data_t * kdata,
                                 wsdata_t * tdata) {

     //attached all stored content
     uint16_t i;
     for (i = 0; i < kdata->valuecnt; i++) {
          add_tuple_member(tdata, kdata->value[i]);
     }
     
}

static void emit_singletons_to_tuple(proc_instance_t * proc,
                                 key_data_t * kdata,
                                 wsdata_t * tdata) {

     //if any keepone present - scan each keep-one position for any values
     if (kdata->keepone_cnt) {
          uint16_t i;
          for (i = 0; i < proc->keepone_cnt; i++) {
               if (kdata->value[proc->maxvalues + i]) {
                    add_tuple_member(tdata, kdata->value[proc->maxvalues + i]);
               }
          }
     }

}
//create a new tuple for emitting output
//  - called when expiration conditions are met
static void emit_values(proc_instance_t * proc, key_data_t * kdata) {
     dprint("emit_values");
     wsdata_t * tdata = wsdata_alloc(dtype_tuple);
     if (!tdata) {
          return;
     } 

     wsdata_add_reference(tdata);
     
     if (proc->label_output) {
          wsdata_add_label(tdata, proc->label_output);
     }

     if (kdata->key) {
          add_tuple_member(tdata, kdata->key);
     }
     if (kdata->common) {
          add_tuple_member(tdata, kdata->common);
     }

     emit_values_to_tuple(proc, kdata, tdata);
     emit_singletons_to_tuple(proc, kdata, tdata);

     ws_set_outdata(tdata, proc->outtype_tuple, proc->dout);
     wsdata_delete(tdata);
}

//create a new tuple for emitting output
//  - but preserve certain elements because event is expected to continue
static void emit_state_preserve_keys(proc_instance_t * proc, key_data_t * kdata) {
     dprint("emit_state_preserve_keys");
     if (kdata->valuecnt) {
          emit_values(proc, kdata);
     }
     uint16_t i;
     for (i = 0; i < kdata->valuecnt; i++) {
          wsdata_delete(kdata->value[i]);
          kdata->value[i] = NULL;
     }
     kdata->valuecnt = 0;
}

static void clean_state(proc_instance_t * proc, key_data_t * kdata) {
     //clean up references
     if (kdata->key) {
          wsdata_delete(kdata->key);
     }
     if (kdata->common) {
          wsdata_delete(kdata->common);
     }

     uint16_t i;
     for (i = 0; i < kdata->valuecnt; i++) {
          wsdata_delete(kdata->value[i]);
     }

     //delete any elements in keepone section
     if (kdata->keepone_cnt) {
          for (i = 0; i < proc->keepone_cnt; i++) {
               if (kdata->value[proc->maxvalues + i]) {
                    wsdata_delete(kdata->value[proc->maxvalues + i]);
               }
          }
     }

     memset(kdata, 0, proc->key_struct_size);

}

//emit any data and delete state
static void emit_state(void * vdata, void * vproc) {
     dprint("emit_state");
     proc_instance_t * proc = (proc_instance_t *)vproc;
     key_data_t * kdata = (key_data_t *)vdata;
     dprint("emit state");

     //see if it is worth emitting data out
     if (kdata->valuecnt) {
          emit_values(proc, kdata);
     }

     clean_state(proc, kdata);
}

//used when walking state table to see if records should be expired
static void expire_state(void * vdata, void * vproc) {
     key_data_t * kdata = (key_data_t *)vdata;
     proc_instance_t * proc = (proc_instance_t*)vproc;
     if (!kdata->generation) {
          return;
     }
     if (kdata->generation != proc->generation) {
          emit_state(vdata, vproc);
     }
}


static int check_expiration(void * vdata, void * vproc) {
     dprint("checking expiration");
     key_data_t * kdata = (key_data_t *)vdata;
     proc_instance_t * proc = (proc_instance_t*)vproc;
     if (!kdata->generation) {
          return 0;
     }
     dprint("checking expiration %u %u %u", kdata->generation, proc->generation,
            proc->loop_generation);

     if ((kdata->generation != proc->generation) && 
         (kdata->generation != proc->loop_generation)) {
          emit_state(vdata, vproc);
          return 0;
     }
     return 1;
}

static inline void check_expire_loop(proc_instance_t * proc) {
     if (proc->loop_started) {
          dprint("loop_started- check expire_loop");
          stringhash5_walker_next(proc->session_walker);

          if (proc->session_walker->loop == proc->loop_target) {
               dprint("loop ended");
               proc->loop_started = 0;
          }
     }
}

// the following is a function to take in command arguments and initalize
// this processor's instance..
//  also register as a source here..
// return 1 if ok
// return 0 if fail
int proc_init(wskid_t * kid, int argc, char ** argv, void ** vinstance, ws_sourcev_t * sv,
              void * type_table) {

     dprint("proc_init");     
     //allocate proc instance of this processor
     proc_instance_t * proc =
          (proc_instance_t*)calloc(1,sizeof(proc_instance_t));
     *vinstance = proc;

     ws_default_statestore(&proc->buflen);
     proc->generation = 1;

     proc->maxcnt = 8;
     proc->label_output = wsregister_label(type_table, "GROUPEVENTS");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }


     if (proc->value_cnt) {
          proc->maxvalues = proc->maxcnt * proc->value_cnt;
     }
     else {
          tool_print("no value defined, using entire tuple as event");
          proc->maxvalues = proc->maxcnt;
     }
     tool_print("storing %u values per key", proc->maxvalues);
     proc->key_struct_size = sizeof(key_data_t) + 
          sizeof(wsdata_t*) * proc->maxvalues +
          sizeof(wsdata_t*) * proc->keepone_cnt;


     if (proc->nest_key.cnt) {
          //other init - init the stringhash table
          proc->session_table = stringhash5_create(0, proc->buflen,
                                                   proc->key_struct_size);
          if (!proc->session_table) {
               return 0;
          }
          stringhash5_set_callback(proc->session_table, emit_state, proc);
          proc->buflen = proc->session_table->max_records;
     }
     else {
          tool_print("no key defined, using global key");
          proc->global_key = calloc(1, proc->key_struct_size);
     }

     //use the stringhash5-adjusted value of max_records to reset buflen

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
     if ((meta_type == dtype_tuple) && 
         wslabel_match(type_table, port, "ENDSINGLE")) {
          return proc_endsingle;
     }
     if ((meta_type == dtype_tuple) && 
         (wslabel_match(type_table, port, "ENDSTATE") ||
          wslabel_match(type_table, port, "END"))) {
          return proc_endstate;
     }
     if (wslabel_match(type_table, port, "EXPIRE")) {
          if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               if (!proc->session_walker) {
                    proc->session_walker =
                         stringhash5_walker_init(proc->session_table,
                                                 check_expiration, proc);
               }
               tool_print("doing expire processing");
               return proc_expire;
          }
          return NULL;
     }

     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          return proc_flush;
     }
     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          return proc_tuple;
     }

     return NULL; // a function pointer
}


//only select first key found as key to use
static int nest_search_key(void * vproc, void * vkey,
                           wsdata_t * tdata, wsdata_t * member) {
     dprint("nest_search_key");
     //proc_instance_t * proc = (proc_instance_t*)vproc;
     wsdata_t ** pkey = (wsdata_t **)vkey;
     wsdata_t * key = *pkey;
     if (!key) {
          *pkey = member;
          return 1;
     }
     return 0;
}

//look up nested_key index offset
static int nest_search_keepone(void * vproc, void * vkey,
                               wsdata_t * tdata, wsdata_t * member,
                               wslabel_t * label, int offset) {
     dprint("nest_search_key");
     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t*)vkey;

     //perform validation
     if (offset >= proc->keepone_cnt) {
          return 0;
     }

     //check if value is already filled out
     if (kdata->value[proc->maxvalues + offset] == NULL) {
          kdata->value[proc->maxvalues + offset] = member;
          wsdata_add_reference(member);
          kdata->keepone_cnt++;
     }

     return 0;
}


static int nest_add_value(void * vproc, void * vkdata,
                           wsdata_t * tdata, wsdata_t * member) {
     dprint("nest_add_value");
     proc_instance_t * proc = (proc_instance_t*)vproc;
     key_data_t * kdata = (key_data_t *)vkdata;

     kdata->value[kdata->valuecnt] = member;
     wsdata_add_reference(member);
     kdata->valuecnt++;

     //if by adding this value - we have reached max..
     if (kdata->valuecnt >= proc->maxvalues) {
          emit_state_preserve_keys(proc, kdata);
     }
     return 1;
}

//called when an expire event is sent to processing element
//  triggers gradual expiration of events
static int proc_expire(void * vinstance, wsdata_t* input_data,
                       ws_doutput_t * dout, int type_index) {

     dprint("proc_expire called");
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;

     if (!proc->global_key && proc->session_walker) {
          if (proc->loop_started) {
               return 1;
          }
          dprint("new loop");
          proc->loop_started = 1;
          proc->loop_target = proc->session_walker->loop + 1;
          proc->loop_generation = proc->generation;
          check_expire_loop(proc);
     }
     else {
          if (proc->global_key) {
               expire_state(proc->global_key, proc);
          }
          else {
               //walk entire hashtable..
               stringhash5_scour(proc->session_table, expire_state, proc);
          }
     }

     proc->generation++;
     if (proc->generation == 0) {
          proc->generation = 1;
     }


     return 1;
}

//process when a event at a specific port wants to flush values for a given key
static int proc_endstate(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     dprint("proc_endstate");
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;
     key_data_t * kdata = NULL;
     wsdata_t * key = NULL;

     //check if using hashtable.. otherwise global key
     if (proc->global_key) {
          kdata = proc->global_key;
     }
     else if (proc->session_table) {
          tuple_nested_search(input_data, &proc->nest_key,
                              nest_search_key,
                              proc, &key);
          if (key) {
               kdata = (key_data_t *) stringhash5_find_attach_wsdata(proc->session_table, key);
          }
     }

     if (kdata) {
          emit_values_to_tuple(proc, kdata, input_data);
          clean_state(proc, kdata);
          if (key) {
               stringhash5_delete_wsdata(proc->session_table, key);
          }
     }
     ws_set_outdata(input_data, proc->outtype_tuple, dout);

     return 1;
}

//expire data at key - appending values and singletons to triggering tuple
static int proc_endsingle(void * vinstance, wsdata_t* input_data,
                          ws_doutput_t * dout, int type_index) {

     dprint("proc_endstate");
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;
     key_data_t * kdata = NULL;
     wsdata_t * key = NULL;

     //check if using hashtable.. otherwise global key
     if (proc->global_key) {
          kdata = proc->global_key;
     }
     else if (proc->session_table) {
          tuple_nested_search(input_data, &proc->nest_key,
                              nest_search_key,
                              proc, &key);
          if (key) {
               kdata = (key_data_t *) stringhash5_find_attach_wsdata(proc->session_table, key);
          }
     }

     if (kdata) {
          emit_values_to_tuple(proc, kdata, input_data);
          emit_singletons_to_tuple(proc, kdata, input_data);
          clean_state(proc, kdata);
          if (key) {
               stringhash5_delete_wsdata(proc->session_table, key);
          }
     }
     ws_set_outdata(input_data, proc->outtype_tuple, dout);

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     dprint("proc_tuple");
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     proc->meta_process_cnt++;
     key_data_t * kdata = NULL;
     wsdata_t * key = NULL;


     //check if using hashtable.. otherwise global key
     if (proc->global_key) {
          kdata = proc->global_key;
     }
     else if (proc->session_table) {
          tuple_nested_search(input_data, &proc->nest_key,
                              nest_search_key,
                              proc, &key);
          if (key) {
               kdata = (key_data_t *) stringhash5_find_attach_wsdata(proc->session_table, key);
          }
     }

     if (!kdata) {
          return 0;
     }
     kdata->generation = proc->generation;

     uint64_t commonhash = 0;
     wsdata_t * common = NULL;
     if (proc->nest_common.cnt) {
          tuple_nested_search(input_data, &proc->nest_common,
                              nest_search_key,
                              proc, &common);
          if (common) {
               commonhash = evahash64_data(common, 0x55613443);
          }
     }

     //check if commonhash matches..- if not evict old state
     if (kdata->commonhash != commonhash) {
          emit_state(kdata, proc);
          kdata->commonhash = commonhash;
     }

     //populate state as needed
     if (!kdata->key && key) {
          kdata->key = key;
          wsdata_add_reference(key);
     }
     if (!kdata->common && common) {
          kdata->common = common;
          wsdata_add_reference(common);
     }
     if (proc->keepone_cnt && (kdata->keepone_cnt < proc->keepone_cnt)) {
          tuple_nested_search_ext(input_data, &proc->nest_keepone,
                                  nest_search_keepone,
                                  proc, kdata);
     }

     if (proc->nest_value.cnt) {
          if (tuple_nested_search(input_data, &proc->nest_value,
                                  nest_add_value,
                                  proc, kdata) ) {

               //check if state was added but is now empty.. flush keys
               if (kdata->valuecnt == 0) {
                    emit_state(kdata, proc);
               }
          }
     }
     else {      //add entire tuple
          kdata->value[kdata->valuecnt] = input_data;
          wsdata_add_reference(input_data);
          kdata->valuecnt++;
     }

     if (kdata->valuecnt >= proc->maxvalues) {
          emit_state(kdata, proc);
     }

     check_expire_loop(proc);

     //always return 1 since we don't know if table will flush old data
     return 1;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;

     // Note:  the following seems backwards.  But, if hitemit is set, we only
     // want full sets of elements, so rec_erase is used to throw away all of 
     // the partials at flush time.  But, if hitemit is not set, then we are
     // expecting the partials, hence the use of emit_state here.
     if (proc->global_key) {
          emit_state(proc->global_key, proc);
     }
     else if (proc->session_table) {
          stringhash5_scour_and_flush(proc->session_table, emit_state, proc);
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     if (proc->global_key) {
          free(proc->global_key);
     }
     else if (proc->session_table) {
          stringhash5_destroy(proc->session_table);
     }

     //free dynamic allocations
     free(proc);

     return 1;
}

