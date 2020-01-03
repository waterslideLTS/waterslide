/*
proc_keysort.c

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

#define PROC_NAME "keysort"
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
char *proc_alias[]             =  { "sortkey", "groupsort", "sortgroup", NULL };
char proc_purpose[]            =  "sorts events about a key from lowest to highest value";
char proc_description[] = "streaming window sort of events per key, pressure expiration";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'V',"","LABEL",
     "value to sort on",0,0},
     {'n',"","count",
     "maximum number of events to store at key",0,0},
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
     {NULL, NULL}
};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};

typedef struct _el_data_t {
     double value;
     wsdata_t * data;
} el_data_t;

typedef struct _key_data_t {
     uint32_t next;
     uint32_t generation; //for expiration
     el_data_t el[0];
} key_data_t;

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_expire(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     stringhash5_t * session_table;
     uint32_t buflen;
     wslabel_nested_set_t nest_key;
     wslabel_nested_set_t nest_value;

     ws_outtype_t * outtype_tuple;
     ws_doutput_t * dout;

     uint32_t maxcnt;  //max number of events per key
     size_t key_struct_size;

     key_data_t * global_key; //when not key is specified, use a global key table
     uint32_t generation;

} proc_instance_t;


static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     dprint("proc_cmd_options");
     int op;

     while ((op = getopt(argc, argv, "v:V:N:n:M:")) != EOF) {
          switch (op) {
          case 'V':
          case 'v':
               wslabel_nested_search_build(type_table, &proc->nest_value, optarg);
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

static void emit_state(void * vdata, void * vproc) {
     proc_instance_t * proc = (proc_instance_t *)vproc;
     key_data_t * kdata = (key_data_t *)vdata;

     dprint("emit_state");
     //flush all stored content
     uint32_t i;
     for (i = 0; i < proc->maxcnt; i++) {
          uint32_t k = (i + kdata->next) % proc->maxcnt;
          if (kdata->el[k].data) {
               ws_set_outdata(kdata->el[k].data, proc->outtype_tuple, proc->dout);
               wsdata_delete(kdata->el[k].data);
          }
     }

     memset(kdata, 0, proc->key_struct_size);
}

#define DEFAULT_TABLE_SIZE (50000)

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

     proc->buflen = DEFAULT_TABLE_SIZE;

     proc->maxcnt = 20;
     proc->generation = 1;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->nest_value.cnt) {
          tool_print("need to specify a value to sort");
          return 0;
     }
     tool_print("storing %u values per key", proc->maxcnt);
     proc->key_struct_size = sizeof(key_data_t) + 
          (sizeof(el_data_t) * proc->maxcnt);

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
     if (wslabel_match(type_table, port, "EXPIRE")) {
          if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
               tool_print("doing expire processing");
               return proc_expire;
          }
          return NULL;
     }

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

static inline void replace_kv(proc_instance_t * proc, key_data_t * kdata,
                      double dv, wsdata_t * tdata) {
     uint32_t pos = kdata->next;

     dprint("replace_kv %u", pos);
     if (kdata->el[pos].data) {
          dprint("emit data");
          ws_set_outdata(kdata->el[pos].data, proc->outtype_tuple, proc->dout);
          wsdata_delete(kdata->el[pos].data);
     }
     kdata->el[pos].value = dv;
     kdata->el[pos].data = tdata;
     wsdata_add_reference(tdata);
     kdata->next = (kdata->next + 1) % proc->maxcnt;
}

static void insert_kv(proc_instance_t * proc, key_data_t * kdata,
                     wsdata_t * value, wsdata_t * tdata) {
     dprint("insert kv");
    
     double dv = 0; 
     if (!dtype_get_double(value, &dv)) {
          dprint("could not convert to double");
          return; 
     }
     dprint("current value %.0f", dv);

     //check immediate prior values
     uint32_t p = (proc->maxcnt + kdata->next - 1) % proc->maxcnt;  
     if (!kdata->el[p].data || (kdata->el[p].value <= dv)) {
          dprint("in order insert: %f", dv);
          replace_kv(proc, kdata, dv, tdata);
          return;
     }
     //check lowest value
     if (kdata->el[kdata->next].value > dv) {
          //emit tdata without insertion
          //TODO: decide that value is too off -- reset (detect reboot)
          dprint("insert prior");
          ws_set_outdata(tdata, proc->outtype_tuple, proc->dout);
          return;
     }

     //emit oldest one.. 
     if (kdata->el[kdata->next].data) {
          ws_set_outdata(kdata->el[kdata->next].data, proc->outtype_tuple, proc->dout);
          wsdata_delete(kdata->el[kdata->next].data);
     }

     //ok out of order.. do more complicated stuff
     dprint("out of order");
     uint32_t prev = kdata->next;
     uint32_t i;
     for (i = 1; i < proc->maxcnt; i++) {
          uint32_t cursor = (kdata->next + i) % proc->maxcnt;
          if (kdata->el[cursor].value <= dv) {
               //shift to fill space
               kdata->el[prev].value = kdata->el[cursor].value;
               kdata->el[prev].data = kdata->el[cursor].data; 
          }
          else {
               dprint("found hole");
               kdata->el[prev].value = dv;
               kdata->el[prev].data = tdata;
               wsdata_add_reference(tdata);
               return;
          }
          prev = cursor;
     }
     dprint("unexpected exit!!!!!");

}

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

static int proc_expire(void * vinstance, wsdata_t* input_data,
                       ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;
     tool_print("proc_expire!!!");
     if (proc->global_key) {
          expire_state(proc->global_key, proc);
     }
     else {
          //walk entire hashtable..
          stringhash5_scour(proc->session_table, expire_state, proc);
     }
     proc->generation++;
     if (proc->generation == 0) {
          proc->generation = 1;
     }
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

     wsdata_t * value = NULL;
     if (tuple_nested_search(input_data, &proc->nest_value,
                             nest_search_key,
                             proc, &value) ) {
          if (value) {
               insert_kv(proc, kdata, value, input_data);
          }
     }
     
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

