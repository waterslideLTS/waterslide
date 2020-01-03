/*
proc_keyarrival.c

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

// annotates the sequence of arrival for elements of a key starting with 1

#define PROC_NAME "keyarrival"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "stringhash5.h"
#include "procloader.h"
#include "sysutil.h"
#include "wstypes.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
// Use to fix proc_tags: 
//char *proc_menus[]             =  { "Filters", NULL };
char *proc_tags[]              =  {"Filtering", "Stream manipulation", NULL};
char *proc_alias[]             =  { "arrival", "keyseq", "keysequence", NULL };
char proc_purpose[]            =  "annotates the arrival sequence of each key";
char proc_description[] = "annotates the arrival sequence of each key, starting index 0 and counting upwards.  with emit the first N tuples for each value in a specified LABEL (key). It is also possible to control the size of the storage table with the '-M' option (default value is 350000 bytes).";
proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'L',"","LABEL",
     "Label of sequence counter (ARRIVAL) by default",0,0},
     {'F',"","filename",
     "load existing database (key table) from file",0,0},
     {'O',"","filename",
     "write database (key table) to file",0,0},
     {'M',"","records",
     "maximum table size",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "LABEL of key to track";
char *proc_input_types[]       =  {"tuple", "any", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"any", NULL};
char proc_requires[]           =  "None";
// Ports: 
proc_port_t proc_input_ports[] =  {
     {NULL, NULL}
};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  {"keyarrival <LABEL of key> [-L <LABEL of sequence>]", NULL};
proc_example_t proc_examples[] =  {
	{NULL, NULL}
};

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _key_data_t {
     uint64_t sequence;
} key_data_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     uint64_t lastsequence;

     ws_outtype_t * outtype_tuple;
     stringhash5_t * key_table;

     uint32_t tablemax;
     wslabel_nested_set_t nest;

     wslabel_t * label_arrival;
     char * outfile;
     char * open_table;

} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "O:F:L:M:")) != EOF) {
          switch (op) {
          case 'O':
               proc->outfile = strdup(optarg);
               break;
          case 'F':
               proc->open_table = strdup(optarg);
               break;
          case 'L':
               proc->label_arrival = wsregister_label(type_table, optarg);
               break;
          case 'M':
               proc->tablemax = atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build(type_table, &proc->nest, argv[optind]);
          tool_print("using key %s", argv[optind]);
          optind++;
     } 
     return 1;
}

static int read_stored_table(proc_instance_t * proc) {
     uint64_t maxseq = 0;
     if (!proc->open_table) {
          return 0;
     }
     FILE * fp = sysutil_config_fopen(proc->open_table, "r");
     if (!fp) {
          tool_print("unable to open file %s", proc->open_table);
          return 0;
     }
     tool_print("opening file %s", proc->open_table);
     proc->key_table = stringhash5_read(fp);
     if (proc->key_table) {
          if (!fread(&maxseq, sizeof(uint64_t), 1, fp)) {
               tool_print("error opening file %s", proc->open_table);
               return 0;
          }
     }

     sysutil_config_fclose(fp);

     tool_print("setting last sequence number %"PRIu64, maxseq);
     proc->lastsequence = maxseq;

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

     ws_default_statestore(&proc->tablemax);

     proc->label_arrival = wsregister_label(type_table, "ARRIVAL");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (proc->nest.cnt == 0) {
          tool_print("must specify key to process");
          return 0;
     }

     if (!read_stored_table(proc)) {
          proc->key_table = stringhash5_create(0, proc->tablemax, 
                                               sizeof(key_data_t));
     }
     if (!proc->key_table) {
          return 0;
     }

     //use the stringhash5-adjusted value of max_records to reset tablemax
     proc->tablemax = proc->key_table->max_records;

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

     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          return proc_flush;
     }
     if (meta_type == dtype_tuple) {
          return proc_tuple;
     }
     // we are happy.. now set the processor function
     return NULL; // a function pointer
}

static int nest_search_callback_match(void * vproc, void * vevent,
                                      wsdata_t * tdata, wsdata_t * member) {

     proc_instance_t * proc = vproc;
     key_data_t * kdata;
     kdata = (key_data_t *)stringhash5_find_attach_wsdata(proc->key_table, member);
     if (!kdata) {
          return 0;
     }
     if (kdata->sequence == 0) {
          kdata->sequence = proc->lastsequence + 1;
          proc->lastsequence++;
     }
     tuple_member_create_uint64(tdata, kdata->sequence, proc->label_arrival);
     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     tuple_nested_search(input_data, &proc->nest, nest_search_callback_match,
                         proc, NULL);
     // this is new data.. pass as output
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;
     return 1;
}


static void serialize_table(proc_instance_t * proc) {
     if (proc->outfile) {
          tool_print("Writing key table to %s", proc->outfile);
          FILE * fp = fopen(proc->outfile, "w");
          if (fp) {
               if (stringhash5_dump(proc->key_table, fp)) {
                    //add last sequence to end of record 
                    fwrite(&proc->lastsequence, sizeof(uint64_t), 1, fp);
               }
               fclose(fp);
          }
     }
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     serialize_table(proc);

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //destroy table
     stringhash5_destroy(proc->key_table);

     free(proc);

     return 1;
}

