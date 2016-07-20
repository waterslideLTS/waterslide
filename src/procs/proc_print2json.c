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
#define PROC_NAME "print2json"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_binary.h"
#include "cjson.h"

char proc_version[] = "0.1";
char *proc_menus[]  = { "Filters", NULL };
char *proc_alias[]  = { "pmetajson", "pjson", NULL };
char proc_name[]    = PROC_NAME;
char proc_purpose[] = "print out tuple metadata in JSON format";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'O',"","outfile",
     "set output file (stdout default)",0,0},
     {'s',"","",
     "dont act as a sink (required for generating Cubism visualization)",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char *proc_input_types[]    = {"tuple", NULL};

#define LOCAL_MAX_TYPES 25

//function prototypes for local functions
static int  proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];

     wslabel_t * label;

     cJSON *json_root;
     cJSON *json_root_array;

     FILE * outfp;

     int act_as_sink;
} proc_instance_t;


static int proc_cmd_options(int argc, char ** argv, proc_instance_t * proc,
                                                       void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "w:O:s?")) != EOF) {
          switch (op) {
          case 'O':
          case 'w':
               proc->outfp = fopen(optarg, "w");
               if (!proc->outfp) {
                    error_print("unable to open file %s for writing", optarg);
                    return 0;
               }   
               tool_print("opening %s for writing", optarg);
               break;
          case 's':
               proc->act_as_sink = 0;
               break;
          case '?':
          default:
               return 0;
          }
     }
     while (optind < argc) {
          proc->label = wsregister_label(type_table, argv[optind]);
          tool_print("labeling objects as %s", argv[optind]);
          optind++;
     } 

     return 1;
}

// the following is a function to take in command arguments and initalize
// this processor's instance..
// also register as a source here..
// return 1 if ok
// return 0 if fail
int proc_init(wskid_t * kid, int argc, char ** argv, void ** vinstance, 
              ws_sourcev_t * sv, void * type_table) {

     //allocate proc instance of this processor
     proc_instance_t * proc =
          (proc_instance_t*) calloc(1,sizeof(proc_instance_t));
     *vinstance = proc;

     proc->outfp = stdout;

     proc->json_root = cJSON_CreateObject();

     proc->act_as_sink = 1;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     proc->json_root_array = cJSON_CreateArray();
     cJSON_AddStringToObject(proc->json_root,"type","WS");
     cJSON_AddItemToObject(proc->json_root, "data", proc->json_root_array);

     return 1; 
}

// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
// return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * meta_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {

     proc_instance_t * proc = (proc_instance_t *) vinstance;


     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type,
                                                     proc->label);  

     if (meta_type == dtype_tuple) {
          return proc_tuple;
     }

     return NULL;
}

static inline void write_to_file(proc_instance_t * proc, wsdata_t * tdata) {
     cJSON *array_element;
     cJSON_AddItemToArray(proc->json_root_array,array_element = cJSON_CreateObject());

     wsdt_tuple_t * tup = (wsdt_tuple_t*) tdata->data;
     wsdata_t * member;

     int i;
     for (i = 0; i < tup->len; i++) {
          member = tup->member[i];

          char * key;
          if (member->label_len) {
               key= member->labels[member->label_len - 1]->name; 
          } else {
               key= "UNKNOWN";
          }

          int len;
          char * value;
          if (member->dtype == dtype_binary) { 
          } else {
               dtype_string_buffer(member, &value, &len);
          }

          cJSON_AddStringToObject(array_element,key,value);
     }
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     dprint("print proc_process_meta");

     proc_instance_t * proc = (proc_instance_t*) vinstance;

     proc->meta_process_cnt++;

     write_to_file(proc, input_data);

     if (!proc->act_as_sink) {
          ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
          proc->outcnt++;
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     fflush(proc->outfp);

     char * print = cJSON_Print(proc->json_root);
     fprintf(proc->outfp, "%s\n", print);
     free(print);

     //destroy outfile
     if (proc->outfp && (proc->outfp != stdout)) {
          fclose(proc->outfp);
     }

     cJSON_Delete(proc->json_root);

     //free dynamic allocations
     free(proc);

     return 1;
}

