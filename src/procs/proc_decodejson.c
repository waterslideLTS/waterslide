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
#define PROC_NAME "decodejson"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "cjson.h"
#include "waterslide.h"
#include "waterslidedata.h"
#include "mimo.h"
#include "procloader.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_string.h"

char proc_version[] = "1.0";
char *proc_alias[] = { "json", "jsondecode", NULL };
char proc_name[] = PROC_NAME;
char *proc_tags[] = { "decoder", NULL };
char proc_requires[] = "";
char proc_purpose[] = "decode a JSON string into tuples";

char *proc_synopsis[] = {"decodejson", NULL};

proc_example_t proc_examples[] = {
     {" ... | decodejson JSONSTR | print -TV","decode the JSON contained in the JSONSTR member and print the contents as a tree"},
     {NULL,""}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_description[] = \
"Decodejson converts a buffer containing a JSON string into WS tuples. " 
"JSON keys and values are converted into tuple labels and values respectively "
"Nested JSON objects are converted into nested tuples. JSON arrays are "
"converted into nested tuples, where each member in the subtuple has the "
"label \"<KEY>_<INDEX>\", similar to the lastn kid.";


char *proc_tuple_member_labels[] = {"conditional on input", NULL};
char proc_nonswitch_opts[] = "label of buffer containing JSON";
char *proc_input_types[] = {"tuple", NULL};
char *proc_output_types[] = {"tuple", NULL};
proc_port_t proc_input_ports[] = {{NULL, NULL}};
char *proc_tuple_container_labels[] = {"conditional on input", NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     void * type_table;
     wslabel_t * label_parseme;
     ws_outtype_t * outtype_tuple;
} proc_instance_t;

//function prototypes for local functions
static int proc_tuple(void *, wsdata_t *, ws_doutput_t *, int);
static inline void walk_cjson(proc_instance_t *, wsdata_t *, cJSON *, char *);
static inline void walk_cjson_array(proc_instance_t *, wsdata_t *, cJSON *, char *);
static inline void walk_cjson_number(proc_instance_t *, wsdata_t *, cJSON *, char *);
static inline void walk_cjson_object(proc_instance_t *, wsdata_t *, cJSON *, char *, int);
static inline void walk_cjson_string(proc_instance_t *, wsdata_t *, cJSON *, char *);
static inline void walk_cjson_string_literal(proc_instance_t *, wsdata_t *, cJSON *, char *, char *);


static int proc_cmd_options(int argc, char ** argv, proc_instance_t * proc,
                                                       void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "")) != EOF) {
          switch (op) {
          default:
               return 0;
          }
     }

     while (optind < argc) {
          proc->label_parseme = wssearch_label(type_table, argv[optind]);
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

     proc->type_table = type_table;

     proc->outtype_tuple = NULL;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->label_parseme) {
          dprint("A label is required!");
          return 0;
     }

     return 1; 
}

// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
// return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * input_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {

     proc_instance_t * proc = (proc_instance_t *) vinstance;

     // Only accept tuples as input.
     if (!wsdatatype_match(type_table, input_type, "TUPLE_TYPE")) {
          return NULL;
     }

     // Register tuple as the output type.
     proc->outtype_tuple = ws_add_outtype(olist, input_type, NULL);

     return proc_tuple;
}

static int32_t get_current_index_size(void * type_table) {
     mimo_datalists_t * mdl = (mimo_datalists_t *) type_table;
     return mdl->index_len;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*) vinstance;

     // Get the tuple member data.
     wsdata_t * wsd_json = 
          tuple_find_single_label(input_data, proc->label_parseme);

     uint32_t start_index_size = get_current_index_size(proc->type_table);

     // Confirm the data is string type.
     if (!wsd_json || wsd_json->dtype != dtype_string) {
          return 0;
     }

     wsdt_string_t * string = (wsdt_string_t *) wsd_json->data;

     // XXX: this is really slow, need a better way to get a correctly
     // nul-terminated char * to the json string 
     char * json_str = strndup(string->buf, string->len);

     // Parse the Json.
     cJSON * cjson = cJSON_Parse(json_str);

     // If not valid json, pass the current tuple and bail.
     if (!cjson) {
          free(json_str);
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
          return 1;
     }

     // Walk the JSON object.
     walk_cjson_object(proc, input_data, cjson, NULL, 0);

     // Set the outdata. Clone the tuple as necessary to deal with newly
     // allocated labels.
     uint32_t end_index_size = get_current_index_size(proc->type_table);
     if (start_index_size == end_index_size) {
          // the index size didn't change, write the data out
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     } else {
          // the index size changed: dupe the tuple to index the new labels 
          // within the tuple
          tool_print("cloning tuple to add new labels into label index");
          wsdata_t * input_data_copy = wsdata_alloc(dtype_tuple);
          if(!input_data_copy) {
               return 0;
          }
          if(!tuple_deep_copy(input_data, input_data_copy)) {
               return 0;
          }
          ws_set_outdata(input_data_copy, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     // Cleanup.
     cJSON_Delete(cjson);
     free(json_str);

     proc->meta_process_cnt++;

     return 1;
}

static inline void walk_cjson_array(proc_instance_t * proc, wsdata_t * tdata, cJSON * item, char * name) {
     wslabel_t * array_label;

     char * new_name;
     if (!item->string && name) {
          array_label = wsregister_label(proc->type_table, name);
          new_name = name;
     } else {
          array_label = wsregister_label(proc->type_table, item->string);
          new_name = item->string;
     }

     // Create a subtuple to hold this json array. 
     wsdata_t * new_tdata = ws_get_outdata(proc->outtype_tuple);
     wsdata_add_label(new_tdata, array_label);
     add_tuple_member(tdata, new_tdata);
     tdata = new_tdata;

     cJSON * child = item->child;

     char labelbuf[256];
     int cnt=0;
     while (child) {
          // Give each json array member the WS label of <KEY>_<idx>. This
          // isn't clean but WS doesn't support lists so it might be the best
          // we can get.
          snprintf(labelbuf, sizeof(labelbuf), "%s_%d", new_name, cnt);
          walk_cjson(proc, tdata, child, labelbuf);
          child=child->next;
          cnt++;
     }
}

static inline void walk_cjson(proc_instance_t * proc, wsdata_t * tdata,
                              cJSON *item, char * name) {
     if (!item) return;

     switch ((item->type) & 255) {
          case cJSON_NULL:
               walk_cjson_string_literal(proc, tdata, item, name, "null");
               break;
          case cJSON_True:
               walk_cjson_string_literal(proc, tdata, item, name, "true");
               break;
          case cJSON_False:
               walk_cjson_string_literal(proc, tdata, item, name, "false");
               break;
          case cJSON_Number:
               walk_cjson_number(proc, tdata, item, name);
               break;
          case cJSON_String:
               walk_cjson_string(proc, tdata, item, name);
               break;
          case cJSON_Array:
               walk_cjson_array(proc, tdata, item, name);
               break;
          case cJSON_Object:
               walk_cjson_object(proc, tdata, item, name, 1);
               break;
     }
}

static inline void walk_cjson_string_literal(proc_instance_t * proc, 
          wsdata_t * tdata, cJSON *item, char * name, char * string_literal) {
     wslabel_t * string_label;
     if (!name) {
          string_label = wsregister_label(proc->type_table, item->string);
     } else {
          string_label = wsregister_label(proc->type_table, name);
     }

     tuple_dupe_string(tdata, string_label, string_literal,
          strlen(string_literal)); 
}

static inline void walk_cjson_number(proc_instance_t * proc, wsdata_t * tdata, 
                                     cJSON *item, char * name) {
     wslabel_t * number_label;
     if (!name) {
          number_label = wsregister_label(proc->type_table, item->string);
     } else {
          number_label = wsregister_label(proc->type_table, name);
     }
     tuple_member_create_double(tdata, item->valuedouble, number_label);
}

static inline void walk_cjson_string(proc_instance_t * proc, wsdata_t * tdata, 
                                     cJSON *item, char * name) {
     wslabel_t * string_label;
     if (!name) {
          string_label = wsregister_label(proc->type_table, item->string);
     } else {
          string_label = wsregister_label(proc->type_table, name);
     }
     tuple_dupe_string(tdata, string_label, item->valuestring,
          strlen(item->valuestring)); 
}

static inline void walk_cjson_object(proc_instance_t * proc, wsdata_t * tdata,
                                     cJSON *item, char * name, int create_tuple) {
     cJSON *child = item->child;

     // If this isn't the outermost json object, create a nested tuple and add
     // the JSON key as a WS container label.
     if (create_tuple) {
          wslabel_t * object_label;
          if (!name) {
               object_label = wsregister_label(proc->type_table, item->string);
          } else {
               object_label = wsregister_label(proc->type_table, name);
          }
          wsdata_t * new_tdata = ws_get_outdata(proc->outtype_tuple);
          wsdata_add_label(new_tdata, object_label);
          add_tuple_member(tdata, new_tdata);
          tdata = new_tdata;
     }

     while (child) {
          walk_cjson(proc, tdata, child, NULL);
          child=child->next;
     }
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
