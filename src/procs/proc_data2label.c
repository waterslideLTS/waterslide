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
#define PROC_NAME "data2label"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include "waterslide.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_int.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_uint16.h"
#include "datatypes/wsdt_uint8.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_fixedstring.h"
#include "waterslidedata.h"
#include "procloader.h"

char proc_version[]     = "1.1";
char *proc_menus[]     = { "Filters", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "generates label from integer or string data";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of uint members to match";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[] = {
     {"none","pass if match"},
     {NULL, NULL}
};

#define LABELLEN 1024
#define DATALEN   768

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_set_t lset;
     wsdatatype_t * dtype_uint;
     wsdatatype_t * dtype_int;
     wsdatatype_t * dtype_uint64;
     wsdatatype_t * dtype_uint16;
     wsdatatype_t * dtype_uint8;
     wsdatatype_t * dtype_string;
     wsdatatype_t * dtype_fixedstring;
     uint32_t match_type;
  void * type_table;
} proc_instance_t;


static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "L:R:")) != EOF) {
          switch (op) {
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset, argv[optind]);
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

     proc->dtype_uint   = wsdatatype_get(type_table, "UINT_TYPE");
     proc->dtype_int    = wsdatatype_get(type_table, "INT_TYPE");
     proc->dtype_uint64 = wsdatatype_get(type_table, "UINT64_TYPE");
     proc->dtype_uint16 = wsdatatype_get(type_table, "UINT16_TYPE");
     proc->dtype_uint8  = wsdatatype_get(type_table, "UINT8_TYPE");
     proc->dtype_string = wsdatatype_get(type_table, "STRING_TYPE");
     proc->dtype_fixedstring = wsdatatype_get(type_table, "FIXEDSTRING_TYPE");
     proc->type_table = type_table;
     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     if (!proc->lset.len) {
          tool_print("ERROR: must specify a label for member to match");
          return 0;
     }

     return 1; 
}

// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
//return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * input_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vinstance;

     if (!wsdatatype_match(type_table, input_type, "TUPLE_TYPE")) {
          return NULL;  // not matching expected type
     }
     proc->outtype_tuple = ws_add_outtype(olist, input_type, NULL);

     return proc_process_meta; // a function pointer
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     int id, foundType;
     wsdata_t * member;
     wslabel_t * label;
     tuple_labelset_iter_t iter;
     char newlabelname[LABELLEN];
     char datastring[DATALEN+1];
     int len;

     proc->meta_process_cnt++;
     tuple_init_labelset_iter(&iter, input_data, &proc->lset);
     while (tuple_search_labelset(&iter, &member, &label, &id)) {
       foundType = 1;
       if ( member->dtype == proc->dtype_uint64){
	 snprintf(datastring, DATALEN, "=%d", (int) *((uint64_t *)member->data));
       }
       else if ( member->dtype == proc->dtype_uint){
	 snprintf(datastring, DATALEN, "=%d", (int) *((unsigned int *)member->data));
       }
       else if ( member->dtype == proc->dtype_uint8){
	 snprintf(datastring, DATALEN, "=%d", (int) *((uint8_t *)member->data));
       }
       else if ( member->dtype == proc->dtype_uint16){
	 snprintf(datastring, DATALEN, "=%d", (int) *((uint16_t *)member->data));
       }
       else if ( member->dtype == proc->dtype_int){
	 snprintf(datastring, DATALEN, "=%d", (int) *((int *)member->data));
       }
       else if ( member->dtype == proc->dtype_string){
	 len = ((wsdt_string_t *)member->data)->len;
	 if (len >= DATALEN) {
           len = DATALEN-1;
         }
         ((wsdt_string_t *)member->data)->buf[len] = 0;
	 snprintf(datastring, len, "=%s",  ((wsdt_string_t *)member->data)->buf );
	 datastring[len+1]= '\0';
       }
       else if ( member->dtype == proc->dtype_fixedstring){
	 len = ((wsdt_fixedstring_t *)member->data)->len;
	 datastring[0] = '=';
	 if (len > DATALEN-1) len = DATALEN-1;
	 memcpy((void *) &datastring[1],  (void *) ((wsdt_fixedstring_t *)member->data)->buf, len);//not null terminated
	 datastring[len+1]= '\0';
       }
       //none of the dtypes match what we are looking for
       else {
         foundType = 0;
       }

       if(foundType){
	 strncpy(newlabelname, label->name, LABELLEN);

         if(strlen(label->name) >= LABELLEN) {
              // need to null-terminate before treating as a valid c-string
              newlabelname[LABELLEN-1] = '\0';
         }

	 strncat(newlabelname, datastring, LABELLEN-1-strlen(newlabelname));
	 tuple_add_member_label(input_data, member,  wsregister_label(proc->type_table, newlabelname));
       }
     }

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

