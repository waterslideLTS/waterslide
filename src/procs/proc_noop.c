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
#define PROC_NAME "noop"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_flush.h"
#include "procloader.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
// Use to fix proc_tags: 
//char *proc_menus[]             =  { "Filters", NULL };
char *proc_tags[]              =  {"Filters", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "No Operation passthrough";
char proc_description[] = "A passthrough that performs no operation on data"; 

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'S',"","",
     "act as a tuple input source but emit nothing",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "None";
char *proc_input_types[]       =  {"any", NULL};
char *proc_output_types[]      =  {"any", NULL};
char proc_requires[]           =  "None";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};

#define LOCAL_MAX_TYPES 25

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_source(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;
     ws_outtype_t * outtype_meta[LOCAL_MAX_TYPES];
     ws_outtype_t * outtype_tuple;
     int do_source;
} proc_instance_t;


static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "S")) != EOF) {
          switch (op) {
          case 'S':
               proc->do_source = 1;
               break;
          default:
               return 0;
          }
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


     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (proc->do_source) {
          proc->outtype_tuple = ws_register_source(dtype_tuple, proc_source, sv);
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

     //register output..
     // pass the input type to the output..
     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }

     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          return NULL;
     }

     //handle any datatype passed in
     proc->outtype_meta[type_index] = ws_add_outtype(olist, meta_type, NULL);

     // we are happy.. now set the processor function
     return proc_process_meta; // a function pointer
}

static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     //pass through
     ws_set_outdata(input_data, proc->outtype_meta[type_index], dout);
     proc->outcnt++;
     return 1;
}

//return 0 = no data left
static int proc_source(void * vinstance, wsdata_t* input_data,
                       ws_doutput_t * dout, int type_index) {

     //return no data as source
     return 0;
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

