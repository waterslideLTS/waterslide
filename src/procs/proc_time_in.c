/* 
proc_time_in.c - for emitting a tuple with a timestamp at prescribed interval

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

#define PROC_NAME "time_in"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "datatypes/wsdt_tuple.h"
#include "wstypes.h"

#define LOCAL_MAX_TYPES 50

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  { "Source", NULL };
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "emits a tuple with a timestamp";
proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     //the following must be left as-is to signify the end of the array
     {'o',"","sec",
     "offset from wall clock time",0,0},
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "time in seconds (m for minutes, h for hours)";
char *proc_input_types[]       =  { "tuple", NULL };
// (Potential) Output types: flush, tuple, meta[LOCAL_MAX_TYPES]
char *proc_output_types[]      =  { "tuple", NULL };
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  { "time_in <seconds>", NULL };


//function prototypes for local functions
static int source_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     wslabel_t * label_datetime;
     ws_outtype_t * outtype_tuple;
     time_boundary_t epoch_boundary;
     int emit_time;
     time_t lasttime;
     time_t offset;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc) {

     int op;

     while ((op = getopt(argc, argv, "o:O:")) != EOF) {
          switch (op) {
          case 'o':
          case 'O':
               proc->offset = (time_t)atoi(optarg);
               tool_print("using offset %u seconds", (uint32_t)proc->offset);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          proc->epoch_boundary.increment_ts =
               sysutil_get_duration_ts(argv[optind]);
          if (proc->epoch_boundary.increment_ts) {
               fprintf(stderr,"%s emit every ", PROC_NAME);
               sysutil_print_time_interval(stderr,
                                           proc->epoch_boundary.increment_ts);
               fprintf(stderr,"\n");
               proc->emit_time = 1;
               return 1;
          }
          else {
               tool_print("invalid time %d",
                          (int)proc->epoch_boundary.increment_ts);
               return 0;
          }

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

     //read in command options
     if (!proc_cmd_options(argc, argv, proc)) {
          return 0;
     }

     proc->label_datetime = wssearch_label(type_table, "DATETIME");
     if (!proc->emit_time) {
          proc->epoch_boundary.increment_ts = sysutil_get_duration_ts("60s");
     }
     proc->outtype_tuple =
          ws_register_source_byname(type_table, "TUPLE_TYPE", source_tuple, sv);

     if (proc->outtype_tuple == NULL) {
          fprintf(stderr, "registration failed\n");
          return 0;
     }


     return 1; 
}


proc_process_t proc_input_set(void * vinstance, wsdatatype_t * input_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {

     return NULL;
}


static int source_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     time_t sec = time(NULL);
     if (sec == proc->lasttime) {
          return 1;
     }
     proc->lasttime = sec;

     if (sysutil_test_time_boundary(&proc->epoch_boundary, sec - proc->offset) != 1) {
          return 1;
     }

     tuple_member_create_sec(input_data, sec, proc->label_datetime);

     proc->outcnt++;
     ws_set_outdata(input_data, proc->outtype_tuple, dout);

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

