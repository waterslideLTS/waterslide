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
//catch buffers of tcp messages from a tcp thrower application..
//  in terms of a client-server model, this is a client that connects to a
//  server
#define PROC_NAME "tcpcatch"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
//#include <stropts.h>
#include <poll.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_double.h"
#include "tcp_rw.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  {"source", "input", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "reads from tcp server, creates binary data for parsing";
char proc_description[] = "Catches TCP data at a server port and uses it as the source for a subsequent pipeline.  The tcpcatch kid is usually paired with a tcpthrow running in a separately running pipeline.  While tcpcatch is normally run in a client-mode (receiving data from a server), the mode can be reversed with the '-s' option.  The '-h' and '-p' options allow specification of where to catch the incoming data.  The '-w' and '-b' options are used to initialize state on the socket.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'s',"","server",
      "Run in server mode (default is client mode)", 0, 0},
     {'h',"","hostname",
      "connect to server at hostname",0,0},
     {'p',"","port",
     "Use listening port",0,0},
     {'w',"","",
     "dont wait till data is available",0,0},
     {'b',"","",
      "Use blocking sockets (default: non-blocking)", 0,0}, 
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "";
char *proc_input_types[]       =  {NULL};
// (Potential) Output types: bin
char *proc_output_types[]      =  {"any", NULL};
char proc_requires[]           =  "tcpthrow should be used in a separately running process";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  {"tcpcatch -h <hostname> -p <port> [-s] [-w] [-b]", NULL};
proc_example_t proc_examples[] =  {
	{"tcpcatch -h localhost -p 5555 | ...", "catch data sent by a separately running process to localhost:5555 and utilize normal event processing in the subsequent pipeline."},
	{NULL, NULL}
};

//function prototypes for local functions
static int data_source(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t loops;
     ws_outtype_t * outtype_bin;
     tcp_catch_t * tcpc;
     char * hostname;
     int wait;
     uint16_t port;
     int blocking;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;
     int server = 0;

     while ((op = getopt(argc, argv, "bswp:h:")) != EOF) {
          switch (op) {
          case 'b':
               proc->blocking = 1;
               break;
          case 's':
               server = 1;
               break;
          case 'h':
               proc->hostname = strdup(optarg);
               break;
          case 'p':
               proc->port = atoi(optarg);
               break;
          case 'w':
               proc->wait = 0;
               break;
          default:
               return 0;
          }
     }

     if (!server && NULL == proc->hostname) {
          return 0;
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

     proc->port = 1445;
     proc->wait = 1;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     proc->outtype_bin =
          ws_register_source_byname(type_table, "BINARY_TYPE", data_source, sv);

     proc->tcpc = tcp_catch_init(proc->hostname, proc->port, proc->blocking);
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
     return NULL;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
//
static int data_source(void * vinstance, wsdata_t* source_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->loops++;
     proc->meta_process_cnt += tcp_catch_data(proc->tcpc, proc->outtype_bin,
                                       source_data, dout);
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("output cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("polling loops cnt %" PRIu64, proc->loops);

     tcp_catch_destroy(proc->tcpc);

     //free dynamic allocations
     free(proc->hostname);
     free(proc);

     return 1;
}


