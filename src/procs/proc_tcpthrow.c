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
//throw buffers of tcp messages from a tcp thrower application..
//  in terms of a client-server model, this is a server that receives
//  connections
#define PROC_NAME "tcpthrow"
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
char *proc_tags[]              =  {"source", "output", "stream manipulation", NULL};
char *proc_alias[]             =  { NULL };
char proc_purpose[]            =  "writes tcp messages, sends to connected clients";
char proc_description[] = "Throws data from the current pipeline to a socket that generates TCP packets to transport the data through the specified port.  The tcpthrow kid is typically used in conjunction with a tcpcatch that is running in a separately running process.  The tcpthrow is normally run in a server-mode, sending data to a separate client that is receiving data.  This mode can be reversed using the '-c' option along with the '-h' option to specify the host in the reversed mode.  The '-p' option must be specified and will open the specified port on the machine running the pipeline.  Additional options include '-w' for specifying a sleep time before sending data, '-b' for initializing the socket in a blocking mode', and the '-R' and '-v' options for controlling output reporting.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'c',"", "",
     "Run in client mode (default is server mode)", 0, 0},
     {'p',"","port",
     "Use listening port",0,0},
     {'h',"","hostname",
     "Connect to hostname when in client mode",0,0},
     {'w',"","sec",
     "wait w seconds before starting",0,0},
     {'b',"","",
      "Use blocking sockets (default: non-blocking)",0,0},
     {'R',"","cnt",
      "number of frames before reporting status",0,0},
     {'v',"","",
      "verbose output",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "";
char *proc_input_types[]       =  {"any", NULL};
// (Potential) Output types: 
char *proc_output_types[]      =  {NULL};
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  {"tcpthrow -p <port> [-c -h <hostname>] [-w <value>] [-b] [-R <cnt>] [-v]", NULL};
proc_example_t proc_examples[] =  {
	{"... | tcpthrow -p 5555", "Throws data generated in previous pipeline via port 5555 on the localhost using TCP packets and a separately running process that is listening on port 5555."},
	{NULL, NULL}
};

static int proc_binary(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     tcp_throw_t * tcpt;
     uint16_t port;
     char* hostname;
     int wait;
     int blocking;
     int cntReportInterval;
     uint64_t nextReportCnt;
     struct timeval last_time;
     uint64_t last_cnt;
     uint64_t last_dropped;
     uint64_t last_sent;
     int verbose;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;
     int client = 0;

     while ((op = getopt(argc, argv, "bcw:p:h:R:v")) != EOF) {
          switch (op) {
          case 'b':
               proc->blocking = 1;
               break;
          case 'c':
               client = 1;
               break;
          case 'w':
               proc->wait = atoi(optarg);
               break;
          case 'p':
               proc->port = atoi(optarg);
               break;
          case 'h':
               proc->hostname = optarg;
               break;
          case 'R':
               proc->cntReportInterval = atoi(optarg);
               if (!proc->verbose) { proc->verbose = 1; }
               break;
          case 'v':
               proc->verbose++;
               break;
          default:
               return 0;
          }
     }

     if (client && NULL == proc->hostname) {
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
     proc->wait = 4;
     proc->cntReportInterval = 1000000;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     proc->tcpt = tcp_throw_init(proc->hostname, proc->port, proc->blocking);
     if (!proc->tcpt) {
          return 0;
     }

     if (NULL == proc->hostname && proc->wait) {
          tool_print("waiting %d seconds for catchers", proc->wait);
          sleep(proc->wait);
          tool_print("done waiting");
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
     //accept only binary datatypes
     if (wsdatatype_match(type_table, input_type, "BINARY_TYPE")) {
          return proc_binary;
     }

     return NULL;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
//
static int proc_binary(void * vinstance, wsdata_t* input_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     if (proc->verbose &&
         proc->meta_process_cnt >= proc->nextReportCnt) {
          struct timeval ctime;
          gettimeofday(&ctime, 0);

          if (proc->meta_process_cnt > 1) {
               double sec = ctime.tv_sec - proc->last_time.tv_sec + 
                    ((double)(ctime.tv_usec - proc->last_time.tv_usec)) /
                    1000000.0;
               double rate = (proc->meta_process_cnt - proc->last_cnt) / sec;
               tool_print("DUMP: %"PRIu64" records, %"PRIu64" sent, %"PRIu64
                          " dropped in %0.2f sec (%lf records/sec)",
                          proc->meta_process_cnt - proc->last_cnt,
                          proc->tcpt->sent - proc->last_sent,
                          proc->tcpt->dropped - proc->last_dropped,
                          sec, rate);
          }

          proc->last_cnt = proc->meta_process_cnt;
          proc->last_sent = proc->tcpt->sent;
          proc->last_dropped = proc->tcpt->dropped;
          proc->last_time = ctime;
          proc->nextReportCnt = proc->meta_process_cnt +
               proc->cntReportInterval;
     }

     tcp_throw_data(proc->tcpt, input_data);
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("sent cnt %" PRIu64, proc->tcpt->sent);
     tool_print("dropped cnt %" PRIu64 ", %.2f%%", proc->tcpt->dropped,
                100.0 * (float)proc->tcpt->dropped / (float)
                proc->meta_process_cnt);
     tcp_throw_destroy(proc->tcpt);

     //free dynamic allocations
     free(proc);

     return 1;
}


