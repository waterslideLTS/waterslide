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
#define PROC_NAME "exec_in"
#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <poll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <signal.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "wstypes.h"
#include "popen2.h"
#include "datatypes/wsdt_tuple.h"

char proc_version[]     = "1.1";
char *proc_tags[]     = { "input", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "exec a process, read output as input to waterslide";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'R',"","string",
     "delimiter for input (default:newline)",0,0},
     {'L',"","",
     "label of input",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

#define EXEC_BUF_MAX 2048

//function prototypes for local functions
static int data_source_exec(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _buffer_list_t {
     wsdata_t * wsd;
     uint8_t * buf;
     int len;
     struct _buffer_list_t * next;
} buffer_list_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;
     //uint64_t bad_record;
     buffer_list_t * freeq;
     buffer_list_t * active;

     ws_outtype_t * outtype_tuple;
     wslabel_t * label_out;

     int isdone;

     struct pollfd fds[1];

     char * delim;
     int delim_len;

     char * command;

     pid_t pid;
     int input_fd;
     int output_fd;
} proc_instance_t;


static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;

     while ((op = getopt(argc, argv, "L:R:")) != EOF) {
          switch (op) {
          case 'L':
               proc->label_out = wsregister_label(type_table, optarg);
               break;
          case 'R':
               proc->delim = strdup(optarg);
               proc->delim_len = strlen(optarg);
               break;
          default:
               return 0;
          }
     }
     int cmdlen = 0;
     int i;
     for (i = optind ; i < argc ; i++) {
          if (i != optind) {
               cmdlen++;
          }
          cmdlen += strlen(argv[i]);
     }
     if (!cmdlen) {
          tool_print("need to specify command");
          return 0;
     }
     proc->command = calloc(1, cmdlen + 1);
     if (!proc->command) {
          tool_print("error allocating command array");
          return 0;
     }
     int offset = 0;
     for (i = optind ; i < argc ; i++) {
          if (i != optind) {
               proc->command[offset] = ' ';
               offset++;
          }
          int slen = strlen(argv[i]);
          memcpy(proc->command + offset, argv[i], slen);
          offset += slen;
     }

     dprint("command: %s", proc->command);

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

     proc->label_out = wsregister_label(type_table, "EXEC");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     proc->pid = popen2(proc->command, &proc->input_fd, &proc->output_fd);
     if (proc->pid == -1) {
          tool_print("unable to exec command");
          return 0;
     }

     proc->fds[0].fd = proc->output_fd;
     proc->fds[0].events = POLLIN;

     //do something cool with sockets
     proc->outtype_tuple =
          ws_register_source_byname(type_table, "TUPLE_TYPE",
                                    data_source_exec, sv);

     
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



static int data_source_exec(void * vinstance, wsdata_t* source_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (proc->isdone) {
          return 0;
     }

     dprint("ready to poll");
     int rc = poll(proc->fds, 1, 1);

     if (rc < 0) {
          dprint("poll -1");
          proc->isdone = 1;
          return 0;
     }
     else if (rc==0) {
          dprint("poll 0");

          int status = 0;
          int r = waitpid(proc->pid, &status, WNOHANG); 
          if (r == 0) {
               return 1;
          }
          
          dprint("pid %d status %d, %d", (int)proc->pid, status, r);

          if ((r == -1) || WIFEXITED(status)) {
               dprint("exited");
               proc->isdone = 1;
               return 0;
          }
          return 1;
     }
     else if (proc->fds[0].revents != POLLIN) {
          dprint("no events");
          proc->isdone = 1;
          return 0;
     }

     //allocate data to read..
     char * buf = NULL;
     int len = 0;

     dprint("allocating data");
     wsdata_t * wsd = wsdata_create_buffer(EXEC_BUF_MAX, &buf, &len);
     if (!wsd) {
          tool_print("unable to allocate buffer");
          return 0;
     }
     dprint("allocating reading buffer");
     int rlen = read(proc->output_fd, buf, len);
     if (rlen <= 0) {
          wsdata_delete(wsd);
          tool_print("unable to read buffer");
          proc->isdone = 1;
          return 0;
     }

     proc->meta_process_cnt++;
     wsdata_t * tuple = wsdata_alloc(dtype_tuple);
     if (!tuple) {
          wsdata_delete(wsd);
          tool_print("unable to read buffer");
          proc->isdone = 1;
          return 0;
     }
     tuple_member_create_dep_binary(tuple, wsd, proc->label_out,
                                    buf, rlen);

     ws_set_outdata(tuple, proc->outtype_tuple, dout);     
     proc->outcnt++;
     return 1;
}

static void free_buffer_list(buffer_list_t * head) {
     buffer_list_t * cursor = head;
     buffer_list_t * next = NULL;
     while(cursor) {
          next = cursor->next;
          if (cursor->wsd) {
               wsdata_delete(cursor->wsd);
          }
          free(cursor);
          cursor = next;
     }
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("input frames %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);
     //tool_print("bad record cnt %" PRIu64, proc->bad_record);

     if (proc->pid > 0) {
          close(proc->input_fd);
          close(proc->output_fd);
          kill(proc->pid, SIGTERM);
     }
     free_buffer_list(proc->active);
     free_buffer_list(proc->freeq);

     //free dynamic allocations
     free(proc);

     return 1;
}

