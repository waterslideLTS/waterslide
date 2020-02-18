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
//#define DEBUG 1
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
#include <limits.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "wstypes.h"
#include "popen2.h"
#include "sysutil.h"
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
          "separator for input (default:newline)",0,0},
     {'I',"","",
          "include separator in output",0,0},
     {'S',"","",
          "detect ascii strings",0,0},
     {'L',"","",
          "label of input",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
          "",0,0}
};

#define EXEC_BUF_MAX PIPE_BUF

//function prototypes for local functions
static int data_source_exec(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _buffer_list_t {
     wsdata_t * wsd;
     char * buf;
     int len;
     int remainder;
     struct _buffer_list_t * next;
} buffer_list_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;
     //uint64_t bad_record;
     buffer_list_t * freeq;
     buffer_list_t * active;
     buffer_list_t * active_tail;

     ws_outtype_t * outtype_tuple;
     wslabel_t * label_out;

     int isdone;
     int detect_strings;

     struct pollfd fds[1];

     char * sep;
     int sep_len;

     int include_sep;

     char * command;

     pid_t pid;
     int input_fd;
     int output_fd;
} proc_instance_t;


static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {

     int op;

     while ((op = getopt(argc, argv, "SIL:R:")) != EOF) {
          switch (op) {
          case 'S':
               proc->detect_strings = 1;
               break;
          case 'I':
               proc->include_sep = 1;
               break;
          case 'L':
               proc->label_out = wsregister_label(type_table, optarg);
               break;
          case 'R':
               proc->sep = strdup(optarg);
               proc->sep_len = strlen(optarg);
               sysutil_decode_hex_escapes(proc->sep, &proc->sep_len); 
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

     proc->sep = "\n";
     proc->sep_len = 1;

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

static void add_to_active(proc_instance_t * proc,
                          wsdata_t* wsd,
                          char * buf, int len,
                          int remainder) {
     dprint("add_to_active ; len %d, remainder %d", len, remainder);
     if (proc->active_tail && (proc->active_tail->wsd == wsd)) {
          dprint("reset active len and offset");
          proc->active_tail->buf = buf;
          proc->active_tail->len = len;
          proc->active_tail->remainder = remainder;
     }
     else {
          dprint("creating new node");
          buffer_list_t * node;
          if (proc->freeq) {
               node = proc->freeq;
               proc->freeq = node->next;
          }
          else {
               node = (buffer_list_t*)malloc(sizeof(buffer_list_t));
               if (!node) {
                    dprint("error allocating buffer");
                    return;
               }
          }
          dprint("creating new node %x", node);
          node->wsd = wsd;
          node->buf = buf;
          node->len = len;
          node->remainder = remainder;
          node->next = NULL;
          if (proc->active_tail) {
               proc->active_tail->next = node;
               proc->active_tail = node;
          }
          else {
               proc->active = node;
               proc->active_tail = node;
          }
     }
}

static void recycle_cursor(proc_instance_t * proc,
                           buffer_list_t * cursor) {
     dprint("recycle_cursor");
     if (cursor == proc->active) {
          proc->active = cursor->next;
          if (proc->active == NULL) {
               proc->active_tail = NULL;
          }
     }
     wsdata_delete(cursor->wsd);
     cursor->wsd = NULL;
     cursor->next = proc->freeq;
     proc->freeq = cursor;
}

static int aggregate_output(proc_instance_t * proc,
                             wsdata_t* wsd,
                             char * buf, int len,
                             ws_doutput_t * dout) {
     dprint("aggregate_output, len %d", len);
     buffer_list_t * cursor;
     buffer_list_t * next;
     int segments = 0;
     int agglen = 0;

     //get overall length;
     //get segments
     for (cursor = proc->active; cursor; cursor = cursor->next) {
          dprint("potential segment %d", cursor->len);
          dprint("cursor %x", cursor);
          if (cursor->wsd != wsd) {
               segments++;
               agglen += cursor->len;
               dprint("segment %d", cursor->len);
          }
     }
     if (segments && !agglen) {
          //some null active set
          cursor = proc->active;
          while (cursor) {
               next = cursor->next;
               if (cursor->wsd != wsd) {
                    recycle_cursor(proc, cursor);
               }
               cursor = next;
          }
     }
     
     agglen += len;
     dprint("agglen %d", agglen);

     if (!agglen) {
          return 0;
     }

     wsdata_t * agg = NULL;
     char * agg_buf = NULL;
     int offset = 0;
     if (!segments) {
          agg = wsd;
          agg_buf = buf;
     }
     else {
          int alen;
          agg = wsdata_create_buffer(agglen, &agg_buf, &alen);
          if (!agg) {
               return 0;
          }
          wsdata_add_reference(agg);
          cursor = proc->active;
          while (cursor) {
               next = cursor->next;
               if (cursor->wsd != wsd) {
                    if (cursor->len) {
                         memcpy(agg_buf + offset, cursor->buf, cursor->len); 
                         offset += cursor->len;
                    }
                    recycle_cursor(proc, cursor);
               }
               cursor = next;
          }
          if (len && ((len + offset) <= agglen)) {
               memcpy(agg_buf + offset, buf, len); 
          }
          dprint("offset %d", offset);
     }
     wsdata_t * tuple = wsdata_alloc(dtype_tuple);
     if (tuple) {
          if (proc->detect_strings) {
               tuple_member_create_dep_strdetect(tuple, agg, proc->label_out,
                                                 agg_buf, agglen);
          }
          else {
               tuple_member_create_dep_binary(tuple, agg, proc->label_out,
                                              agg_buf, agglen);
          }

          ws_set_outdata(tuple, proc->outtype_tuple, dout);     
          proc->outcnt++;
     }
     if (agg != wsd) {
          wsdata_delete(agg);
     }
     return 1;
}

static void local_detect_sep(proc_instance_t * proc,
                             wsdata_t* wsd,
                             char * buf, int len,
                             int remainder,
                             ws_doutput_t * dout) {
     dprint("detecting sep len %d, remainder %d", len, remainder);

     if (proc->active_tail && (proc->active_tail->wsd == wsd)) {
          dprint("reset active len and offset");
          proc->active_tail->buf = buf;
          proc->active_tail->len = len;
          proc->active_tail->remainder = remainder;
     }

     while (len) {
          char * buf_offset = (char *)memmem(buf, len, proc->sep, proc->sep_len);
          if (!buf_offset) {
               add_to_active(proc, wsd, buf, len, remainder);
               return;
          }
          int leftover = buf_offset - buf;
          if (proc->include_sep) {
               aggregate_output(proc, wsd, buf, leftover + proc->sep_len, dout);
          }
          else {
               aggregate_output(proc, wsd, buf, leftover, dout);
          }

          int skip = leftover + proc->sep_len;
          buf += skip;
          len -= skip;
     }

     // if exited here
     if (proc->active_tail && (proc->active_tail->wsd == wsd)) {
          //free active tail..
          recycle_cursor(proc, proc->active_tail);
     }
     else {
          wsdata_delete(wsd);
     }
}

static int data_source_exec(void * vinstance, wsdata_t* source_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (proc->isdone) {
          return 0;
     }

     //dprint("ready to poll");
     int rc = poll(proc->fds, 1, 1);

     if (rc < 0) {
          dprint("poll -1");
          proc->isdone = 1;
          return aggregate_output(proc, NULL, NULL, 0, dout);
     }
     else if (rc==0) {
          //dprint("poll 0");

          int status = 0;
          int r = waitpid(proc->pid, &status, WNOHANG); 
          if (r == 0) {
               return 1;
          }
          
          dprint("pid %d status %d, %d", (int)proc->pid, status, r);

          if ((r == -1) || WIFEXITED(status)) {
               dprint("exited");
               proc->isdone = 1;
               return aggregate_output(proc, NULL, NULL, 0, dout);
          }
          return 1;
     }
     else if (proc->fds[0].revents != POLLIN) {
          dprint("no events");
          proc->isdone = 1;
          return aggregate_output(proc, NULL, NULL, 0, dout);
     }

     //allocate data to read..
     char * buf = NULL;
     int len = 0;
     int rollover = 0;

     wsdata_t * wsd = NULL;
     dprint("allocating data");
     if (proc->active_tail) {
          dprint("found active_tail");
          if (proc->active_tail->remainder) {
               dprint("found remainder");
               rollover = proc->active_tail->len;
               buf = proc->active_tail->buf;
               len = rollover + proc->active_tail->remainder;
               wsd = proc->active_tail->wsd;
          }
          else {
               dprint("do rollover");
               wsd = wsdata_create_buffer(EXEC_BUF_MAX, &buf, &len);
               if (!wsd) {
                    tool_print("unable to allocate buffer");
                    return 0;
               }
               wsdata_add_reference(wsd);
               rollover = proc->sep_len - 1;
               if (proc->active_tail->len < rollover) {
                    rollover = proc->active_tail->len;
               }
               if (rollover) {
                    int offset = proc->active_tail->len - rollover;
                    memcpy(buf, proc->active_tail->buf + offset, rollover);
                    proc->active_tail->len -= rollover;
               }
          }
     }
     else {
          dprint("create new");
          wsd = wsdata_create_buffer(EXEC_BUF_MAX, &buf, &len);
          if (!wsd) {
               tool_print("unable to allocate buffer");
               return 0;
          }
          wsdata_add_reference(wsd);
     }

     dprint("allocating reading buffer");
     int rlen = read(proc->output_fd, buf + rollover, len - rollover);
     if (rlen <= 0) {
          wsdata_delete(wsd);
          tool_print("unable to read buffer");
          proc->isdone = 1;
          return 0;
     }
     proc->meta_process_cnt++;
     rlen += rollover;

     local_detect_sep(proc, wsd, buf, rlen, len - rlen, dout);
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

