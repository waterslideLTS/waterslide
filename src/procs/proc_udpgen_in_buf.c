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
#define PROC_NAME "udpgen_in_buf"
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
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "timeparse.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_double.h"

char proc_version[]     = "1.1";
char *proc_tags[]     = { "input", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "opens up UDP port, listens for frames, creates buffers for decoding downstream";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'n',"","count",
     "number of parallel generators",0,0},
     {'U',"","port",
     "listen on UDP port",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

//function prototypes for local functions
static int data_source_udp(void *, wsdata_t*, ws_doutput_t*, int);

#define PKTIN_FILENAME_MAX 1000

#define MAX_INLINE_LABELS 16

#define MAXUDPBUF 9000

typedef struct _listen_sock_t {
     int s; //the file descriptor
     uint16_t port;
     struct sockaddr_in6 sock_server;
     socklen_t socklen;
} listen_sock_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_string;
     listen_sock_t * udp_sock;
     uint64_t expected_frameid;
     uint64_t missed_frame;
     uint64_t out_cnt;
     uint64_t bad_record;
     int blocking;
     int generators;
     int shut;
     uint8_t *gen_shutdown;
} proc_instance_t;

static inline int register_socket(proc_instance_t * proc, uint16_t port) {
     if (proc->udp_sock) {
          tool_print("already have a udp socket");
          return 0;
     }
     proc->udp_sock = (listen_sock_t *)calloc(1, sizeof(listen_sock_t));
     if (!proc->udp_sock) {
          error_print("failed calloc of proc->udp_sock");
          return 0;
     }
     if ((proc->udp_sock->s = socket(AF_INET6, SOCK_DGRAM, IPPROTO_UDP)) == -1) {
          perror("socket()");
          free(proc->udp_sock);
          proc->udp_sock = NULL;
          return 0;
     }
     proc->udp_sock->port = port;

     proc->udp_sock->socklen = sizeof(struct sockaddr_in6);
     proc->udp_sock->sock_server.sin6_family = AF_INET6;
     proc->udp_sock->sock_server.sin6_port = htons(port);
     proc->udp_sock->sock_server.sin6_addr = in6addr_any;
     if(bind(proc->udp_sock->s, (struct sockaddr
                                 *)(&proc->udp_sock->sock_server),
             proc->udp_sock->socklen) == -1) {
          perror("bind()");
          free(proc->udp_sock);
          proc->udp_sock = NULL;
          return 0;
     }

     return 1;
}

static inline int get_udp_data(proc_instance_t * proc, char * buf, int buflen) {
     int len;

     struct sockaddr_in6 sock_client;
     socklen_t socklen = sizeof(struct sockaddr_in6);

     if (!proc->udp_sock) {
          return -1;
     }

     if ((len = recvfrom(proc->udp_sock->s, buf, buflen, 0,
                         (struct sockaddr*)&sock_client, &socklen)) == -1) {
          if (EAGAIN == errno) {
               return 0;
          }
          else {
               perror("recvfrom");
               return -1;
          }
     }

     //do stuff with resulting client stuff

     return len;
}

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;

     while ((op = getopt(argc, argv, "n:U:")) != EOF) {
          switch (op) {
          case 'n':
               proc->generators = atoi(optarg);
               break;
          case 'U':
               if (!register_socket(proc, atoi(optarg))) {
                    return 0;
               }
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

     proc->generators = 1;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     if (!proc->udp_sock) {
          tool_print("listening on port 5555");
          register_socket(proc, 5555);
          if (!proc->udp_sock) {
               return 0;
          }
     }

     proc->gen_shutdown = (uint8_t*)calloc(proc->generators, sizeof(uint8_t));
     if (!proc->gen_shutdown) {
          error_print("failed calloc of proc->gen_shutdown");
          return 0;
     }

     //do something cool with sockets
     proc->outtype_string =
          ws_register_source_byname(type_table, "STRING_TYPE",
                                    data_source_udp, sv);

     
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

static inline int try_shutdown(proc_instance_t * proc, char * buf, int buflen) {
     if (!buflen) {
          return 1;
     }
     int i = atoi(buf);
     if (i < proc->generators){
          if (!proc->gen_shutdown[i]) {
               proc->shut++;
               proc->gen_shutdown[i] = 1;
               tool_print("shut down generator %d", i);
               if (proc->shut == proc->generators) {
                    return 0;
               }
          }
     }
     return 1;
}

static inline int read_csv_udp(proc_instance_t * proc, ws_doutput_t * dout) {
     int plen;
     char * pbuf;
     proc->meta_process_cnt++;

     wsdata_t * wsbuf = wsdata_create_buffer(MAXUDPBUF, &pbuf, &plen);
     if (!wsbuf) {
          tool_print("unable to allocate buffer");
          return 0;
     }
     wsdata_t * wsstr = wsdata_alloc(dtype_string);
     if (!wsstr) {
          tool_print("unable to allocate buffer");
          wsdata_delete(wsbuf);
          return 0;
     }
     wsdata_assign_dependency(wsbuf, wsstr);
     wsdt_string_t * str = (wsdt_string_t *)wsstr->data;
     str->buf = pbuf;


     int len = get_udp_data(proc,
                            pbuf,
                            MAXUDPBUF);
     if (len <= 0) {
          wsdata_delete(wsstr);
          //some sort of fatal error
          if (len < 0) {
               return 0;
          }
          // nonblocking retry
          else {
               return 1;
          }
     }
     str->len = len;

     pbuf[len-1] = '\0';

     if (len < 100) {
          int rtn = try_shutdown(proc, pbuf, len);
          wsdata_delete(wsstr);
          return rtn;
     }

     //check if frame count is detected:
     //if (memcmp(pbuf, "frame ", 6) != 0) {
     if (memcmp(pbuf, "packet ", 7) != 0) {
          tool_print("unexpected pkt");
          wsdata_delete(wsstr);
          return 0;
     }

     ws_set_outdata(wsstr, proc->outtype_string, dout);
     proc->outcnt++;
     return 1;
}

static int data_source_udp(void * vinstance, wsdata_t* source_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     return read_csv_udp(proc, dout);
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("frame cnt   %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     if (proc->udp_sock) {
          close(proc->udp_sock->s);
          free(proc->udp_sock);
     }

     //free dynamic allocations
     if (proc->gen_shutdown) {
          free(proc->gen_shutdown);
     }
     free(proc);

     return 1;
}

