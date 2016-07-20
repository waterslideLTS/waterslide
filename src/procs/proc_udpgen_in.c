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
#define PROC_NAME "udpgen_in"
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
char proc_purpose[]    = "opens up UDP socket and reads in buffers, decodes FIREHOSE parameters";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'U',"","port",
     "listen on UDP port",0,0},
     {'b',"","",
     "blocking port listen",0,0},
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

     char buf[MAXUDPBUF +1];
} listen_sock_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;
     uint64_t missed_frame;
     //uint64_t bad_record;

     uint64_t expected_frameid;

     ws_outtype_t * outtype_tuple;
     wslabel_t * label_key;
     wslabel_t * label_value;
     wslabel_t * label_bias;
     wslabel_t * label_frame;
     listen_sock_t * udp_sock;
     int blocking;
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

     //add nonblocking
     if (!proc->blocking) { 
          if (fcntl(proc->udp_sock->s, F_SETFL, FNDELAY) < 0) {
               perror("nonblocking - fcntl(FNDELAY)");
               return 1;
          }
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

     while ((op = getopt(argc, argv, "bU:")) != EOF) {
          switch (op) {
          case 'U':
               if (!register_socket(proc, atoi(optarg))) {
                    return 0;
               }
               break;
          case 'b':
               proc->blocking = 1;
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

     proc->label_key = wsregister_label(type_table, "KEY");
     proc->label_value = wsregister_label(type_table, "VALUE");
     proc->label_bias = wsregister_label(type_table, "BIAS");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     if (!proc->udp_sock) {
          tool_print("must specify a udp port to listen");
          return 0;
     }

     //do something cool with sockets
     proc->outtype_tuple =
          ws_register_source_byname(type_table, "TUPLE_TYPE",
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

static inline int read_csv_udp(proc_instance_t * proc, ws_doutput_t * dout) {
     wsdata_t * tdata;

     ///TODO-- READ in PKTS -- nonblocking
     int len = get_udp_data(proc,
                            proc->udp_sock->buf,
                            MAXUDPBUF);
     if (len <= 0) {
          //some sort of fatal error
          if (len < 0) {
               return 0;
          }
          // nonblocking retry
          else {
               return 1;
          }
     }
     char * buf = proc->udp_sock->buf;
     buf[len] = '\0';

     if (len < 100) {
          tool_print("frame too small");
          return 0;
     }

     char * sep;
     sep = strsep(&buf, "\n");

     if (!sep) {
          tool_print("no separator in pkt");
          return 0;
     }
     //check if frame count is detected:
     //if (strncmp(sep, "frame ", 6) != 0) {
     if (strncmp(sep, "packet ", 7) != 0) {
          tool_print("unexpected pkt");
          return 0;
     }
     proc->meta_process_cnt++;

     //read in 
     uint64_t frameid = (uint64_t)strtoul(sep+6, NULL, 10);

     if (frameid != proc->expected_frameid) {
          dprint("got %u, expected %u", (uint32_t)frameid,
                     (uint32_t)proc->expected_frameid);
          if (frameid > proc->expected_frameid) {
               proc->missed_frame+= frameid - proc->expected_frameid;
          }
     }
     proc->expected_frameid = frameid + 1;

     //parse buffer
     while (((sep = strsep(&buf, "\n")) != NULL) && (sep[0] != 0)) {
          char * pbuf = sep;
          uint64_t key = strtoul(strsep(&pbuf, ","),NULL, 10);
          uint32_t value = atoi(strsep(&pbuf, ","));
          uint32_t bias = atoi(pbuf);
               proc->outcnt++;
          tdata = ws_get_outdata(proc->outtype_tuple);
          if (tdata) {
               tuple_member_create_uint64(tdata, key, proc->label_key);
               tuple_member_create_uint(tdata, value, proc->label_value);
               tuple_member_create_uint(tdata, bias, proc->label_bias);
               ws_set_outdata(tdata, proc->outtype_tuple, dout);
               proc->outcnt++;
          }
         /* 
          uint64_t key;
          uint32_t value;
          uint32_t bias;
          if( sscanf(sep, "%"PRIu64",%u,%u", 
                     &key, &value, &bias) == 3 ) {
               tdata = ws_get_outdata(proc->outtype_tuple);
               if (tdata) {
                    tuple_member_create_uint64(tdata, key, proc->label_key);
                    tuple_member_create_uint(tdata, value, proc->label_value);
                    tuple_member_create_uint(tdata, bias, proc->label_bias);
                    ws_set_outdata(tdata, proc->outtype_tuple, dout);
                    proc->outcnt++;
               }
          }
          else {
               tool_print("bad record '%s'", sep);
               proc->bad_record++;
          }
          */
     }
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
     tool_print("missed frame cnt %" PRIu64, proc->missed_frame);
     tool_print("output cnt %" PRIu64, proc->outcnt);
     //tool_print("bad record cnt %" PRIu64, proc->bad_record);

     if (proc->udp_sock) {
          close(proc->udp_sock->s);
          free(proc->udp_sock);
     }

     //free dynamic allocations
     free(proc);

     return 1;
}

