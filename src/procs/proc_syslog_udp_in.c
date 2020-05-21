// syslog_udp_in - 
//  supports receiving Syslog Messages over UDP (RFC 5426)
/*
Copyright 2020 Morgan Stanley

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

#define PROC_NAME "syslog_udp_in"
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

char proc_version[]     = "1.1";
char *proc_tags[]     = { "input", NULL };
char *proc_alias[]     = { "syslogudp", "syslog_udp", "udp_syslog", "udpsyslog", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "opens up UDP port, listens for syslog messages";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'U',"","port",
     "listen on UDP port (default:1514)",0,0},
     {'B',"","",
     "Block on UDP listen (non-blocking default)",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

//function prototypes for local functions
static int data_source_udp(void *, wsdata_t*, ws_doutput_t*, int);

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

     wslabel_t * label_datetime;
     wslabel_t * label_message;
     wslabel_t * label_source_ip;
     wslabel_t * label_source_port;
     wslabel_t * label_syslog;

     uint16_t port;
     int blocking;

     ws_outtype_t * outtype_tuple;
     listen_sock_t * udp_sock;
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

static inline int get_udp_data(proc_instance_t * proc, char * buf, int buflen,
                               struct sockaddr_in6 * psock_client, socklen_t * psocklen) {
     int len;

     if (!psock_client || !psocklen) {
          struct sockaddr_in6 sock_client;
          socklen_t socklen = sizeof(struct sockaddr_in6);
          psock_client = &sock_client;
          psocklen = &socklen;
     }

     if (!proc->udp_sock) {
          return -1;
     }

     if ((len = recvfrom(proc->udp_sock->s, buf, buflen, 0,
                         (struct sockaddr*)psock_client, psocklen)) == -1) {
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

     while ((op = getopt(argc, argv, "bBu:U:")) != EOF) {
          switch (op) {
          case 'b':
          case 'B':
               proc->blocking = 1;
               break;
          case 'u':
          case 'U':
               proc->port = atoi(optarg);
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


     proc->label_datetime = wsregister_label(type_table, "DATETIME");
     proc->label_message = wsregister_label(type_table, "MESSAGE");
     proc->label_source_ip = wsregister_label(type_table, "SOURCE_IP");
     proc->label_source_port = wsregister_label(type_table, "SOURCE_PORT");
     proc->label_syslog = wsregister_label(type_table, "SYSLOG");

     proc->port = 1514;

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     if (!register_socket(proc, proc->port)) {
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
     int plen;
     char * pbuf;

     wsdata_t * wsbuf = wsdata_create_buffer(MAXUDPBUF, &pbuf, &plen);
     if (!wsbuf) {
          tool_print("unable to allocate buffer");
          return 0;
     }

     struct sockaddr_in6 sock_client;
     socklen_t socklen = sizeof(struct sockaddr_in6);

     int len = get_udp_data(proc,
                            pbuf,
                            MAXUDPBUF,
                            &sock_client, &socklen);
     if (len <= 0) {
          wsdata_delete(wsbuf);
          //some sort of fatal error
          if (len < 0) {
               return 0;
          }
          // nonblocking retry
          else {
               return 1;
          }
     }
     proc->meta_process_cnt++;

     wsdata_t * tuple = wsdata_alloc(dtype_tuple);
     if (!tuple) {
          tool_print("unable to allocate tuple");
          wsdata_delete(wsbuf);
          return 0;
     }
     wsdata_add_label(tuple, proc->label_syslog);

     //otherwise we now have a syslog buffer
     wsdata_t * wsstr = wsdata_alloc(dtype_string);
     if (!wsstr) {
          tool_print("unable to allocate buffer");
          wsdata_delete(wsbuf);
          wsdata_delete(tuple);
          return 0;
     }
     wsdata_assign_dependency(wsbuf, wsstr);
     wsdt_string_t * str = (wsdt_string_t *)wsstr->data;
     str->buf = pbuf;
     str->len = len;
     if (str->len && str->buf[str->len-1] == '\n') {
          str->len--;
     }
     wsdata_add_label(wsstr, proc->label_message);


     struct timeval current;
     gettimeofday(&current, NULL);
     wsdt_ts_t ts;
     ts.sec = current.tv_sec;
     ts.usec = current.tv_usec;

     tuple_member_create_ts(tuple, ts, proc->label_datetime);
     add_tuple_member(tuple, wsstr);

     char srcout[INET6_ADDRSTRLEN];
     const char* result = inet_ntop(AF_INET6, (const void *)&sock_client.sin6_addr,
                                    srcout, INET6_ADDRSTRLEN);
     if (!result) {
          return 0;
     }
     tuple_dupe_string(tuple, proc->label_source_ip, result, strlen(result));

     tuple_member_create_uint(tuple, ntohs(sock_client.sin6_port), proc->label_source_port);


     ws_set_outdata(tuple, proc->outtype_tuple, dout);
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

     return 1;
}

