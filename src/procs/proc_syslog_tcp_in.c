// syslog_tcp_in - 
//  supports receiving Syslog Messages over TLS
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

#define PROC_NAME "syslog_tcp_in"
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

#ifndef NOTLS
#include "openssl/ssl.h"
#include "openssl/err.h"
#endif

#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "timeparse.h"
#include "signal.h"
#include "datatypes/wsdt_tuple.h"

char proc_version[]     = "1.1";
char *proc_tags[]     = { "input", NULL };
char *proc_alias[]     = { "syslog_tcp", "syslogtcp", "syslog_tls", "syslogtls",
     "tls_in", "tcp_in", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "opens up TCP/TLS port, listens for syslog messages";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'P',"","port",
     "listen on TCP port (default:1514)",0,0},
     {'B',"","",
     "Block on TCP listen (non-blocking default)",0,0},
     {'I',"","ip address",
     "bind server to a local ip address",0,0},
#ifndef NOTLS
     {'C',"","cert_pem_file",
     "Attempt to use TLS with cert file in pem format",0,0},
     {'K',"","key_pem_file",
     "Attempt to use TLS with key file in pem format",0,0},
     {'M',"","ca_pem file",
     "Enforce client certificates signed by ca_pem file",0,0},
     {'m',"","ca_pem directory",
     "Enforce client certificates signed by CA pem files within a directory",0,0},
#endif
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};


#define MAX_INLINE_LABELS 16

#define MAXTCPBUF 9000

#define MAX_PARTIAL (131072)
#define MAX_SESSION (255)
typedef struct _tcp_session_t {
     int sd;
     char * partial;
     int partial_len;
     wsdata_t * client_ip;
     uint16_t client_port;
     struct _tcp_session_t * next;
#ifndef NOTLS
     SSL * ssl;
     wsdata_t * subj;
#endif
} tcp_session_t;

typedef void (*tcp_session_callback)(void *, tcp_session_t *, char *, int);

#define MAXRECV (2048)
typedef struct _tcp_server_t {
     int sd;
     struct sockaddr_in6 sock_server;
     socklen_t socklen;
     tcp_session_t * active; //linked list of clients
     tcp_session_t * freeq; 
     int num_session;
     uint16_t port;
     int blocking;
     int num_clients;
     char recv[MAXRECV];
     void * cbdata;
     tcp_session_callback callback;
     wslabel_t * label_client_ip;
     int do_tls;
#ifndef NOTLS
     SSL_CTX * ctx;
     int verify_client_cert;
     wslabel_t * label_subj;
#endif
} tcp_server_t;

static tcp_server_t * tcp_server_create(uint16_t port,
                                        tcp_session_callback callback,
                                        void * cbdata,
                                        wslabel_t * label_client_ip,
                                        int blocking,
                                        char * bindip) {
     tcp_server_t * server = (tcp_server_t *)calloc(1, sizeof(tcp_server_t));
     if (!server) {
          error_print("failed calloc of proc->server");
          return NULL;
     }
     server->sd = socket(AF_INET6, SOCK_STREAM, IPPROTO_TCP);
     if (server->sd < 0) {
          perror("socket");
          return NULL;
     }
     server->callback = callback;
     server->cbdata = cbdata;
     server->label_client_ip = label_client_ip;

     server->port = port;
     server->blocking = blocking;

     server->socklen = sizeof(struct sockaddr_in6);
     server->sock_server.sin6_family = AF_INET6;
     server->sock_server.sin6_port = htons(port);


     struct in6_addr ip6;
     if (bindip) {
          //check if ipv4:
          int res;
          int blen = strlen(bindip);
          if ((memchr(bindip, ':', blen) == NULL) && 
              (memchr(bindip, '.', blen) != NULL)) {
               //append ipv6 header
               if ((blen + 8) > INET6_ADDRSTRLEN) {
                    //invalid string
                    tool_print("invalid ipv4 string %s", bindip);
                    return NULL;
               }
               char abuf[INET6_ADDRSTRLEN];
               memcpy(abuf, "::FFFF:", 7);
               memcpy(abuf+ 7, bindip, blen);
               abuf[7+blen] = '\0';

               res = inet_pton(AF_INET6, abuf, &ip6);
          }
          else {
               res = inet_pton(AF_INET6, bindip, &ip6);
          }
          if (res <= 0) {
               tool_print("unable to bind to ip %s", bindip);
               return NULL;
          }
          server->sock_server.sin6_addr = ip6;
     }
     else {
          server->sock_server.sin6_addr = in6addr_any;
     }

     /* set reuse flag */
     int val = 1;
     int ret;
     ret = setsockopt(server->sd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
     if (ret < 0) {
          perror("setsockopt");
          close(server->sd);
          return NULL;
     }

     /* make non-blocking (always!) for accepting new connections */
     ret = fcntl(server->sd, F_SETFL, FNDELAY);
     if (ret < 0) {
          perror("fcntl(..., FNDELAY)");
          close(server->sd);
          return NULL;
     }

     /* Finish initialization */
     ret = bind(server->sd, 
                (struct sockaddr *)(&server->sock_server),
                server->socklen);
     if (ret < 0) {
          perror("bind");
          close(server->sd);
          return NULL;
     }

     ret = listen(server->sd, MAX_SESSION);
     if (ret < 0) {
          perror("listen");
          close(server->sd);
          return NULL;
     }

     //ignore SIGPIPE so process does not die on dropped connections
     //signal(SIGPIPE, SIG_IGN);

     return server;
}
static int tcp_server_init_tls(tcp_server_t * server, char * certfile,
                               char * keyfile) {
#ifndef NOTLS
     SSL_load_error_strings();
     const SSL_METHOD * method;
     method = TLS_server_method();
     server->ctx = SSL_CTX_new(method);

     if (!server->ctx) {
          perror("unable to create TLS context");
          ERR_print_errors_fp(stderr);
          return 0;

     }
     SSL_CTX_set_min_proto_version(server->ctx, TLS1_2_VERSION);
     SSL_CTX_set_ecdh_auto(server->ctx, 1);
     /* Set the key and cert */
     if (SSL_CTX_use_certificate_file(server->ctx, certfile, SSL_FILETYPE_PEM) <= 0) {
          ERR_print_errors_fp(stderr);
          return 0;
     }

     if (SSL_CTX_use_PrivateKey_file(server->ctx, keyfile, SSL_FILETYPE_PEM) <= 0 ) {
          ERR_print_errors_fp(stderr);
          return 0;
     }
     server->do_tls = 1;
     return 1;

#else
     tool_print("tls services not compiled");
     return 0;
#endif
}

static int tcp_server_init_clientCA(tcp_server_t * server, 
                                    const char * cafile,
                                    const char * capath,
                                    wslabel_t * label_subj) {

#ifndef NOTLS
     server->label_subj = label_subj;
     int ret = SSL_CTX_load_verify_locations(server->ctx, cafile,
                                             capath);
     
     if (ret <= 0) { 
          ERR_print_errors_fp(stderr);
          return 0;
     }
     server->verify_client_cert = 1;

     SSL_CTX_set_verify(server->ctx,
                        SSL_VERIFY_PEER|SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                        NULL);
     return 1;
#else
     tool_print("tls services not compiled");
     return 0;
#endif
}

static int tcp_server_init_session(tcp_server_t * server, int sd, 
                                   struct sockaddr_in6 * sock_client,
                                   socklen_t socklen) {
     if (server->num_clients >= MAX_SESSION) {
          errno = EAGAIN;
          tool_print("Too many clients\n");
          close(sd);
          return 0;
     }

     //allocate space for new connection
     tcp_session_t * session = NULL;
     if (server->freeq) {
          session = server->freeq;
          server->freeq = session->next;
     }
     else {
          session = (tcp_session_t*)malloc(sizeof(tcp_session_t));
          if (!session) {
               tool_print("unable to allocate new session");
               close(sd);
               return 0;
          }
     }
     memset(session, 0, sizeof(tcp_session_t));
     session->sd = sd;
     
#ifndef NOTLS
     if (server->do_tls) {
          dprint("setting up TLS for session");
          session->ssl = SSL_new(server->ctx);
          SSL_set_fd(session->ssl, session->sd);
          if (SSL_accept(session->ssl) <= 0) {
               dprint("TLS did not work for session");
               ERR_print_errors_fp(stderr);
               SSL_shutdown(session->ssl);
               SSL_free(session->ssl);
               close(sd);
               return 0;
          }
          if (server->verify_client_cert) {
               if (SSL_get_verify_result(session->ssl) != X509_V_OK) {
                    dprint("unable to verify client cert");
                    ERR_print_errors_fp(stderr);
                    SSL_shutdown(session->ssl);
                    SSL_free(session->ssl);
                    close(sd);
                    return 0;
               }
               X509 * peer = SSL_get_peer_certificate(session->ssl);
               if (peer) {
                    char *subj = X509_NAME_oneline(X509_get_subject_name(peer), NULL, 0);
                    if (subj) {
                         session->subj = wsdata_create_string(subj,
                                                              strlen(subj));
                         if (session->subj) {
                              wsdata_add_label(session->subj, server->label_subj);
                              wsdata_add_reference(session->subj);
                         }
                         OPENSSL_free(subj);
                    } 
                    X509_free(peer);
               }
          }
     }
#endif
     //turn on non-blocking after TLS is accepted (could stall)
     if (!server->blocking) {
          int ret = fcntl(sd, F_SETFL, FNDELAY);
          if (ret < 0) {
               perror("fcntl(..., FNDELAY)");
               close(sd);
               return 0;
          }
     }

     char abuf[INET6_ADDRSTRLEN];
     char * result = (char *)inet_ntop(AF_INET6, (const void *)&sock_client->sin6_addr, abuf,
                                       INET6_ADDRSTRLEN);
     if (result) {
          session->client_ip = wsdata_create_string(result,
                                                    strlen(result));
          if (session->client_ip) {
               wsdata_add_label(session->client_ip, server->label_client_ip);
               wsdata_add_reference(session->client_ip);
          }
     }
     session->client_port = ntohs(sock_client->sin6_port); 

     session->next = server->active;
     server->active = session;
     server->num_clients++;
     dprint("Accepted new connection %d\n", server->num_clients);
     return 1;
}

//check if new connections..
static int tcp_server_check(tcp_server_t * server) {
     //try to accept -- 
     struct sockaddr_in6 sock_client;
     socklen_t socklen = sizeof(struct sockaddr_in6);

     int sd = accept(server->sd, (struct sockaddr *)&sock_client, &socklen);
     if (sd < 0 && (errno != EAGAIN && errno != EWOULDBLOCK)) {
          perror("accept");
          return 0;
     } else if (sd >= 0) {
          return tcp_server_init_session(server, sd, &sock_client, socklen);
     }
     //else {
          //non-blocking polling
     //}
     return 1;
}

static int tcp_server_recv(tcp_server_t * server) {
     tcp_server_check(server);

     tcp_session_t * cursor = server->active;
     tcp_session_t * prev = NULL;
     tcp_session_t * next = NULL;

     while(cursor) {
          next = cursor->next;

          int ret;
#ifndef NOTLS
          if (server->do_tls && cursor->ssl) {
               ret = SSL_read(cursor->ssl, server->recv, MAXRECV);
               if (ret < 0) {
                    if (ret != -1) {
                         dprint("ret %d", ret);
                    }
                    int err = SSL_get_error(cursor->ssl, ret);
                    switch(err) {
                    case SSL_ERROR_NONE:
                    case SSL_ERROR_WANT_READ:
                         errno = EWOULDBLOCK;
                         break;
                    case SSL_ERROR_ZERO_RETURN:
                         // peer disconnected...
                         ret = 0;
                    }
               }
          }
          else {
               ret = read(cursor->sd, server->recv, MAXRECV);
          }
#else
          ret = read(cursor->sd, server->recv, MAXRECV);
#endif
          if (((ret < 0) && (errno != EAGAIN) && (errno != EWOULDBLOCK)) || 
              (ret == 0)) {
               dprint("socket closing %d %d", ret, errno );
               //kill socket, clock out session
               dprint("closing socket %d", cursor->sd);
#ifndef NOTLS
               if (server->do_tls && cursor->ssl) {
                    SSL_shutdown(cursor->ssl);
                    SSL_free(cursor->ssl);
               }
#endif
               close(cursor->sd);
               server->callback(server->cbdata, cursor, NULL, 0);
               if (cursor->client_ip) {
                    wsdata_delete(cursor->client_ip);
               }
               if (cursor->partial) {
                    free(cursor->partial);
               }
#ifndef NOTLS
               if (cursor->subj) {
                    wsdata_delete(cursor->subj);
               }
#endif
               if (!prev) {
                    server->active = next;
               }
               else {
                    prev->next = next;
               }
               cursor->next = server->freeq;
               server->freeq = cursor;
               server->num_clients--;
          }
          else {
               if (ret > 0) {
                    //do callback
                    server->callback(server->cbdata, cursor, server->recv, ret);
               }
               prev = cursor;
          }
          cursor = next;
     }
     return 1;
}

     
static void tcp_server_destroy(tcp_server_t * server) {
     //stop accepting new connections
     close(server->sd);

     tcp_session_t * cursor = server->active;
     tcp_session_t * next = NULL;

     //close existing connections
     while(cursor) {
          next = cursor->next;
#ifndef NOTLS
          if (server->do_tls && cursor->ssl) {
               SSL_shutdown(cursor->ssl);
               SSL_free(cursor->ssl);
          }
          if (cursor->subj) {
               wsdata_delete(cursor->subj);
          }
#endif
          close(cursor->sd);
          if (cursor->client_ip) {
               wsdata_delete(cursor->client_ip);
          }
          if (cursor->partial) {
               free(cursor->partial);
          }
          free(cursor);
          cursor = next;
     }

     cursor = server->freeq;
     while(cursor) {
          next = cursor->next;
          free(cursor);
          cursor = next;
     }
#ifndef NOTLS
     if (server->do_tls) {
          SSL_CTX_free(server->ctx);
     }
#endif

     free(server);
}

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;
     ws_doutput_t * dout;

     wslabel_t * label_datetime;
     wslabel_t * label_message;
     wslabel_t * label_source_ip;
     wslabel_t * label_source_port;
     wslabel_t * label_syslog;

     uint16_t port;
     int blocking;

     int msg_trailer;
     char * bindip;

     ws_outtype_t * outtype_tuple;
     tcp_server_t * server;
     char * certfile;
     char * keyfile;
     char * ca_file;
     char * ca_path;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;

     while ((op = getopt(argc, argv, "m:M:I:c:C:k:K:bBp:P:")) != EOF) {
          switch (op) {
          case 'M':
               proc->ca_file = strdup(optarg);
               break;
          case 'm':
               proc->ca_path = strdup(optarg);
               break;
          case 'I':
               proc->bindip = strdup(optarg);
               break;
          case 'c':
          case 'C':
               proc->certfile = strdup(optarg);
               break;
          case 'k':
          case 'K':
               proc->keyfile = strdup(optarg);
               break;
          case 'b':
          case 'B':
               proc->blocking = 1;
               break;
          case 'P':
          case 'p':
               proc->port = atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     return 1;
}

static int tcp_session_append_partial(tcp_session_t * session, char * buf, int buflen) {
     //append string to prior
     int nlen = session->partial_len + buflen;
     if (nlen > MAX_PARTIAL) {
          //free up partial
          if (session->partial) {
               free(session->partial);
          }
          session->partial = NULL;
          session->partial_len = 0;
          return 0;
     }
     char * nbuf = (char *) malloc(nlen);
     if (!nbuf) {
          if (session->partial) {
               free(session->partial);
          }
          session->partial = NULL;
          session->partial_len = 0;
          return 0;
     }
     if (session->partial) {
          memcpy(nbuf, session->partial, session->partial_len);
     }
     memcpy(nbuf + session->partial_len, buf, buflen);
     session->partial = nbuf;
     session->partial_len = nlen;

     return 1;
}

static void emit_message(proc_instance_t * proc, tcp_session_t * session,
                    char * buf, int buflen) {

     dprint("emit_message %d %d", buflen, session->partial_len);
     if ((buflen <= 0) && (session->partial_len <= 0)) {
          return;
     }
     wsdata_t * tuple = wsdata_alloc(dtype_tuple);
     if (!tuple) {
          return;
     }
     wsdata_add_reference(tuple);
     wsdata_add_label(tuple, proc->label_syslog);

     struct timeval current;
     gettimeofday(&current, NULL);
     wsdt_ts_t ts;
     ts.sec = current.tv_sec;
     ts.usec = current.tv_usec;

     tuple_member_create_ts(tuple, ts, proc->label_datetime);

     add_tuple_member(tuple, session->client_ip);
#ifndef NOTLS
     add_tuple_member(tuple, session->subj);
#endif
     tuple_member_create_uint(tuple, session->client_port, proc->label_source_port);

     wsdt_string_t *str = tuple_create_string(tuple, proc->label_message, buflen + session->partial_len);
     if (!str) {
          wsdata_delete(tuple);
          return;
     }

     if (session->partial && session->partial_len) {
          memcpy(str->buf, session->partial, session->partial_len);
     }
     memcpy(str->buf + session->partial_len, buf, buflen);

     ws_set_outdata(tuple, proc->outtype_tuple, proc->dout);
     wsdata_delete(tuple);
     proc->outcnt++;
}

static void proc_receive_session(void * vproc, tcp_session_t * session,
                                 char * buf, int buflen) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     if (!buf) {
          ///this session is closing - flush any partial buffers..
          return;
     }

     //search for end of strings
     char * hit = memchr(buf, proc->msg_trailer, buflen);
     if (!hit) {
          tcp_session_append_partial(session, buf, buflen);
          return;
     }
     int hlen = 0;
     while (hit) {
          hlen = hit - buf;
          
          emit_message(proc, session, buf, hlen);
         
          if (session->partial) { 
               free(session->partial);
               session->partial = NULL;
               session->partial_len = 0;
          }
          hlen++;

          if (hlen >= buflen) {
               return;
          }
          buf += hlen;
          buflen -= hlen;
          hit = memchr(buf, proc->msg_trailer, buflen);
     }
     if (buflen) {
          session->partial = (char *)malloc(buflen);
          if (!session->partial) {
               dprint("bad parial");
               return;
          }
          memcpy(session->partial, buf, buflen);
          session->partial_len = buflen;
     }
}
     
//function prototypes for local functions
static int data_source_tcp(void *, wsdata_t*, ws_doutput_t*, int);

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

     proc->msg_trailer = '\n'; 

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     proc->server = tcp_server_create(proc->port, proc_receive_session, proc,
                                      proc->label_source_ip, proc->blocking,
                                      proc->bindip);
     if (!proc->server) {
          tool_print("unable to initialize server");
          return 0;
     }
     if (proc->certfile && proc->keyfile) {
          tool_print("initializing TLS server");
          if (!tcp_server_init_tls(proc->server, proc->certfile, proc->keyfile)) {
               return 0;
          }
          if (proc->ca_file || proc->ca_path) {
               if (!tcp_server_init_clientCA(proc->server, proc->ca_file,
                                             proc->ca_path,
                                             wsregister_label(type_table, 
                                                              "SUBJECT"))) {
                    return 0;
               }
          }
     }

     //do something cool with sockets
     proc->outtype_tuple =
          ws_register_source_byname(type_table, "TUPLE_TYPE",
                                    data_source_tcp, sv);
     
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

static int data_source_tcp(void * vinstance, wsdata_t* source_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;

     //poll socket and receive data
     return tcp_server_recv(proc->server);
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("frame cnt   %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     tcp_server_destroy(proc->server);

     return 1;
}

