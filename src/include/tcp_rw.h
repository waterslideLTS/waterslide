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

#ifndef _TCP_RW_H
#define _TCP_RW_H

// Network headers
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <time.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <datatypes/wsdt_binary.h>
#include <assert.h>
#include <signal.h>
#include "cppwrap.h"

#define MAX_CLIENTS 512

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

/*************************************************
  *
  * Shared code between thrower / catcher
  *
  ***********************************************/

typedef struct tcp_comm_t {
     int server_socket;
     int fds[MAX_CLIENTS];
     int num_clients;
     const char *hostname;
     int port;
     time_t last_connect_try;
     int blocking_connections;
     int verbose;
} tcp_comm_t;

static int
create_listen_socket(const char* hostname, int port)
{
     int sd, val, ret;
     socklen_t socklen;
     struct sockaddr_in sa_in;
    
     sd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
     if (sd < 0) {
          perror("socket");
          return -1;
     }

     socklen = sizeof(struct sockaddr_in);
     sa_in.sin_family = AF_INET;
     sa_in.sin_port = htons(port);
     sa_in.sin_addr.s_addr = htonl(INADDR_ANY);

     /* set reuse flag */
     val = 1;
     ret = setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
     if (ret < 0) {
          perror("setsockopt");
          close(sd);
          return -1;
     }
     
     /* make non-blocking (always!) */
     ret = fcntl(sd, F_SETFL, FNDELAY);
     if (ret < 0) {
          perror("fcntl(..., FNDELAY)");
          close(sd);
          return -1;
     }

     /* Finish initialization */
     ret = bind(sd, (struct sockaddr*)(&sa_in), socklen);
     if (ret < 0) {
          perror("bind");
          close(sd);
          return -1;
     }

     ret = listen(sd, MAX_CLIENTS);
     if (ret < 0) {
          perror("listen");
          close(sd);
          return -1;
     }
     
     return sd;
}

static int
establish_connection(tcp_comm_t *comm)
{
     int sd, ret;
     struct sockaddr_in sa_in;
     socklen_t socklen;
     struct hostent *hp;

     socklen = sizeof(struct sockaddr_in);
     sa_in.sin_port = htons(comm->port);

     sd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
     if (sd < 0) {
          perror("socket");
          return -1;
     }

     hp = gethostbyname(comm->hostname);
     if (!hp) {
          perror("gethostbyname");
          close(sd);
          return -1;
     }

     sa_in.sin_family = hp->h_addrtype;
     memcpy(&sa_in.sin_addr.s_addr, hp->h_addr, hp->h_length);

     ret = connect(sd, (const struct sockaddr*)&sa_in, socklen);
     if (ret < 0 && errno == ECONNREFUSED) {
          close(sd);
          return -1;
     } else if (ret < 0) {
          perror("connect");
          close(sd);
          return -1;
     }

     if (!comm->blocking_connections) {
          /* make non-blocking */
          ret = fcntl(sd, F_SETFL, FNDELAY);
          if (ret < 0) {
               perror("fcntl(..., FNDELAY)");
               close(sd);
               return -1;
          }
     }

     fprintf(stderr, "established client connection\n");

     return sd;
}

inline static int
check_connection_status(tcp_comm_t* comm)
{
     int ret, sd;

     if (comm->server_socket >= 0) {
          /* check for new clients */
          sd = accept(comm->server_socket, NULL, NULL);
          if (sd < 0 && (errno != EAGAIN && errno != EWOULDBLOCK)) {
               perror("accept");
               return -1;
          } else if (sd >= 0) {
               if (comm->num_clients < MAX_CLIENTS) {
                    if (comm->verbose) {
                         fprintf(stderr, "Accepted new connection %d\n",
                                 comm->num_clients);
                    }
                    if (!comm->blocking_connections) {
                         ret = fcntl(sd, F_SETFL, FNDELAY);
                         if (ret < 0) {
                              perror("fcntl(..., FNDELAY)");
                              close(sd);
                              return -1;
                         }
                    }
                    comm->fds[comm->num_clients++] = sd;
               } else {
                    errno = EAGAIN;
                    fprintf(stderr, "Too many clients\n");
                    close(sd);
                    return -1;
               }
          }
     } else {
          /* make sure we are connected */
          if (comm->fds[0] < 0 && comm->last_connect_try + 1 < time(NULL)) {
               comm->fds[0] =
                    establish_connection(comm);
               if (comm->fds[0] >= 0) {
                    comm->num_clients++;
               } else {
                    comm->last_connect_try = time(NULL);
               }
          }
     }

     return 0;
}

static int
comm_init(tcp_comm_t *comm)
{
     if (NULL == comm->hostname) {
          /* Run as server */
          comm->server_socket = create_listen_socket(comm->hostname,
                                                     comm->port);
          if (comm->server_socket < 0) {
               return -1;
          }
     } else {
          /* Run as client */
          comm->server_socket = -1;
          comm->fds[0] =
             establish_connection(comm);
          if (comm->fds[0] >= 0) {
               comm->num_clients++;
          } else {
               comm->last_connect_try = time(NULL);
          }
     }

     /* ignore SIGPIPE so we don't die on dropped connections */
     signal(SIGPIPE, SIG_IGN);

     return 0;
}

static int
comm_destroy(tcp_comm_t *comm)
{
     int i;

     if (comm->server_socket >= 0) {
          close(comm->server_socket);
     }
     for (i = 0 ; i < comm->num_clients ; ++i) {
          close(comm->fds[i]);
     }

     return 0;
}


/*************************************************
 *
 * Thrower code
 *
 ************************************************/

typedef struct thrower_client_t {
     size_t header_bytes_sent;
     size_t bytes_sent;
     wsdata_t *pending;
     uint64_t msglen;
} thrower_client_t;

typedef struct tcp_throw_t {
     tcp_comm_t base;
     thrower_client_t clients[MAX_CLIENTS];
     uint64_t sent;
     uint64_t dropped;
} tcp_throw_t;

tcp_throw_t*
tcp_throw_init(const char* hostname, int port, int blocking)
{
     int ret;
     tcp_throw_t *thrower = (tcp_throw_t *)calloc(sizeof(tcp_throw_t), 1);
     if (!thrower) {
          error_print("failed tcp_throw_init calloc of thrower");
          return NULL;
     }

     thrower->base.hostname = hostname;
     thrower->base.port = port;
     thrower->base.blocking_connections = blocking;

     ret = comm_init(&thrower->base);
     if (ret < 0) {
          free(thrower);
          return NULL;
     }

     return thrower;
}

static int tcp_throw_send_msg(tcp_throw_t*, int, wsdata_t*);

int
tcp_throw_destroy(tcp_throw_t *thrower)
{
     int i;
     /* flush out partially sent messages so receiver doesn't timeout */
     for (i = 0 ; i < thrower->base.num_clients ; ++i) {
          while (thrower->clients[i].pending != NULL) {
               if (tcp_throw_send_msg(thrower, i, NULL) < 0) {
                    break;
               }
          }
     }

     comm_destroy(&thrower->base);

     free(thrower);

     return 1;
}

static int
tcp_throw_send_msg(tcp_throw_t *thrower, int idx,
                   wsdata_t *indata)
{
     int ret;
     int fd = thrower->base.fds[idx];
     thrower_client_t *client = &thrower->clients[idx];

     /* try to finish previous message */
     if (NULL != client->pending) {
          wsdt_binary_t *bin = (wsdt_binary_t *) client->pending->data;
          assert(bin->len == (int)client->msglen);

          ret = write(fd, ((char*) &client->msglen) + client->header_bytes_sent,
                      sizeof(uint64_t) - client->header_bytes_sent);
          if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
               perror("header write");
               return -1;
          }
          if (ret < 0) ret = 0;
          client->header_bytes_sent += ret;
          if (client->header_bytes_sent != sizeof(uint64_t)) {
               thrower->dropped++;
               return 0;
          }

          ret = write(fd, bin->buf + client->bytes_sent,
                      client->msglen - client->bytes_sent);
          if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
               perror("payload write");
               return -1;
          } else {
               if (ret < 0) ret = 0;
               client->bytes_sent += ret;
          }

          if (client->bytes_sent == client->msglen) {
               wsdata_delete(client->pending);
               client->pending = NULL;
          } else {
               thrower->dropped++;
               return 0;
          }
          thrower->sent++;
     }

     /* if no message queued, try to send new message */
     if (NULL == indata) return 0;
     wsdt_binary_t *bin = (wsdt_binary_t*) indata->data;
     client->bytes_sent = 0;
     client->header_bytes_sent = 0;
     client->msglen = bin->len;
     client->pending = indata;

     /* write the header */
     ret = write(fd, ((char*) &client->msglen) + client->header_bytes_sent,
                 sizeof(uint64_t) - client->header_bytes_sent);
     if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
          perror("header write");
          return -1;
     } else if (ret <= 0) {
          /* sent nothing - just drop the whole thing on the floor */
          thrower->dropped++;
          client->pending = NULL;
          return 0;
     } else {
          client->header_bytes_sent += ret;
     }
     if (client->header_bytes_sent != sizeof(uint64_t)) {
          wsdata_add_reference(client->pending);
          return 0;
     }

     /* write the payload */
     ret = write(fd, bin->buf + client->bytes_sent,
                 client->msglen - client->bytes_sent);
     if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
          perror("payload write");
          return -1;
     } else {
          if (ret < 0) ret = 0;
          client->bytes_sent += ret;
     }
     if (client->bytes_sent == client->msglen) {
          client->pending = NULL;
     } else {
          wsdata_add_reference(client->pending);
          return 0;
     }

     thrower->sent++;

     return 0;
}

int
tcp_throw_data(tcp_throw_t *thrower, wsdata_t *data)
{
     int ret, i, j;

     ret = check_connection_status(&thrower->base);
     if (ret < 0) {
          return 0;
     }

     for (i = 0 ; i < thrower->base.num_clients ; ++i) {
         if (tcp_throw_send_msg(thrower, i, data) < 0) {
               close(thrower->base.fds[i]);
               thrower->base.fds[i] = -1;
               memset(&thrower->clients[i], 0, sizeof(thrower->clients[i]));
               for (j = i ; j < thrower->base.num_clients - 1 ; j++) {
                    thrower->base.fds[j] = thrower->base.fds[j + 1];
               }
               i--;
               thrower->base.num_clients--;
         }
     }

     return 1;
}


/*************************************************
 *
 * Catcher code
 *
 ************************************************/

typedef struct tcp_catch_t {
     tcp_comm_t base;
} tcp_catch_t;

tcp_catch_t*
tcp_catch_init(const char* hostname, int port, int blocking)
{
     int ret;
     tcp_catch_t *catcher = (tcp_catch_t *)calloc(sizeof(tcp_catch_t), 1);
     if (!catcher) {
          error_print("failed tcp_catch_init calloc of catcher");
          return NULL;
     }

     catcher->base.hostname = hostname;
     catcher->base.port = port;
     catcher->base.blocking_connections = blocking;

     ret = comm_init(&catcher->base);
     if (ret < 0) {
          free(catcher);
          return NULL;
     }

     return catcher;
}

int
tcp_catch_destroy(tcp_catch_t *catcher)
{
     comm_destroy(&catcher->base);

     free(catcher);

     return 1;
}

static int
tcp_catch_recv_msg(int fd, ws_outtype_t *outtype,
                   wsdata_t *data, ws_doutput_t *dout)
{
     int ret, tmp = 0;
     uint64_t msglen;
     uint64_t readlen;

     while (tmp != sizeof(uint64_t)) {
          ret = read(fd, ((char*) &msglen) + tmp, sizeof(uint64_t) - tmp);
          if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
               perror("header read");
               close(fd);
               return -1;
          } else if (tmp == 0 && ret < 0) {
               return 0;
          } else if (ret == 0) {
               fprintf(stderr, "dropped connection\n");
               return -1;
          } else if (ret < 0) {
               ret = 0;
          }
          tmp += ret;
     }

     wsdata_t *outdata = dtype_alloc_binary(msglen);
     if (!outdata) {
          return 0;
     }
     wsdt_binary_t *bin = (wsdt_binary_t*)outdata->data;

     readlen = 0;
     while ((int)readlen < bin->len) {
          ret = read(fd, bin->buf + readlen, bin->len - readlen);
          if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
               perror("payload read");
               close(fd);
               return -1;
          } else if (ret >= 0) {
               readlen += ret;
          }
     }

     ws_set_outdata(outdata, outtype, dout);

     return 1;
}

int
tcp_catch_data(tcp_catch_t *catcher, ws_outtype_t* outtype,
               wsdata_t *data, ws_doutput_t *dout)
{
     int count = 0, ret, i, j;

     ret = check_connection_status(&catcher->base);
     if (ret < 0) {
          return 0;
     }

     for (i = 0 ; i < catcher->base.num_clients ; ++i) {
          ret = tcp_catch_recv_msg(catcher->base.fds[i], outtype, data, dout);
          if (ret < 0) {
               close(catcher->base.fds[i]);
               catcher->base.fds[i] = -1;
               for (j = i ; j < catcher->base.num_clients - 1 ; j++) {
                    catcher->base.fds[j] = catcher->base.fds[j + 1];
               }
               i--;
               catcher->base.num_clients--;
               ret = 0;
          }
          count += ret;
     }

     return count;
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _TCP_RW_H
