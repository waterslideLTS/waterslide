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

#define PROC_NAME "redis"
//#define DEBUG 1

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <hiredis/hiredis.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_double.h"
#include "stringhash5.h"
#include "procloader.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.0";
char *proc_tags[]              =  {"Database", "Distributed", NULL};
char *proc_alias[]             =  { "hiredis", NULL };
char proc_purpose[]            =  "interact with redis database";
char proc_description[]  = "Interact with redis database including "
     "GET, SET, DELETE, SUBSCRIBE, PUBLISH, INCREMENT, and DECREMENT. "
     "This kid can act as a source for subscription events. One can use this "
     "kid for interacting with other stream processing, for distributed "
     "processing or for storing data into more permanent storage.";

proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'V',"","LABEL",
     "member to use as value for set operations",0,0},
     {'h',"","hostname",
     "redis hostname (default to localhost)",0,0},
     {'p',"","",
     "redis port (default to 6379)",0,0},
     {'P',"","channel",
     "publish channel name",0,0},
     {'S',"","channel",
     "subscription channel name (blocking operation)",0,0},
     {'L',"","LABEL",
     "label of output value",0,0},
     {'x',"","seconds",
     "expiration value for SET operations",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]     =  "LABEL of key";
char *proc_input_types[]       =  {"tuple", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  {"tuple", NULL};
//char proc_requires[]           =  "";
// Ports: QUERYAPPEND
proc_port_t proc_input_ports[] =  {
     {"none","Get value at key"},
     {"GET","Get value at key"},
     {"SET","Set value at key"},
     {"SETNX","Set value at key if key does not exist"},
     {"INCR","Increment value at key"},
     {"DECR","Decrement value at key"},
     {"PUBLISH","Publish value at specified channel"},
     {NULL, NULL}
};
//char *proc_tuple_container_labels[] =  {NULL};
//char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {"VALUE", NULL};
char *proc_synopsis[]          =  {"[PORT]:redis <LABEL> [-V VALUE] [-h hostname] [-p port]", NULL};
proc_example_t proc_examples[] =  {
     {"... | GET:redis WORD -L INFO | ...", "Queries redis server "
      "with the specified string in the WORD buffer; "
      "labels any result as INFO."},
     {"... | SET:redis WORD -V COUNT ", "Sets key and value in redis server "
      "with the specified key string in the WORD buffer and specified value "
      "string in COUNT buffer.; Has not output in this mode."},
     {"... | SET:redis WORD -V COUNT -x 5m ", "Sets key and value in redis server "
      "with the specified key string in the WORD buffer and specified value "
      "string in COUNT buffer; Value in redis is held for only 5 minutes."},
     {"... | INCR:redis WORD -L RESULT | ...", "Increments value at key in redis server "
      "using the specified key string in the WORD buffer; "
      "Resulting value after increment is labeled RESULT."},
     {"... | DECR:redis WORD -L RESULT | ...", "Decrements value at key in redis server "
      "using the specified key string in the WORD buffer;"
      "Resulting value after decrement is labeled RESULT."},
     {"... | PUBLISH:redis -P WordChannel VERB ADVERB | ...", "Publishes values in redis server "
      "using the channel named WordChannel using value strings found in VERB and ADVERB; "
      "input tuple is passed downstream as a passthrough operation."},
     {"redis -S SubChannel -h redishost -p 12345 -L REDISOUT | ...",
      "Subscribes to channel SubChannel in redis server found at host redishost and port 12345;"
      "Any output strings are labeled RESIDOUT; This acts as source kid that blocks waiting for input."},
	{NULL, NULL}
};

//function prototypes for local functions
static int proc_get(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_set(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_setnx(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_incr(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_decr(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_publish(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_rsubscribe(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     redisContext * rc;
     wslabel_nested_set_t nest_keys;
     wslabel_nested_set_t nest_values;
     char * publish_channel;
     char * subscribe_channel;
     time_t expire_sec;

     wslabel_t * label_outvalue;

     char * hostname;
     uint16_t port;

     ws_outtype_t * outtype_tuple;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "x:V:L:h:p:P:S:v:")) != EOF) {
          switch (op) {
          case 'V':
               wslabel_nested_search_build(type_table, &proc->nest_values, optarg);
               break;
          case 'P':
               proc->publish_channel = strdup(optarg);;
               tool_print("publishing to channel at %s", proc->publish_channel);
               break;
          case 'S':
               proc->subscribe_channel = strdup(optarg);;
               tool_print("subscribing to channel at %s", proc->subscribe_channel);
               break;
          case 'x':
               proc->expire_sec = sysutil_get_duration_ts(optarg);
               tool_print("expiring at %u seconds", (uint32_t) proc->expire_sec);
               break;
          case 'L':
               proc->label_outvalue = wsregister_label(type_table, optarg);
               break;
          case 'h':
               proc->hostname = strdup(optarg);
               break;
          case 'p':
               proc->port = (uint16_t)atoi(optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build(type_table, &proc->nest_keys,
                                      argv[optind]);
          tool_print("using key %s", argv[optind]);
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


     proc->hostname = "localhost";
     proc->port = 6379;
     proc->label_outvalue = wsregister_label(type_table, "VALUE");
     proc->publish_channel = "waterslide";

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     struct timeval timeout = { 1, 500000 }; // 1.5 seconds
     redisContext * c =
          redisConnectWithTimeout(proc->hostname, proc->port, timeout);
     if (c == NULL || c->err) {
          if (c) {
               printf("Connection error: %s\n", c->errstr);
               redisFree(c);
          } else {
               printf("Connection error: can't allocate redis context\n");
          }
          return 0;
     }
     proc->rc = c;
     dprint("got context");

     if (proc->subscribe_channel) {
          proc->outtype_tuple =
               ws_register_source_byname(type_table,
                                         "TUPLE_TYPE", proc_rsubscribe, sv);
          if (proc->outtype_tuple ==NULL) {
               error_print("Error attempting to register source");
               return 0;
          }
          redisReply *reply =
               redisCommand(proc->rc, "SUBSCRIBE %s", proc->subscribe_channel);
          if (!reply) {
               error_print("unable to subscribe to channel %s",
                           proc->subscribe_channel);
               return 0;
          }
          freeReplyObject(reply);
     }
     dprint("finished init");

     return 1; 
}

// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
//return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * meta_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vinstance;

     dprint("input_set");
     if (meta_type != dtype_tuple) {
          dprint("not tuple");
          return NULL;
     }

     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
     }

     if (!port || wslabel_match(type_table, port, "GET") ||
         wslabel_match(type_table, port, "QUERY")) {
          dprint("input GET");
          return proc_get;
     }
     else if (wslabel_match(type_table, port, "SETNX")) {
          dprint("input SETNX");
          return proc_setnx;
     }
     else if (wslabel_match(type_table, port, "SET")) {
          dprint("input SET");
          return proc_set;
     }
     else if (wslabel_match(type_table, port, "INSERT")) {
          dprint("input SET");
          return proc_set;
     }
     else if (wslabel_match(type_table, port, "INCR")) {
          dprint("input INCR");
          return proc_incr;
     }
     else if (wslabel_match(type_table, port, "DECR")) {
          dprint("input DECR");
          return proc_decr;
     }
     else if (wslabel_match(type_table, port, "PUBLISH")) {
          dprint("input publish");
          return proc_publish;
     }

     dprint("input null");
     return NULL; // a function pointer
}

static int nest_search_callback_get(void * vproc, void * vevent,
                                    wsdata_t * tdata, wsdata_t * member) {
     dprint("key foundyy");
     proc_instance_t * proc = (proc_instance_t*)vproc;
     //search for member key
     char * buf = NULL;
     int len = 0;

     if (!dtype_string_buffer(member, &buf, &len)) {
          return 0;
     }
     redisReply *reply;

     dprint("query %.*s", len, buf);
     reply = redisCommand(proc->rc, "GET %b", buf, len);
     dprint("done...");
     if (reply) {
          dprint("reply succeeded");
          if ((reply->type == REDIS_REPLY_STRING) && reply->len && reply->str) {
               tuple_dupe_binary(tdata, proc->label_outvalue, reply->str, reply->len);
          }
          freeReplyObject(reply);
     }
     else {
          dprint("reply failed");
     }
     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_get(void * vinstance, wsdata_t * tuple,
                      ws_doutput_t * dout, int type_index) {

     dprint("proc_get");
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     dprint("search keys");
     tuple_nested_search(tuple, &proc->nest_keys,
                         nest_search_callback_get,
                         proc, NULL);
     
     ws_set_outdata(tuple, proc->outtype_tuple, dout);

     //always return 1 since we don't know if table will flush old data
     return 1;
}

static int proc_rsubscribe(void * vinstance, wsdata_t * tuple,
                      ws_doutput_t * dout, int type_index) {
     
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     redisReply *reply = NULL;

     dprint("attempting to get subscribe reply");
     if (redisGetReply(proc->rc,(void**)&reply) != REDIS_OK) {
          dprint("got invalid reply");
          return 1;
     }
     
     dprint("got reply %d", reply->type);
     if ((reply->type == REDIS_REPLY_STRING) && reply->len && reply->str) {
          dprint("got reply string");
          tuple_dupe_binary(tuple, proc->label_outvalue, reply->str, reply->len);
          ws_set_outdata(tuple, proc->outtype_tuple, dout);
     }
     else if (reply->type == REDIS_REPLY_ARRAY) {
          if (reply->elements == 3) {
               redisReply *r = reply->element[2];
               if ((r->type == REDIS_REPLY_STRING) && r->len && r->str) {
                    tuple_dupe_binary(tuple, proc->label_outvalue, r->str, r->len);
                    ws_set_outdata(tuple, proc->outtype_tuple, dout);
               }
          }
     }
     freeReplyObject(reply);
     return 1;
}

//select first element in search
static int nest_search_callback_one(void * vproc, void * vkey,
                                    wsdata_t * tdata, wsdata_t * member) {
     wsdata_t ** pkey = (wsdata_t**)vkey; 
     if (*pkey != NULL) {
          return 0;
     }
     *pkey = member;
     return 1;
}

static int proc_setnx(void * vinstance, wsdata_t * tuple,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     wsdata_t * key = NULL;
     wsdata_t * value = NULL;

     tuple_nested_search(tuple, &proc->nest_keys,
                         nest_search_callback_one,
                         proc, &key);
     tuple_nested_search(tuple, &proc->nest_values,
                         nest_search_callback_one,
                         proc, &value);

     if (key && value) {
          dprint("found key and value");
          char * keybuf = NULL;
          int keylen = 0;
          char * valbuf = NULL;
          int vallen = 0;
          
          if (dtype_string_buffer(key, &keybuf, &keylen) && 
              dtype_string_buffer(value, &valbuf, &vallen)) {
               dprint("found key and value strings");

               redisReply *reply;

               if (proc->expire_sec) {
                    reply = redisCommand(proc->rc, "SET %b %b NX EX %d",
                                         keybuf, keylen, valbuf, vallen,
                                         (int)proc->expire_sec);
               }
               else {
                    dprint("setting %.*s %.*s", keylen, keybuf, vallen, valbuf);
                    reply = redisCommand(proc->rc, "SET %b %b NX",
                                         keybuf, keylen, valbuf, vallen);
               }
               if (reply) {
                    freeReplyObject(reply);
               }
          }
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}



static int proc_set(void * vinstance, wsdata_t * tuple,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     wsdata_t * key = NULL;
     wsdata_t * value = NULL;

     tuple_nested_search(tuple, &proc->nest_keys,
                         nest_search_callback_one,
                         proc, &key);
     tuple_nested_search(tuple, &proc->nest_values,
                         nest_search_callback_one,
                         proc, &value);

     if (key && value) {
          dprint("found key and value");
          char * keybuf = NULL;
          int keylen = 0;
          char * valbuf = NULL;
          int vallen = 0;
          
          if (dtype_string_buffer(key, &keybuf, &keylen) && 
              dtype_string_buffer(value, &valbuf, &vallen)) {
               dprint("found key and value strings");

               redisReply *reply;

               if (proc->expire_sec) {
                    reply = redisCommand(proc->rc, "SET %b %b EX %d",
                                         keybuf, keylen, valbuf, vallen,
                                         (int)proc->expire_sec);
               }
               else {
                    dprint("setting %.*s %.*s", keylen, keybuf, vallen, valbuf);
                    reply = redisCommand(proc->rc, "SET %b %b",
                                         keybuf, keylen, valbuf, vallen);
               }
               if (reply) {
                    freeReplyObject(reply);
               }
          }
     }

     //always return 1 since we don't know if table will flush old data
     return 1;
}

static int nest_search_callback_incr(void * vproc, void * vevent,
                                    wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     //search for member key
     char * buf = NULL;
     int len = 0;

     if (!dtype_string_buffer(member, &buf, &len)) {
          return 0;
     }
     redisReply *reply;

     reply = redisCommand(proc->rc, "INCR %b", buf, len);
     if (reply) {
          if (reply->type == REDIS_REPLY_INTEGER) {
               tuple_member_create_int(tdata, reply->integer, proc->label_outvalue);
          }
          freeReplyObject(reply);
     }
     return 1;
}

static int proc_incr(void * vinstance, wsdata_t * tuple,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     tuple_nested_search(tuple, &proc->nest_keys,
                         nest_search_callback_incr,
                         proc, NULL);
     
     ws_set_outdata(tuple, proc->outtype_tuple, dout);

     //always return 1 since we don't know if table will flush old data
     return 1;
}

static int nest_search_callback_decr(void * vproc, void * vevent,
                                    wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     //search for member key
     char * buf = NULL;
     int len = 0;

     if (!dtype_string_buffer(member, &buf, &len)) {
          return 0;
     }
     redisReply *reply;

     reply = redisCommand(proc->rc, "DECR %b", buf, len);
     if (reply) {
          if (reply->type == REDIS_REPLY_INTEGER) {
               tuple_member_create_int(tdata, reply->integer, proc->label_outvalue);
          }
          freeReplyObject(reply);
     }
     return 1;
}

static int proc_decr(void * vinstance, wsdata_t * tuple,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     tuple_nested_search(tuple, &proc->nest_keys,
                         nest_search_callback_decr,
                         proc, NULL);
     
     ws_set_outdata(tuple, proc->outtype_tuple, dout);

     //always return 1 since we don't know if table will flush old data
     return 1;
}

static int nest_search_callback_publish(void * vproc, void * vevent,
                                    wsdata_t * tdata, wsdata_t * member) {
     dprint("got publish value");
     proc_instance_t * proc = (proc_instance_t*)vproc;
     //search for member key
     char * buf = NULL;
     int len = 0;

     if (!dtype_string_buffer(member, &buf, &len)) {
          return 0;
     }
     redisReply *reply;

     dprint("attempting to publish value");
     reply = redisCommand(proc->rc, "PUBLISH %s %b", proc->publish_channel, buf, len);
     if (reply) {
          dprint("success in publishing");
          freeReplyObject(reply);
     }
     return 1;
}


static int proc_publish(void * vinstance, wsdata_t * tuple,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     tuple_nested_search(tuple, &proc->nest_keys,
                         nest_search_callback_publish,
                         proc, NULL);
     tuple_nested_search(tuple, &proc->nest_values,
                         nest_search_callback_publish,
                         proc, NULL);
     
     ws_set_outdata(tuple, proc->outtype_tuple, dout);

     //always return 1 since we don't know if table will flush old data
     return 1;
}




//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     if (proc->rc) {
          redisFree(proc->rc);
     }
     free(proc);

     return 1;
}


