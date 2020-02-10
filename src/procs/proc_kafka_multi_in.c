/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012, Magnus Edenhill
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: 
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer. 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution. 
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * based on librdkafka Apache Kafka consumer example
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */


//#define DEBUG 1
#define PROC_NAME "kafka_multi_in"
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <signal.h>
#include "librdkafka/rdkafka.h"  /* for Kafka driver */
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "wstypes.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_binary.h"

char proc_version[]     = "1.1";
char *proc_tags[]     = { "source", "input", NULL };
char *proc_alias[]     = { NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "multi topic kafka subscriber";
char proc_nonswitch_opts[] = "kafka topic:partition to subscribe";
proc_port_t proc_input_ports[] =  {
     {"STAT","append running stats to tuple"},
     {"STATS","append running stats to tuple"},
     {"TRIGGER","append running stats to tuple"},
     {NULL, NULL}
};
proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'b',"","broker",
     "Specify a host and port for broker",0,0},
     {'L',"","label",
     "Specify an output label default DATA",0,0},
     {'g',"","group",
     "consumer group",0,0},
     {'S',"","",
     "detect if buffer is ascii strings",0,0},
     {'X',"","key=value",
     "set kafka config option",0,0},
     {'V',"","",
     "send logs in output tagged with LOG label",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

//function prototypes for local functions
static int data_source(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_stats(void *, wsdata_t*, ws_doutput_t*, int);

#define GRP_MAX 64
typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;
     uint64_t outlen;

     ws_outtype_t * outtype_tuple;
     char * tasks;
     char * brokers;
     int partition;
     char * topic;
     char group_default[GRP_MAX];
     char * group;
     wslabel_t * label_data;
     wslabel_t * label_log;
     wslabel_t * label_buf;
     wslabel_t * label_kafka;
     wslabel_t * label_topic;
     wslabel_t * label_partition;
     wslabel_t * label_datetime;
     wslabel_t * label_outcnt;
     wslabel_t * label_outlen;
     wslabel_t * label_msg;
     wslabel_t * label_mode;
     wslabel_t * label_err;
     wslabel_t * label_reason;
     wslabel_t * label_throttle_ms;
     wslabel_t * label_broker;
     wslabel_t * label_broker_id;
     wslabel_t * label_offset;

     rd_kafka_t *rk;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
     rd_kafka_topic_partition_list_t *topics;
	char errstr[512];
     ws_doutput_t * dout;

     int stringdetect;
     int tuple_logs;
} proc_instance_t;


static int handle_kafka_config_option(proc_instance_t * proc, char * kv) {
     char * key = kv;
     char * value = strchr(kv,'=');
     if (!value || (key == value)) {
          tool_print("invalid key=value combination");
          return 0; //invalid key=value combination
     }

     //split into two strings: key and value
     value[0] = 0;   //convert = to null
     value++;

     rd_kafka_conf_res_t res;

     res = rd_kafka_topic_conf_set(proc->topic_conf, key, value, NULL, 0);

     //if not topic config -- try general config
     if (res == RD_KAFKA_CONF_UNKNOWN) {
          res = rd_kafka_conf_set(proc->conf, key, value, NULL, 0);
     }
     if (res != RD_KAFKA_CONF_OK) {
          tool_print("INVALID kafka config option %s=%s", key, value);
          return 0;
     }
     tool_print("setting kafka config option %s=%s", key, value);
     return 1;
}


static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;

     while ((op = getopt(argc, argv, "vVX:g:Sb:p:L:")) != EOF) {
          switch (op) {
          case 'v':
          case 'V':
               proc->tuple_logs = 1;
               break;
          case 'g':
               proc->group = optarg;
               break;
          case 'b':
               proc->brokers = optarg;
               tool_print("using broker: %s", optarg);
               break;
          case 'p':
               proc->partition = atoi(optarg);
               tool_print("using partition: %d", proc->partition);
               break;
          case 'L':
               proc->label_buf = wsregister_label(type_table, optarg);
               break;
          case 'S':
               proc->stringdetect = 1;
               break;
          case 'X':
               if (!handle_kafka_config_option(proc, optarg)) {
                    error_print("invalid kafka option");
                    return 0;
               }
               break; 
          default:
               return 0;
          }
     }
     rd_kafka_conf_set_default_topic_conf(proc->conf, proc->topic_conf);
     if (optind < argc) {
          proc->topics = rd_kafka_topic_partition_list_new(argc - optind);
     }
     int i;
     for (i = optind ; i < argc ; i++) {
          /* Parse "topic[:part] */
          char *topic = argv[i];
          char *t;
          int32_t partition = -1;

          if ((t = strstr(topic, ":"))) {
               *t = '\0';
               partition = atoi(t+1);
          }

          rd_kafka_topic_partition_list_add(proc->topics, topic, partition);
          if (partition >= 0) {
               tool_print("reading from topic %s:%d", topic, partition);
          }
          else {
               tool_print("reading from topic %s", topic);
          }
     }

     return 1;
}

static wsdata_t * allocate_log_tuple(proc_instance_t * proc) {
     wsdata_t * tuple = wsdata_alloc(dtype_tuple);

     if (!tuple) {
          return NULL;
     }

     wsdata_add_label(tuple, proc->label_kafka);
     wsdata_add_label(tuple, proc->label_log);

     struct timeval current;
     gettimeofday(&current, NULL);

     wsdt_ts_t ts;
     ts.sec = current.tv_sec;
     ts.usec = current.tv_usec;

     tuple_member_create_ts(tuple, ts, proc->label_datetime);

     return tuple;
}

static void rebalance_cb_tuple(rd_kafka_t *rk,
                               rd_kafka_resp_err_t err,
                               rd_kafka_topic_partition_list_t *partitions,
                               void *opaque) {
     proc_instance_t * proc = (proc_instance_t*)opaque;

     if (!proc || !proc->dout) {
          return;
     }

     wsdata_t * tuple = allocate_log_tuple(proc);
     if (!tuple) {
          return;
     }

     char * msg = "Consumer group rebalanced";
     tuple_dupe_string(tuple, proc->label_msg, msg, strlen(msg));

     const char * mode;

     switch (err) {
     case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
          mode = "assign";
          rd_kafka_assign(proc->rk, partitions);
          break;
     case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
          mode = "revoke";
          rd_kafka_assign(proc->rk, NULL);
          break;
     default:
          mode = rd_kafka_err2str(err);
          rd_kafka_assign(proc->rk, NULL);
     }
     if (mode && strlen(mode)) {
          tuple_dupe_string(tuple, proc->label_mode, mode, strlen(mode));
     }

     if (partitions)  {
          int i;
          for (i = 0 ; i < partitions->cnt ; i++) {
               wsdata_t * sub = tuple_member_create_wsdata(tuple, dtype_tuple,
                                                           proc->label_partition);
               if (sub) {
                    char * topic = partitions->elems[i].topic;
                    if (topic && strlen(topic)) {
                         tuple_dupe_string(sub, proc->label_topic,
                                           topic, strlen(topic));
                    }
                    tuple_member_create_int32(sub,
                                            partitions->elems[i].partition,
                                            proc->label_partition);
                    tuple_member_create_int64(sub,
                                            partitions->elems[i].offset,
                                            proc->label_offset);
               }
          }
     }
     //output tuple
     ws_set_outdata(tuple, proc->outtype_tuple, proc->dout);
}

static void print_partition_list (FILE *fp,
                                  const rd_kafka_topic_partition_list_t
                                  *partitions) {
     int i;
     for (i = 0 ; i < partitions->cnt ; i++) {
          fprintf(stderr, "%s %s [%"PRId32"] offset %"PRId64,
                  i > 0 ? ",":"",
                  partitions->elems[i].topic,
                  partitions->elems[i].partition,
                  partitions->elems[i].offset);
     }
     if (i) {
          fprintf(stderr, "\n");
     }
}

static void rebalance_cb (rd_kafka_t *rk,
                          rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *partitions,
                          void *opaque) {

     proc_instance_t * proc = (proc_instance_t*)opaque;
     if (proc && proc->tuple_logs && proc->dout) {
          rebalance_cb_tuple(rk, err, partitions, opaque);
          return;
     }
     tool_print("%% Consumer group rebalanced: ");

     switch (err) {
     case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
          tool_print("      assigned");
          print_partition_list(stderr, partitions);
          rd_kafka_assign(proc->rk, partitions);
          break;

     case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
          tool_print("      revoked");
          print_partition_list(stderr, partitions);
          rd_kafka_assign(proc->rk, NULL);
          break;

     default:
          tool_print("      failed: %s", rd_kafka_err2str(err));
          rd_kafka_assign(proc->rk, NULL);
          break;
     }
}

static void err_cb_tuple(rd_kafka_t *rk, int err, const char *reason, void *opaque) {
     proc_instance_t * proc = (proc_instance_t*)opaque;

     if (!proc || !proc->dout) {
          return;
     }

     wsdata_t * tuple = allocate_log_tuple(proc);
     if (!tuple) {
          return;
     }

     const char * msg = rd_kafka_name(rk);
     if (msg && strlen(msg)) {
          tuple_dupe_string(tuple, proc->label_msg, msg, strlen(msg));
     }
     const char * errstr = rd_kafka_err2str(err);
     if (errstr && strlen(errstr)) {
          tuple_dupe_string(tuple, proc->label_err, errstr, strlen(errstr));
     }
     if (reason && strlen(reason)) {
          tuple_dupe_string(tuple, proc->label_reason, reason, strlen(reason));
     }

     ws_set_outdata(tuple, proc->outtype_tuple, proc->dout);
}


int log_cb_tuple(proc_instance_t * proc, const rd_kafka_t *rk, int level,
                  const char *fac, const char *buf) {

     wsdata_t * tuple = allocate_log_tuple(proc);
     if (!tuple) {
          return 0;
     }

     tuple_member_create_int32(tuple,
                               level,
                               proc->label_mode);
     if (fac && strlen(fac)) {
          tuple_dupe_string(tuple, proc->label_err, fac, strlen(fac));
     }
     const char * name = rd_kafka_name(rk);
     if (name && strlen(name)) {
          tuple_dupe_string(tuple, proc->label_msg, name, strlen(name));
     }
     if (buf && strlen(buf)) {
          tuple_dupe_string(tuple, proc->label_reason, buf, strlen(buf));
     }
     ws_set_outdata(tuple, proc->outtype_tuple, proc->dout);

     return 1;
}


void log_cb(const rd_kafka_t *rk, int level,
            const char *fac, const char *buf) {

     if (rk) {
          proc_instance_t * proc = (proc_instance_t*)rd_kafka_opaque(rk);
          if (proc && proc->tuple_logs && proc->dout) {
               if (log_cb_tuple(proc, rk, level, fac, buf)) {
                    return;
               }
          }
     }
     
     rd_kafka_log_print(rk, level, fac, buf);
}

static void err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
     proc_instance_t * proc = (proc_instance_t*)opaque;
     if (proc && proc->tuple_logs && proc->dout) {
          err_cb_tuple(rk, err, reason, opaque);
          return;
     }
	fprintf(stderr, "%% ERROR CALLBACK: %s: %s: %s\n",
	       rd_kafka_name(rk), rd_kafka_err2str(err), reason);
}

static void throttle_cb_tuple (rd_kafka_t *rk, const char *broker_name,
                               int32_t broker_id, int throttle_time_ms,
                               void *opaque) {
     proc_instance_t * proc = (proc_instance_t*)opaque;

     wsdata_t * tuple = allocate_log_tuple(proc);
     if (!tuple) {
          return;
     }


     char * msg = "Throttled";
     tuple_dupe_string(tuple, proc->label_msg, msg, strlen(msg));

     tuple_member_create_int(tuple,
                             throttle_time_ms,
                             proc->label_throttle_ms);

     if (broker_name && strlen(broker_name)) {
          tuple_dupe_string(tuple, proc->label_broker, broker_name,
                            strlen(broker_name));
     }

     tuple_member_create_int32(tuple,
                               broker_id,
                               proc->label_broker_id);

     ws_set_outdata(tuple, proc->outtype_tuple, proc->dout);
}

static void throttle_cb (rd_kafka_t *rk, const char *broker_name,
                         int32_t broker_id, int throttle_time_ms,
                         void *opaque) {
     proc_instance_t * proc = (proc_instance_t*)opaque;
     if (proc && proc->tuple_logs && proc->dout) {
          throttle_cb_tuple(rk, broker_name, broker_id, throttle_time_ms, opaque);
          return;
     }
	fprintf(stderr, "%% THROTTLED %dms by %s (%"PRId32")\n", throttle_time_ms,
	       broker_name, broker_id);
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

     proc->label_buf = wsregister_label(type_table, "BUF");
     proc->label_kafka = wsregister_label(type_table, "KAFKA");
     proc->label_topic = wsregister_label(type_table, "TOPIC");
     proc->label_partition = wsregister_label(type_table, "PARTITION");
     proc->label_datetime = wsregister_label(type_table, "DATETIME");
     proc->label_outcnt = wsregister_label(type_table, "KAFKA_IN_COUNT");
     proc->label_outlen = wsregister_label(type_table, "KAFKA_IN_OUTBYTES");
     proc->label_log = wsregister_label(type_table, "LOG");
     proc->label_data = wsregister_label(type_table, "DATA");
     proc->label_msg = wsregister_label(type_table, "MSG");
     proc->label_err = wsregister_label(type_table, "ERR");
     proc->label_mode = wsregister_label(type_table, "MODE");
     proc->label_reason = wsregister_label(type_table, "REASON");
     proc->label_throttle_ms = wsregister_label(type_table, "THROTTLE_MSEC");
     proc->label_broker = wsregister_label(type_table, "BROKER");
     proc->label_broker_id = wsregister_label(type_table, "BROKER_ID");
     proc->label_offset = wsregister_label(type_table, "OFFSET");

     snprintf(proc->group_default,GRP_MAX,"%s:%d", PROC_NAME, rand());
     proc->group = proc->group_default;

     proc->conf = rd_kafka_conf_new();
     rd_kafka_conf_set_opaque(proc->conf, (void*)proc);
	rd_kafka_conf_set_error_cb(proc->conf, err_cb);
	rd_kafka_conf_set_throttle_cb(proc->conf, throttle_cb);
     rd_kafka_conf_set_rebalance_cb(proc->conf, rebalance_cb);
	rd_kafka_conf_set_log_cb(proc->conf, log_cb);

     /* Kafka topic configuration */
	proc->topic_conf = rd_kafka_topic_conf_new();
     rd_kafka_topic_conf_set(proc->topic_conf, "auto.offset.reset", "earliest",
                             NULL, 0);
     rd_kafka_topic_conf_set(proc->topic_conf, "offset.store.method",
                             "broker", NULL, 0);

     proc->brokers = "localhost:9092";

     /* Create Kafka handle */
          //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     tool_print("using subscriber group %s", proc->group);

     if (rd_kafka_conf_set(proc->conf, "group.id", proc->group,
                           proc->errstr, sizeof(proc->errstr)) !=
         RD_KAFKA_CONF_OK) {
          fprintf(stderr, "%% %s\n", proc->errstr);
          exit(1);
     }
     if (!(proc->rk = rd_kafka_new(RD_KAFKA_CONSUMER, proc->conf,
                             proc->errstr, sizeof(proc->errstr)))) {
          fprintf(stderr,
                  "%% Failed to create Kafka consumer: %s\n",
                  proc->errstr);
          return 0;
     }

     /* Add broker(s) */
     if (rd_kafka_brokers_add(proc->rk, proc->brokers) < 1) {
          fprintf(stderr, "%% No valid brokers specified\n");
          return 0;
     }

     rd_kafka_poll_set_consumer(proc->rk);

     if (!proc->topics || !proc->topics->cnt) {
          tool_print("no topics specified");
          return 0;
     }
     rd_kafka_resp_err_t err;

     /* Start consuming */
     if ((err = rd_kafka_subscribe(proc->rk, proc->topics))) {
          fprintf(stderr,
                  "%% Failed to start consuming topics: %s\n",
                  rd_kafka_err2str(err));
          return 0;
     }


     proc->outtype_tuple =
          ws_register_source_byname(type_table, "TUPLE_TYPE", data_source, sv);


     if (proc->outtype_tuple == NULL) {
          tool_print("registration failed");
          return 0;
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
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     if ((input_type == dtype_tuple) && 
         (wslabel_match(type_table, port, "STAT") ||
          wslabel_match(type_table, port, "TRIGGER") ||
          wslabel_match(type_table, port, "STATS"))) {
          if (!proc->outtype_tuple) {
               proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
          }
          return proc_stats;
     }

     return NULL;
}

static int consume_error_tuple(proc_instance_t * proc,
                                rd_kafka_message_t * rkmessage) {
     wsdata_t * tuple = allocate_log_tuple(proc);
     if (!tuple) {
          return 0;
     }

     char * msg = "Consume error for topic";

     tuple_dupe_string(tuple, proc->label_msg, msg,
                            strlen(msg));

     if (rkmessage->rkt) {
          const char * topic = rd_kafka_topic_name(rkmessage->rkt);
          if (topic && strlen(topic)) {
               tuple_dupe_string(tuple, proc->label_topic, topic,
                                 strlen(topic));
          }
     }
     tuple_member_create_int32(tuple,
                               rkmessage->partition,
                               proc->label_partition);
     tuple_member_create_int64(tuple,
                               rkmessage->offset,
                               proc->label_offset);

     const char * err = rd_kafka_message_errstr(rkmessage);
     if (err && strlen(err)) {
          tuple_dupe_string(tuple, proc->label_err, err,
                            strlen(err));
     }

     ws_set_outdata(tuple, proc->outtype_tuple, proc->dout);
     return 1;

}
static void consume_error_print(proc_instance_t * proc,
                                rd_kafka_message_t * rkmessage) {

     if (proc && proc->tuple_logs && proc->dout) {
          if (consume_error_tuple(proc, rkmessage)) {
               return;
          }
     }
     
	fprintf(stderr,"%% Consume error for topic \"%s\" [%"PRId32"] "
		       "offset %"PRId64": %s\n",
		       rkmessage->rkt ? rd_kafka_topic_name(rkmessage->rkt):"",
		       rkmessage->partition,
		       rkmessage->offset,
		       rd_kafka_message_errstr(rkmessage));
}

//callback per kafka message
static void msg_consume (rd_kafka_message_t *rkmessage, void *vproc) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
               dprint("%% Consumer reached end of "
                      "%s [%"PRId32"] "
                      "message queue at offset %"PRId64"\n",
                      rd_kafka_topic_name(rkmessage->rkt),
                      rkmessage->partition, rkmessage->offset);
               return;
          }

          consume_error_print(proc, rkmessage);


                /*
                   if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
                    rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
                        run = 0;
                 */
		return;
	}
     if (rkmessage->len) {

          //allocate tuple
          wsdata_t * tuple = wsdata_alloc(dtype_tuple);
          if (!tuple) {
               return;
          }
          wsdata_add_label(tuple, proc->label_kafka);
          wsdata_add_label(tuple, proc->label_data);
          const char * topic = rd_kafka_topic_name(rkmessage->rkt);
          if (topic) {
               tuple_dupe_string(tuple, proc->label_topic, topic,
                                 strlen(topic));
          }
          if (proc->stringdetect) {
               int isbinary = 0;
               int i;
               int len = (int)rkmessage->len;
               char * buf = (char *)rkmessage->payload;
               for (i = 0; i < len; i++) {
                    if (!isprint(buf[i])) {
                         isbinary = 1;
                         break;
                    }
               }
               if (isbinary) {
                    tuple_dupe_binary(tuple, proc->label_buf, (char *)rkmessage->payload,
                            (int)rkmessage->len);
               }
               else {
                    tuple_dupe_string(tuple, proc->label_buf, (char *)rkmessage->payload,
                                      (int)rkmessage->len);
               }
          }
          else {
               tuple_dupe_binary(tuple, proc->label_buf, (char *)rkmessage->payload,
                                 (int)rkmessage->len);
          }
          tuple_member_create_int(tuple, rkmessage->partition,
                                  proc->label_partition);

          ws_set_outdata(tuple, proc->outtype_tuple, proc->dout);
          proc->outcnt++;
          proc->outlen += rkmessage->len;

     }
     dprint("payload> %.*s\n",
            (int)rkmessage->len,
            (char *)rkmessage->payload);
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
//
static int data_source(void * vinstance, wsdata_t* source_data,
                       ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->dout = dout;

     proc->meta_process_cnt++;
     rd_kafka_message_t *rkmessage;

     rkmessage = rd_kafka_consumer_poll(proc->rk, 1000);
     if (rkmessage) {
          dprint("got message");
          msg_consume(rkmessage, proc);
          rd_kafka_message_destroy(rkmessage);
     }

     return 1;
}

//append current stats when a stats tuple is sent on the STAT port 
static int proc_stats(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     tuple_member_create_uint64(input_data, proc->outcnt,
                                proc->label_outcnt);
     tuple_member_create_uint64(input_data, proc->outlen,
                                proc->label_outlen);
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("polling loop cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);


     //turn off logging to tuples
     proc->tuple_logs = 0;
     proc->dout = NULL;

     if (proc->rk) {
          rd_kafka_resp_err_t err;
          err = rd_kafka_consumer_close(proc->rk);
          if (err) {
               fprintf(stderr, "%% Failed to close consumer: %s\n",
                       rd_kafka_err2str(err));
          }
     }
     if (proc->topics) {
          rd_kafka_topic_partition_list_destroy(proc->topics);
     }

     if (proc->rk) {
          rd_kafka_destroy(proc->rk);
     }
     /* Let background threads clean up and terminate cleanly. */
     tool_print("kafka listener destroy");
     rd_kafka_wait_destroyed(2000);

     //free dynamic allocations
     free(proc);
     return 1;
}

