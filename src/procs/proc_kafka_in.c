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
 * Apache Kafka consumer & producer performance tester
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

 
#define PROC_NAME "kafka_in"
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
char proc_purpose[]    = "single topic kafka subscriber";
char proc_nonswitch_opts[] = "kafka topic to listen";
proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'b',"","broker",
     "Specify a host and port for broker",0,0},
     {'p',"","partition",
     "Specify partiton for topic",0,0},
     {'L',"","label",
     "Specify an output label default DATA",0,0},
     {'S',"","",
     "detect if buffer is ascii strings",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

//function prototypes for local functions
static int data_source(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     char * tasks;
     char * brokers;
     int partition;
     char * topic;
     wslabel_t * label_buf;
     wslabel_t * label_tuple;
     wslabel_t * label_topic;
     wslabel_t * label_datetime;

     rd_kafka_t *rk;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_queue_t *rkqu;
	rd_kafka_topic_t *rkt;
	char errstr[512];
     ws_doutput_t * dout;

     wsdata_t * wsd_topic;
     int stringdetect;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;

     while ((op = getopt(argc, argv, "Sb:p:L:")) != EOF) {
          switch (op) {
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
          default:
               return 0;
          }
     }

     while (optind < argc) {
          proc->topic = strdup(argv[optind]);
          tool_print("reading from topic %s", argv[optind]);
          optind++;
     }

     return 1;
}

static void err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
	printf("%% ERROR CALLBACK: %s: %s: %s\n",
	       rd_kafka_name(rk), rd_kafka_err2str(err), reason);
}

static void throttle_cb (rd_kafka_t *rk, const char *broker_name,
			 int32_t broker_id, int throttle_time_ms,
			 void *opaque) {
	printf("%% THROTTLED %dms by %s (%"PRId32")\n", throttle_time_ms,
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

	int64_t start_offset = RD_KAFKA_OFFSET_BEGINNING;

     proc->label_buf = wsregister_label(type_table, "BUF");
     proc->label_tuple = wsregister_label(type_table, "KAFKA");
     proc->label_topic = wsregister_label(type_table, "TOPIC");
     proc->label_datetime = wsregister_label(type_table, "DATETIME");

     proc->conf = rd_kafka_conf_new();
	rd_kafka_conf_set_error_cb(proc->conf, err_cb);
	rd_kafka_conf_set_throttle_cb(proc->conf, throttle_cb);

	/* Quick termination */
     char tmp[128];
	snprintf(tmp, sizeof(tmp), "%i", SIGIO);
	rd_kafka_conf_set(proc->conf, "internal.termination.signal", tmp, NULL, 0);

	/* Consumer config */
	/* Tell rdkafka to (try to) maintain 1M messages
	 * in its internal receive buffers. This is to avoid
	 * application -> rdkafka -> broker  per-message ping-pong
	 * latency.
	 * The larger the local queue, the higher the performance.
	 * Try other values with: ... -X queued.min.messages=1000
	 */
	rd_kafka_conf_set(proc->conf, "queued.min.messages", "1000000", NULL, 0);
	rd_kafka_conf_set(proc->conf, "session.timeout.ms", "6000", NULL, 0);

	/* Kafka topic configuration */
	proc->topic_conf = rd_kafka_topic_conf_new();
	rd_kafka_topic_conf_set(proc->topic_conf, "auto.offset.reset", "earliest",
                             NULL, 0);

     proc->brokers = "localhost:9092";

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }
     if (!proc->topic) {
          tool_print("ERROR: kafka topic needs to be specified");
          return 0;
     }
     proc->wsd_topic = wsdata_create_string(proc->topic, strlen(proc->topic));
     if (!proc->wsd_topic) {
          tool_print("unable to create topic");
          return 0;
     }
     wsdata_add_reference(proc->wsd_topic);
     wsdata_add_label(proc->wsd_topic, proc->label_topic);

     /* Create Kafka handle */
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

     /* Create topic to consume from */
     proc->rkt = rd_kafka_topic_new(proc->rk, proc->topic, proc->topic_conf);

     /* Start consuming */
     proc->rkqu = rd_kafka_queue_new(proc->rk);
     tool_print("using parition %d", proc->partition);
     const int r = rd_kafka_consume_start_queue(proc->rkt,
                                                proc->partition, start_offset, proc->rkqu);

     if (r == -1) {
          fprintf(stderr, "%% Error creating queue: %s\n",
                  rd_kafka_err2str(rd_kafka_last_error()));
          return 0;
     }

     proc->outtype_tuple =
          ws_register_source_byname(type_table, "TUPLE_TYPE", data_source, sv);


     if (proc->outtype_tuple == NULL) {
          fprintf(stderr, "registration failed\n");
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
     return NULL;
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

		printf("%% Consume error for topic \"%s\" [%"PRId32"] "
		       "offset %"PRId64": %s\n",
		       rkmessage->rkt ? rd_kafka_topic_name(rkmessage->rkt):"",
		       rkmessage->partition,
		       rkmessage->offset,
		       rd_kafka_message_errstr(rkmessage));


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
          add_tuple_member(tuple, proc->wsd_topic);
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

          ws_set_outdata(tuple, proc->outtype_tuple, proc->dout);
          proc->outcnt++;

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

     int r = rd_kafka_consume_callback_queue(proc->rkqu, 1000,
                                         msg_consume,
                                         proc);
     
     if (r == -1) {
          fprintf(stderr, "%% Error: %s\n",
                  rd_kafka_err2str(rd_kafka_last_error()));
     }
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     if (proc->rkt) {
          tool_print("kafka stopping consumer");
          int r = rd_kafka_consume_stop(proc->rkt, proc->partition);

          if (r != 0) {
               fprintf(stderr, "%% Error: %s\n",
                       rd_kafka_err2str(rd_kafka_last_error()));
          }
     }
     if (proc->rkqu) {
          rd_kafka_queue_destroy(proc->rkqu);
     }
     if (proc->rk) {
          rd_kafka_destroy(proc->rk);
     }
     /* Let background threads clean up and terminate cleanly. */
     tool_print("kafka listener destroy");
     rd_kafka_wait_destroyed(2000);

     if (proc->wsd_topic) {
          wsdata_delete(proc->wsd_topic);
     }
     //free dynamic allocations
     free(proc);
     return 1;
}

