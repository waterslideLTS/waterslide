/*  

proc_kafka_out - module for waterslide

Copyright 2019 Morgan Stanley

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
//#define DEBUG 1
#define PROC_NAME "kafka_out"
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
char *proc_tags[]     = { "output", NULL };
char *proc_alias[]     = { "kafka_produce", "kafka_output", "out_kafka", "produce_kafka", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "kafka producer to single topic";
char proc_nonswitch_opts[] = "tuple item to publish to topic";
proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'b',"","broker",
     "Specify a host and port for broker",0,0},
     {'t',"","topic",
     "topic to produce to",0,0},
     {'k',"","key",
     "item in tuple to be used as a key for kafka partitioning",0,0},
     {'X',"","option",
     "special kafka option for additional tuning and configuration",0,0},
     {'w',"","seconds",
     "set how many seconds to wait to flush kafka enqueue buffers during flush",0,0},
     {'r',"","msec",
     "set how many milli-seconds to wait when publishing fails",0,0},
     {'R',"","count",
     "set how many times a retry should wait before publishing fails",0,0},
     {'Z',"","",
     "zero copy buffers",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

// Ports: 
proc_port_t proc_input_ports[] =  {
     {"none","Store value at key"},
     {"STATS","trigger reporting of stats"},
     {NULL, NULL}
};


//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_stats(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;

     char * tasks;
     char * brokers;
     char * topic;

     int flush_wait_sec;

     uint64_t enqueued;
     uint64_t enqueue_fail;
     uint64_t outbytes;
     uint64_t delivery_fail;
     uint64_t delivery_success;
     uint64_t retry_succeed;
     uint64_t fail_loops;
     uint64_t queue_full_waits;
     uint64_t flush_fail;

     int retry_msec;
     int retry_cnt;


     wslabel_t * label_enqueued;
     wslabel_t * label_enqueue_fail;
     wslabel_t * label_success;
     wslabel_t * label_fail;
     wslabel_t * label_outbytes;
     wslabel_t * label_qdepth;
     wslabel_t * label_retry_succeed;
     wslabel_t * label_fail_loops;
     wslabel_t * label_queue_full_waits;

     rd_kafka_t *rk;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_topic_t *rkt;
	char errstr[512];

     wslabel_nested_set_t nest_key;
     wslabel_nested_set_t nest_value;

     wsdata_t * wsd_topic;
     ws_outtype_t * outtype_tuple;

     int zerocopy;
} proc_instance_t;

//handle options for command line config - like kafkacat or java clients do
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

     //try to set option as a topic option first
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

     while ((op = getopt(argc, argv, "r:R:Zzw:T:t:B:b:x:X:K:k:")) != EOF) {
          switch (op) {
          case 'R':
               proc->retry_cnt = atoi(optarg);
               break;
          case 'r':
               proc->retry_msec = atoi(optarg);
               break;
          case 'Z':
          case 'z':
               proc->zerocopy = 1;
               break;
          case 'W':
          case 'w':
               proc->flush_wait_sec = atoi(optarg);
               tool_print("waiting up to %d seconds to flush kafka output queue",
                          proc->flush_wait_sec);
               break;
          case 'T':
          case 't':
               proc->topic = optarg;
               break;
          case 'B':
          case 'b':
               proc->brokers = optarg;
               tool_print("using broker: %s", optarg);
               break;
          case 'x':
          case 'X':
               if (!handle_kafka_config_option(proc, optarg)) {
                    error_print("unable to configure kafka option");
                    return 0;
               }
               break;
          case 'K':
          case 'k':
               wslabel_nested_search_build(type_table, &proc->nest_key,
                                           optarg);
               break;
          default:
               return 0;
          }
     }

     while (optind < argc) {
          //read in as nested item
          wslabel_nested_search_build(type_table, &proc->nest_value,
                                      argv[optind]);
          optind++;
     }

     return 1;
}


//register callbacks for error conditions
static void err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
	fprintf(stderr, "%% ERROR CALLBACK: %s: %s: %s\n",
	       rd_kafka_name(rk), rd_kafka_err2str(err), reason);
}

//register callback for throttle conditions
static void throttle_cb (rd_kafka_t *rk, const char *broker_name,
			 int32_t broker_id, int throttle_time_ms,
			 void *opaque) {
	fprintf(stderr, "%% THROTTLED %dms by %s (%"PRId32")\n", throttle_time_ms,
	       broker_name, broker_id);
}

//register callback for message delivery - record every produce message
static void dr_msg_cb (rd_kafka_t *rk,
                       const rd_kafka_message_t *rkmessage, void *vproc) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
	if (rkmessage->err) {
          proc->delivery_fail++;
		dprint("%% Message delivery failed: %s",
                 rd_kafka_err2str(rkmessage->err));
	}
	else {
          proc->delivery_success++;
		dprint("%% Message delivered (%zd bytes, partition %"PRId32")",
                 rkmessage->len, rkmessage->partition);
	}
}

static void dr_msg_cb_zerocopy (rd_kafka_t *rk,
                       const rd_kafka_message_t *rkmessage, void *vproc) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
	if (rkmessage->err) {
          proc->delivery_fail++;
		dprint("%% zq_Message delivery failed: %s",
                 rd_kafka_err2str(rkmessage->err));
	}
	else {
          proc->delivery_success++;
		dprint("%% zq_Message delivered (%zd bytes, partition %"PRId32")",
                 rkmessage->len, rkmessage->partition);
	}
     wsdata_t *ref = (wsdata_t *)rkmessage->_private;
     if (ref) {
          wsdata_delete(ref);
     }
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


     proc->retry_cnt = 1;
     proc->retry_msec = 1000;
     proc->flush_wait_sec = 10; //wait up to 10 seconds

     proc->label_enqueued = wsregister_label(type_table, "ENQUEUED");
     proc->label_enqueue_fail = wsregister_label(type_table, "ENQUEUE_FAIL");
     proc->label_success = wsregister_label(type_table, "SUCCESS");
     proc->label_fail = wsregister_label(type_table, "FAIL");
     proc->label_outbytes = wsregister_label(type_table, "OUTBYTES");
     proc->label_qdepth = wsregister_label(type_table, "QUEUEDEPTH");
     proc->label_retry_succeed = wsregister_label(type_table, "RETRY_SUCCEED");
     proc->label_fail_loops = wsregister_label(type_table, "FAIL_LOOPS");
     proc->label_queue_full_waits = wsregister_label(type_table, "QUEUE_FULL_WAITS");

     //set up callbacks for output stats
     proc->conf = rd_kafka_conf_new();
     rd_kafka_conf_set_opaque(proc->conf, proc);
	rd_kafka_conf_set_error_cb(proc->conf, err_cb);
	rd_kafka_conf_set_throttle_cb(proc->conf, throttle_cb);

	/* Quick termination */
     char tmp[128];
	snprintf(tmp, sizeof(tmp), "%i", SIGIO);
	rd_kafka_conf_set(proc->conf, "internal.termination.signal", tmp, NULL, 0);

     /* Kafka topic configuration */
	proc->topic_conf = rd_kafka_topic_conf_new();

     //default broker
     proc->brokers = "localhost:9092";

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (proc->zerocopy) {
          rd_kafka_conf_set_dr_msg_cb(proc->conf, dr_msg_cb_zerocopy);
     }
     else {
          rd_kafka_conf_set_dr_msg_cb(proc->conf, dr_msg_cb);
     }

     if (!proc->nest_value.cnt) {
          tool_print("must specify a value to emit to kafka");
     }
     if (!proc->topic) {
          tool_print("ERROR: kafka topic needs to be specified");
          return 0;
     }

     /* Create Kafka handle */
     proc->rk = rd_kafka_new(RD_KAFKA_PRODUCER, proc->conf,
                             proc->errstr, sizeof(proc->errstr));
     if (!proc->rk) {
          fprintf(stderr,
                  "%% Failed to create Kafka producer: %s\n",
                  proc->errstr);
          return 0;
     }

     /* Add broker(s) */
     if (rd_kafka_brokers_add(proc->rk, proc->brokers) < 1) {
          fprintf(stderr, "%% No valid brokers specified\n");
          return 0;
     }

     /* Create topic to produce to */
     proc->rkt = rd_kafka_topic_new(proc->rk, proc->topic, proc->topic_conf);
     if (!proc->rkt) {
         fprintf(stderr, "%% Failed to create topic object: %s\n",
                 rd_kafka_err2str(rd_kafka_last_error()));
         rd_kafka_destroy(proc->rk); 
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

     if (wsdatatype_match(type_table, input_type, "FLUSH_TYPE")) {
          return proc_flush;
     }

     if (wslabel_match(type_table, port, "STAT") || 
         wslabel_match(type_table, port, "TRIGGER") ||
         wslabel_match(type_table, port, "STATS")) {
          if (input_type == dtype_tuple) {
               proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
               return proc_stats;
          }
          return NULL;
     }

     if (input_type == dtype_tuple) {
          return proc_tuple;
     }

     return NULL;
}

//emit a buffer to a kafka topic
static int write_kafka(proc_instance_t * proc, void * buf, size_t len,
                       const void* key, size_t klen, wsdata_t * data,
                       wsdata_t * kdata) {

     int flags = RD_KAFKA_MSG_F_COPY;
     if (proc->zerocopy) {
          wsdata_add_reference(data);
          if (kdata) {
               wsdata_assign_dependency(kdata, data);
          }
          flags = 0;

     }

     int retry = 0;
     while (retry <= proc->retry_cnt) {
          if (rd_kafka_produce(proc->rkt,
                               RD_KAFKA_PARTITION_UA,  //use builtin key'd partitioner
                               /* Make a copy of the payload. */
                               flags,
                               /* Message payload (value) and length */
                               buf, len,
                               key, klen,
                               data) == -1) {
		
               //handle failure to produce
               dprint("%% Failed to produce to topic %s: %s\n",
                      rd_kafka_topic_name(proc->rkt),
                      rd_kafka_err2str(rd_kafka_last_error()));

               dprint("fail to produce - loop %d", retry + 1);
               proc->fail_loops++;
               /* Poll to handle delivery reports */
               if (rd_kafka_last_error() ==
                   RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                    //try to see if queue can be flushed - wait a sec
                    dprint("queue full error");
                    proc->queue_full_waits++;
                    rd_kafka_poll(proc->rk, proc->retry_msec); //blocking
               }
               else {
                    dprint("other production error");
                    rd_kafka_poll(proc->rk, 0); //blocking
                    usleep(1000 * proc->retry_msec);
               }
               retry++;
          }
          else {
               if (retry) {
                    dprint("retry success after %d loops", retry);
                    proc->retry_succeed++;
               }
               proc->enqueued++;
               proc->outbytes += len;
               dprint( "%% Enqueued message (%zd bytes) "
                       "for topic %s",
                       len, rd_kafka_topic_name(proc->rkt));
               rd_kafka_poll(proc->rk, 0/*non-blocking*/);
               return 1;
          }
     }

     //if reached here - retry has failed
     proc->enqueue_fail++;
     proc->delivery_fail++;

     if (proc->zerocopy) {
          wsdata_delete(data); //delete reference
     }

     return 0;
}

//only select first key found as key to use
static int proc_nest_key_callback(void * vproc, void * vkdata,
                                  wsdata_t * tdata, wsdata_t * member) {
     //proc_instance_t * proc = (proc_instance_t*)vproc;
     wsdata_t ** pdata = (wsdata_t**)vkdata;
     wsdata_t * key = *pdata;

     if (key) {
          return 0;
     }
     *pdata = member;

     return 1;
}

//called for each found value to emit to kafka
static int proc_nest_value_callback(void * vproc, void * vkdata,
                                  wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     wsdata_t * key = (wsdata_t*)vkdata;

     char * kbuf = NULL;
     int klen = 0;
     if (key) {
          dtype_string_buffer(key, &kbuf, &klen);
          dprint("using key %.*s", klen, kbuf);
     }

     char * vbuf = NULL;
     int vlen = 0;
     if (dtype_string_buffer(member, &vbuf, &vlen) && 
         (vlen != 0) && (vbuf != NULL)) {
          write_kafka(proc, vbuf, vlen, kbuf, klen, member, key);
          return 1;
     }
     else { 
          return 0;
     }
}



//// proc processing function assigned to a specific data type in proc_io_init
//
static int proc_tuple(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     wsdata_t * key = NULL;

     //search for key
     if (proc->nest_key.cnt) {
          tuple_nested_search(input_data, &proc->nest_key,
                              proc_nest_key_callback,
                              proc, &key);
     }

     //search for value - emit as kafka item
     tuple_nested_search(input_data, &proc->nest_value,
                         proc_nest_value_callback,
                         proc, key);



     proc->meta_process_cnt++;

     return 1;
}

//flush kafka produce buffer -- prepare for exit
static int proc_flush(void * vinstance, wsdata_t* source_data,
                      ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     fprintf(stderr,
             "%% Flushing Kafka final %d messages..\n",
             rd_kafka_outq_len(proc->rk));
     rd_kafka_flush(proc->rk, proc->flush_wait_sec * 1000 /* wait for max t seconds */);
     tool_print("finished flushing kafka producer");

     /* If the output queue is still not empty there is an issue
      * with producing messages to the clusters. */
     int leftover = rd_kafka_outq_len(proc->rk);
     if (leftover > 0) {
          fprintf(stderr, "%% %d message(s) were probably not delivered\n",
                  leftover);
          proc->delivery_fail += leftover;
          proc->flush_fail += leftover;
     }
     return 1;
}

//append current stats when a stats tuple is sent on the STAT port 
static int proc_stats(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     tuple_member_create_uint64(input_data, proc->enqueued,
                                proc->label_enqueued);
     tuple_member_create_uint64(input_data, proc->enqueue_fail,
                                proc->label_enqueue_fail);
     tuple_member_create_uint64(input_data, proc->retry_succeed,
                                proc->label_retry_succeed);
     tuple_member_create_uint64(input_data, proc->outbytes,
                                proc->label_outbytes);
     tuple_member_create_uint64(input_data, proc->delivery_success,
                                proc->label_success);
     tuple_member_create_uint64(input_data, proc->delivery_fail,
                                proc->label_fail);
     tuple_member_create_uint64(input_data, rd_kafka_outq_len(proc->rk),
                                proc->label_qdepth);
     tuple_member_create_uint64(input_data, proc->fail_loops,
                                proc->label_fail_loops);
     tuple_member_create_uint64(input_data, proc->queue_full_waits,
                                proc->label_queue_full_waits);

     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt         %" PRIu64, proc->meta_process_cnt);
     tool_print("produce outbytes      %" PRIu64, proc->outbytes);
     tool_print("produce enqueued      %" PRIu64, proc->enqueued);
     tool_print("produce success       %" PRIu64, proc->delivery_success);
     tool_print("--- errors ----");
     tool_print("produce event fails   %" PRIu64, proc->delivery_fail);
     tool_print(" enqueue fail         %" PRIu64, proc->enqueue_fail);
     tool_print(" fail on exit flush   %" PRIu64, proc->flush_fail);
     tool_print("--- retry stats ----");
     tool_print("enqueue retry success %" PRIu64, proc->retry_succeed);
     tool_print("fail loops            %" PRIu64, proc->fail_loops);
     tool_print("queue full waits      %" PRIu64, proc->queue_full_waits);

     if (proc->rk) {
          rd_kafka_destroy(proc->rk);
     }

     //free dynamic allocations
     free(proc);
     return 1;
}

