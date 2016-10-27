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

#if 0
#define _GNU_SOURCE /* for strndup() */
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */
/* Do not include these defines from your program, they will not be
 * provided by librdkafka. */
//#include "rd.h"
//#include "rdtime.h"

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

static void msg_consume (rd_kafka_message_t *rkmessage, void *opaque) {
	fprintf(stderr, "called\n");
	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                        if (verbosity >= 1)
                                printf("%% Consumer reached end of "
                                       "%s [%"PRId32"] "
                                       "message queue at offset %"PRId64"\n",
                                       rd_kafka_topic_name(rkmessage->rkt),
                                       rkmessage->partition, rkmessage->offset);

			if (exit_eof && ++eof_cnt == partition_cnt)
				run = 0;

			return;
		}

		printf("%% Consume error for topic \"%s\" [%"PRId32"] "
		       "offset %"PRId64": %s\n",
		       rkmessage->rkt ? rd_kafka_topic_name(rkmessage->rkt):"",
		       rkmessage->partition,
		       rkmessage->offset,
		       rd_kafka_message_errstr(rkmessage));

                if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
                    rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
                        run = 0;

		return;
	}
        printf("payload> %.*s\n",
			(int)rkmessage->len,
			(char *)rkmessage->payload);

}
int main (int argc, char **argv) {
	char *brokers = NULL;
	char mode = 'C';
	char *topic = NULL;
        int *partitions = NULL;
	int opt;
	int sendflags = 0;
	const char *debug = NULL;
	char errstr[512];
	int seed = time(NULL);
        rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_queue_t *rkqu = NULL;
	int64_t start_offset = 0;
	int batch_size = 0;
        const char *stats_cmd = NULL;
        char *stats_intvlstr = NULL;
        char tmp[128];
	rd_kafka_topic_partition_list_t *topics;

	/* Kafka configuration */
	conf = rd_kafka_conf_new();
	rd_kafka_conf_set_error_cb(conf, err_cb);
	rd_kafka_conf_set_throttle_cb(conf, throttle_cb);

	/* Quick termination */
	snprintf(tmp, sizeof(tmp), "%i", SIGIO);
	rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);

	/* Producer config */
	rd_kafka_conf_set(conf, "queue.buffering.max.messages", "500000",
			  NULL, 0);
	rd_kafka_conf_set(conf, "message.send.max.retries", "3", NULL, 0);
	rd_kafka_conf_set(conf, "retry.backoff.ms", "500", NULL, 0);

	/* Consumer config */
	/* Tell rdkafka to (try to) maintain 1M messages
	 * in its internal receive buffers. This is to avoid
	 * application -> rdkafka -> broker  per-message ping-pong
	 * latency.
	 * The larger the local queue, the higher the performance.
	 * Try other values with: ... -X queued.min.messages=1000
	 */
	rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0);
	rd_kafka_conf_set(conf, "session.timeout.ms", "6000", NULL, 0);

	/* Kafka topic configuration */
	topic_conf = rd_kafka_topic_conf_new();
	rd_kafka_topic_conf_set(topic_conf, "auto.offset.reset", "earliest",
				NULL, 0);

	topics = rd_kafka_topic_partition_list_new(1);

	while ((opt =
		getopt(argc, argv,
		       "PCt:p:b:s:k:c:fi:MDd:m:S:x:"
                       "R:a:z:o:X:B:eT:Y:qvIur:lA:OwN")) != -1) {
		switch (opt) {
		case 'P':
		case 'C':
			mode = opt;
			break;
		case 't':
			rd_kafka_topic_partition_list_add(topics, optarg,
							  RD_KAFKA_PARTITION_UA);
			break;
		case 'p':
			partitions = realloc(partitions, ++partition_cnt);
			partitions[partition_cnt-1] = atoi(optarg);
			break;

		case 'b':
			brokers = optarg;
			break;
		case 'D':
			sendflags |= RD_KAFKA_MSG_F_FREE;
			break;
		case 'i':
			dispintvl = atoi(optarg);
			break;
		case 'x':
			exit_after = atoi(optarg);
			break;
		case 'R':
			seed = atoi(optarg);
			break;
		case 'a':
			if (rd_kafka_topic_conf_set(topic_conf,
						    "request.required.acks",
						    optarg,
						    errstr, sizeof(errstr)) !=
			    RD_KAFKA_CONF_OK) {
				fprintf(stderr, "%% %s\n", errstr);
				exit(1);
			}
			break;
		case 'B':
			batch_size = atoi(optarg);
			break;
		case 'o':
			if (!strcmp(optarg, "end"))
				start_offset = RD_KAFKA_OFFSET_END;
			else if (!strcmp(optarg, "beginning"))
				start_offset = RD_KAFKA_OFFSET_BEGINNING;
			else if (!strcmp(optarg, "stored"))
				start_offset = RD_KAFKA_OFFSET_STORED;
			else {
				start_offset = strtoll(optarg, NULL, 10);

				if (start_offset < 0)
					start_offset = RD_KAFKA_OFFSET_TAIL(-start_offset);
			}

			break;
		case 'e':
			exit_eof = 1;
			break;
		case 'd':
			debug = optarg;
			break;
		case 'X':
		{
			char *name, *val;
			rd_kafka_conf_res_t res;

			if (!strcmp(optarg, "list") ||
			    !strcmp(optarg, "help")) {
				rd_kafka_conf_properties_show(stdout);
				exit(0);
			}

			name = optarg;
			if (!(val = strchr(name, '='))) {
				fprintf(stderr, "%% Expected "
					"-X property=value, not %s\n", name);
				exit(1);
			}

			*val = '\0';
			val++;

			res = RD_KAFKA_CONF_UNKNOWN;
			/* Try "topic." prefixed properties on topic
			 * conf first, and then fall through to global if
			 * it didnt match a topic configuration property. */
			if (!strncmp(name, "topic.", strlen("topic.")))
				res = rd_kafka_topic_conf_set(topic_conf,
							      name+
							      strlen("topic."),
							      val,
							      errstr,
							      sizeof(errstr));

			if (res == RD_KAFKA_CONF_UNKNOWN)
				res = rd_kafka_conf_set(conf, name, val,
							errstr, sizeof(errstr));

			if (res != RD_KAFKA_CONF_OK) {
				fprintf(stderr, "%% %s\n", errstr);
				exit(1);
			}
		}
		break;

		case 'T':
                        stats_intvlstr = optarg;
			break;
                case 'Y':
                        stats_cmd = optarg;
                        break;

		case 'q':
                        verbosity--;
			break;

		case 'v':
                        verbosity++;
			break;

		case 'A':
			if (!(latency_fp = fopen(optarg, "w"))) {
				fprintf(stderr,
					"%% Cant open %s: %s\n",
					optarg, strerror(errno));
				exit(1);
			}
                        break;

                case 'O':
                        if (rd_kafka_topic_conf_set(topic_conf,
                                                    "produce.offset.report",
                                                    "true",
                                                    errstr, sizeof(errstr)) !=
                            RD_KAFKA_CONF_OK) {
                                fprintf(stderr, "%% %s\n", errstr);
                                exit(1);
                        }
                        report_offset = 1;
                        break;

		case 'M':
			incremental_mode = 1;
			break;

		case 'N':
			with_dr = 0;
			break;

		default:
                        fprintf(stderr, "Unknown option: %c\n", opt);
			goto usage;
		}
	}

	if (topics->cnt == 0 || optind != argc) {
                if (optind < argc)
                        fprintf(stderr, "Unknown argument: %s\n", argv[optind]);
	usage:
		fprintf(stderr,
			"Usage: %s [-C|-P] -t <topic> "
			"[-p <partition>] [-b <broker,broker..>] [options..]\n"
			"\n"
			"librdkafka version %s (0x%08x)\n"
			"\n"
			" Options:\n"
			"  -C | -P |    Consumer or Producer mode\n"
			"  -G <groupid> High-level Kafka Consumer mode\n"
			"  -t <topic>   Topic to consume / produce\n"
			"  -p <num>     Partition (defaults to random). "
			"Multiple partitions are allowed in -C consumer mode.\n"
			"  -M           Print consumer interval stats\n"
			"  -b <brokers> Broker address list (host[:port],..)\n"
			"  -s <size>    Message size (producer)\n"
			"  -c <cnt>     Messages to transmit/receive\n"
			"  -D           Copy/Duplicate data buffer (producer)\n"
			"  -i <ms>      Display interval\n"
			"  -m <msg>     Message payload pattern\n"
			"  -S <start>   Send a sequence number starting at "
			"<start> as payload\n"
			"  -R <seed>    Random seed value (defaults to time)\n"
			"  -a <acks>    Required acks (producer): "
			"-1, 0, 1, >1\n"
			"  -B <size>    Consume batch size (# of msgs)\n"
			"  -z <codec>   Enable compression:\n"
			"               none|gzip|snappy\n"
			"  -o <offset>  Start offset (consumer)\n"
			"               beginning, end, NNNNN or -NNNNN\n"
			"  -d [facs..]  Enable debugging contexts:\n"
			"               %s\n"
			"  -X <prop=name> Set arbitrary librdkafka "
			"configuration property\n"
			"               Properties prefixed with \"topic.\" "
			"will be set on topic object.\n"
			"               Use '-X list' to see the full list\n"
			"               of supported properties.\n"
			"  -T <intvl>   Enable statistics from librdkafka at "
			"specified interval (ms)\n"
                        "  -Y <command> Pipe statistics to <command>\n"
			"  -I           Idle: dont produce any messages\n"
			"  -q           Decrease verbosity\n"
                        "  -v           Increase verbosity (default 1)\n"
                        "  -u           Output stats in table format\n"
                        "  -r <rate>    Producer msg/s limit\n"
                        "  -l           Latency measurement.\n"
                        "               Needs two matching instances, one\n"
                        "               consumer and one producer, both\n"
                        "               running with the -l switch.\n"
			"  -A <file>    Write per-message latency stats to "
			"<file>. Requires -l\n"
                        "  -O           Report produced offset (producer)\n"
			"  -N           No delivery reports (producer)\n"
			"\n"
			" In Consumer mode:\n"
			"  consumes messages and prints thruput\n"
			"  If -B <..> is supplied the batch consumer\n"
			"  mode is used, else the callback mode is used.\n"
			"\n"
			" In Producer mode:\n"
			"  writes messages of size -s <..> and prints thruput\n"
			"\n",
			argv[0],
			rd_kafka_version_str(), rd_kafka_version(),
			RD_KAFKA_DEBUG_CONTEXTS);
		exit(1);
	}

	if (mode != 'C') {
		printf("this one only does consumer mode");
	}

	dispintvl *= 1000; /* us */

        if (verbosity > 1)
                printf("%% Using random seed %i, verbosity level %i\n",
                       seed, verbosity);
	srand(seed);
	signal(SIGINT, stop);
	signal(SIGUSR1, sig_usr1);


	if (debug &&
	    rd_kafka_conf_set(conf, "debug", debug, errstr, sizeof(errstr)) !=
	    RD_KAFKA_CONF_OK) {
		printf("%% Debug configuration failed: %s: %s\n",
		       errstr, debug);
		exit(1);
	}

        /* Always enable stats (for RTT extraction), and if user supplied
         * the -T <intvl> option we let her take part of the stats aswell. */
        rd_kafka_conf_set_stats_cb(conf, stats_cb);

        if (!stats_intvlstr) {
                /* if no user-desired stats, adjust stats interval
                 * to the display interval. */
                snprintf(tmp, sizeof(tmp), "%i", dispintvl / 1000);
        }

        if (rd_kafka_conf_set(conf, "statistics.interval.ms",
                              stats_intvlstr ? stats_intvlstr : tmp,
                              errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% %s\n", errstr);
                exit(1);
        }

        if (stats_intvlstr) {
                /* User enabled stats (-T) */

                if (stats_cmd) {
                        if (!(stats_fp = popen(stats_cmd, "we"))) {
                                fprintf(stderr,
                                        "%% Failed to start stats command: "
                                        "%s: %s", stats_cmd, strerror(errno));
                                exit(1);
                        }
                } else
                        stats_fp = stdout;
        }

	topic = topics->elems[0].topic;

	if (mode == 'C') {
		/*
		 * Consumer
		 */

		rd_kafka_message_t **rkmessages = NULL;
		size_t i = 0;

		/* Create Kafka handle */
		if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
					errstr, sizeof(errstr)))) {
			fprintf(stderr,
				"%% Failed to create Kafka consumer: %s\n",
				errstr);
			exit(1);
		}

                global_rk = rk;

		if (debug)
			rd_kafka_set_log_level(rk, 7);

		/* Add broker(s) */
		if (!brokers) {
                        //try reasonable default
			fprintf(stderr, "no brokers configured, trying reasonable default\n");
			brokers = "localhost:9092";
		}
		if (brokers && rd_kafka_brokers_add(rk, brokers) < 1) {
			fprintf(stderr, "%% No valid brokers specified\n");
			exit(1);
		}

		/* Create topic to consume from */
		rkt = rd_kafka_topic_new(rk, topic, topic_conf);

		/* Batch consumer */
		if (batch_size)
			rkmessages = malloc(sizeof(*rkmessages) * batch_size);

		/* Start consuming */
		rkqu = rd_kafka_queue_new(rk);
		for (i=0 ; i<(size_t)partition_cnt ; ++i) {
			const int r = rd_kafka_consume_start_queue(rkt,
				partitions[i], start_offset, rkqu);

			if (r == -1) {
				fprintf(stderr, "%% Error creating queue: %s\n",
					rd_kafka_err2str(
						rd_kafka_errno2err(errno)));
				exit(1);
			}
		}

		if (!partitions) {
			const int r = rd_kafka_consume_start_queue(rkt,
					0, start_offset, rkqu);

			if (r == -1) {
				fprintf(stderr, "%% Error creating queue: %s\n",
					rd_kafka_err2str(
						rd_kafka_errno2err(errno)));
				exit(1);
			}
		}
		int loopcnt = 0;
		while (run) {
                        fprintf(stderr, "loop %d\n", loopcnt++);
			/* Consume messages.
			 * A message may either be a real message, or
			 * an error signaling (if rkmessage->err is set).
			 */
			int r;

			if (batch_size) {
				int i;
				int partition = partitions ? partitions[0] :
				    RD_KAFKA_PARTITION_UA;

				/* Batch fetch mode */
				r = rd_kafka_consume_batch(rkt, partition,
							   1000,
							   rkmessages,
							   batch_size);
				if (r != -1) {
					for (i = 0 ; i < r ; i++) {
						msg_consume(rkmessages[i],
							NULL);
						rd_kafka_message_destroy(
							rkmessages[i]);
					}
				}
			} else {
				/* Queue mode */
				r = rd_kafka_consume_callback_queue(rkqu, 1000,
							msg_consume,
							NULL);
			}

			if (r == -1)
				fprintf(stderr, "%% Error: %s\n",
					rd_kafka_err2str(
						rd_kafka_errno2err(errno)));

			/* Poll to handle stats callbacks */
			rd_kafka_poll(rk, 0);
		}

		/* Stop consuming */
		for (i=0 ; i<(size_t)partition_cnt ; ++i) {
			int r = rd_kafka_consume_stop(rkt, i);
			if (r == -1) {
				fprintf(stderr,
					"%% Error in consume_stop: %s\n",
					rd_kafka_err2str(
						rd_kafka_errno2err(errno)));
			}
		}
		rd_kafka_queue_destroy(rkqu);

		/* Destroy topic */
		rd_kafka_topic_destroy(rkt);

		if (batch_size)
			free(rkmessages);

		/* Destroy the handle */
		rd_kafka_destroy(rk);

                global_rk = rk = NULL;

	}

	if (latency_fp)
		fclose(latency_fp);

        if (stats_fp) {
                pclose(stats_fp);
                stats_fp = NULL;
        }

	rd_kafka_topic_partition_list_destroy(topics);

	/* Let background threads clean up and terminate cleanly. */
	rd_kafka_wait_destroyed(2000);

	return 0;
}
#endif 
 
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
char proc_purpose[]    = "reads in buffers from kafka streams";

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'b',"","brokers",
     "Specify a string of brokers",0,0},
     {'p',"","partitions",
     "Specify a set of partitons",0,0},
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
     char * partitions;
     wslabel_t * label_buf;
     wslabel_t * label_tuple;
     wslabel_t * label_topics;
     wslabel_t * label_datetime;

     rd_kafka_t *rk;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_queue_t *rkqu;
	rd_kafka_topic_t *rkt;
	rd_kafka_topic_partition_list_t *topics;
	char errstr[512];
     ws_doutput_t * dout;

     int stringdetect;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;

     while ((op = getopt(argc, argv, "Sb:P:L:")) != EOF) {
          switch (op) {
          case 'b':
               proc->brokers = optarg;
               tool_print("using brokers: %s", optarg);
               break;
          case 'P':
               proc->partitions = optarg;
               tool_print("using partition: %s", optarg);
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
          rd_kafka_topic_partition_list_add(proc->topics, argv[optind],
                                            RD_KAFKA_PARTITION_UA);

          tool_print("reading from only topics %s", argv[optind]);
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
     proc->label_topics = wsregister_label(type_table, "TASK");
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

	proc->topics = rd_kafka_topic_partition_list_new(1);

     proc->brokers = "localhost:9092";

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

	char * topic = proc->topics->elems[0].topic;

     /* Create Kafka handle */
     if (!(proc->rk = rd_kafka_new(RD_KAFKA_CONSUMER, proc->conf,
                             proc->errstr, sizeof(proc->errstr)))) {
          fprintf(stderr,
                  "%% Failed to create Kafka consumer: %s\n",
                  proc->errstr);
          exit(1);
     }

     /* Add broker(s) */
     if (rd_kafka_brokers_add(proc->rk, proc->brokers) < 1) {
          fprintf(stderr, "%% No valid brokers specified\n");
          exit(1);
     }

     /* Create topic to consume from */
     proc->rkt = rd_kafka_topic_new(proc->rk, topic, proc->topic_conf);

     /* Start consuming */
     proc->rkqu = rd_kafka_queue_new(proc->rk);
     tool_print("partions not done yet - using parition 0");
     proc->partitions = NULL;
     if (!proc->partitions) {
          const int r = rd_kafka_consume_start_queue(proc->rkt,
                                                     0, start_offset, proc->rkqu);

          if (r == -1) {
               fprintf(stderr, "%% Error creating queue: %s\n",
                       rd_kafka_err2str(rd_kafka_errno2err(errno)));
               exit(1);
          }
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
                  rd_kafka_err2str(rd_kafka_errno2err(errno)));
     }
     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free dynamic allocations
     free(proc);
     return 1;
}

