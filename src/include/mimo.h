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

#ifndef _MIMO_H
#define _MIMO_H

#include <stdint.h>
#include "waterslide.h"
#include "waterslide_io.h"
#include "waterslidedata.h"
#include "init.h"
#include "shared/tarjan_graph.h"
#include "shared/shared_queue.h"
#include "failoverqueue.h"
#include "listhash.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#define MAX_DEPRECATED_COUNT 32
#define MAX_DEPRECATED_NAME 64

typedef struct _mimo_datalists_t {
     listhash_t * dtype_table;
     listhash_t * label_table;
     listhash_t * kidshare_table;
     uint32_t index_len;
     uint32_t monitors;
} mimo_datalists_t;

#define MAX_FLUSH_SUBS 128

typedef struct _mimo_flush_t {
     ws_outtype_t outtype_flush;
     ws_subscriber_t * sub[MAX_FLUSH_SUBS];
     int cnt;
} mimo_flush_t;

typedef struct _mimo_directory_list_t {
     char ** directories;
     int len;
     int longest_path_len;
} mimo_directory_list_t;

typedef struct _mimo_graph_cycle_t {
     int nthreads; // number of threads in a given cycle
     int * threadlist; // the thread list in this cycle
     uint8_t * fullshq; // a 0/1-indicator for whether a given thread can/cannot enqueue metadata
} mimo_graph_cycle_t;

struct _mimo_t {
     mimo_source_t * sources;
     mimo_sink_t * sinks;
     nhqueue_t * edges;
     ws_proc_instance_t * proc_instance_head;
     ws_proc_instance_t * proc_instance_tail;
     ws_source_list_t * proc_sources;
     ws_source_list_t * proc_monitors;
     mimo_datalists_t datalists;
     listhash_t * proc_module_list;
     char * proc_module_path;
     mimo_directory_list_t * kid_dirlist;
     nhqueue_t * subs;

     listhash_t * deprecated_list;

     // local thread queues
     nhqueue_t ** jobq;
     nhqueue_t ** jobq_freeq;
     nhqueue_t ** sink_freeq; // data to hand off

#ifdef WS_PTHREADS
     failoverqueue_t ** failoverq; // used to interrupt potential deadlock

     // single-writer, single-reader shared (external) thread queues AND
     // multiple-writer, single-reader shared (external) thread queues
     // are defined as shared_queue_t; the actual choice will be based on
     // whether the processing graph dictates multiple writers
     shared_queue_t ** shared_jobq;

     // Tarjan graph for detecting all sharedQ-based cycles of a WS graph
     tarjan_graph_t * tg;
     mimo_graph_cycle_t * mgc; // do not allocate memory for 'mgc' if there is no detected cycle in processing graph
     uint8_t * thread_in_cycle; // 0/1-indicator of whether a thread id belongs to one or more threads
#endif // WS_PTHREADS

     // this lock is needed during mimo flushes to guarantee
     // thread safeness of some accessed globals at flush time
     WS_MUTEX_DECL(lock);

     parse_graph_t * parsed_graph;
     mimo_flush_t flush;
     FILE * graphviz_fp;
     FILE * graphviz_p_fp;
     uint8_t verbose;
     uint8_t valgrind_dbg;
     uint8_t input_validate;
     int kid_uid;
     unsigned int srand_seed;
     int no_flush_on_exit;
     uint32_t thread_id; // should simply be zero for non-pthreads run
     int thread_global_offset; // thread ids are offset by this value
};

struct _mimo_source_t {
     char * name;
     mimo_t * mimo;  //backreference..
     ws_proc_edge_t * edges;
     ws_outtype_t outtype;
     struct _mimo_source_t * next;
};

struct _mimo_sink_t {
     char * name;
     wsdatatype_t * dtype;
     wslabel_t * input_label;
     nhqueue_t * dataq; // data to hand off
     nhqueue_t * sink_freeq;
     struct _mimo_sink_t * next;
};

int check_mimo_sink(mimo_t * mimo, const char * sink_name);
int check_mimo_source(mimo_t * mimo, const char * source_name);
mimo_source_t * mimo_lookup_source(mimo_t * mimo, const char * source_name);
mimo_sink_t * mimo_lookup_sink(mimo_t * mimo, const char * sink_name);
void mimo_using_deprecated(mimo_t * mimo, const char * deprecated_feature);
void mimo_print_deprecated(mimo_t * mimo);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _MIMO_H
