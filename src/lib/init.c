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
//#define DEBUG 1
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "waterslide.h"
#include "listhash.h"
#include "waterslidedata.h"
#include "so_loader.h"
#include "init.h"
#include "wsqueue.h"
#include "mimo.h"
#include "parse_graph.h"
#include "wsperf.h"
#include "stringhash5.h"
#include "stringhash9a.h"
#include "shared/getrank.h"
#include "shared/barrier_init.h"
#include "shared/ws_init_threading.h"

#define MAX_CHARS 25

// Globals
#ifdef WS_PTHREADS
cpu_thread_info_t cpu_thread_mapper; // will hold each thread's cpu id (for init purposes,
                          // we need this to take negative quantities, so leave as
                          // int32_t NOT uint32_t)
uint32_t * num_shq_writers, ** shq_writers; // calloc'd in alloc_threadid_stuff()
uint64_t * os, *osf;
#endif // WS_PTHREADS
int user_threadid_zero_exists = 0;
uint32_t * src_threads;
extern uint32_t work_size, num_src_threads;


// this code is to implemented to be machine architecture-specific
// (for performance purposes). 
// TODO: replace the calloc's with the right allocation of memory to
//       make memory placement optimal for the indicated CPU
static void * ws_init_calloc(uint32_t cpu_id, size_t nmemb, size_t size) {
     return calloc(nmemb, size);
}

static void ws_free(void *ptr, size_t size) {
     free(ptr);
}

static ws_proc_instance_t * ws_new_proc_instance(mimo_t * mimo, const char * name, int argc, 
                                          char * const * argv) {

     ws_proc_instance_t * inst = 
          (ws_proc_instance_t *)calloc(1,sizeof(ws_proc_instance_t));
     if (!inst) {
          error_print("failed ws_new_proc_instance calloc of inst");
          return NULL;
     }

     inst->name = name;
     inst->argc = argc;
     inst->argv = argv;

     //if flow, process first..
     if (strcmp(name, "flow") == 0) {
          if (mimo->verbose) {
               status_print("found flow module - init first");
          }
          inst->next = mimo->proc_instance_head;
          mimo->proc_instance_head = inst;
          if (!mimo->proc_instance_tail) {
               mimo->proc_instance_tail = inst;
          }
          return inst;
     }
     
     //attach to list of instances..
     if (mimo->proc_instance_head == NULL) {
          mimo->proc_instance_head = inst;
          mimo->proc_instance_tail = inst;
     }
     else {
          mimo->proc_instance_tail->next = inst;
          mimo->proc_instance_tail = inst;
     }

     return inst;
}

static int ws_new_mimo_source_edge(mimo_t * mimo, mimo_source_t * src,
                             ws_proc_instance_t * dst, const char * port,
                             const char * src_label) {

     if (!src) {
          return 1;
     }
     //add edge to edge list
     if (!mimo->edges) {
          mimo->edges = queue_init();
     }
     ws_proc_edge_t * edge = 
          (ws_proc_edge_t *)calloc(1, sizeof(ws_proc_edge_t));
     if (!edge) {
          error_print("failed ws_new_mimo_source_edge calloc of edge");
          return 0;
     }
     edge->dst = dst;
     if (port) {
          edge->port = wsregister_label(&mimo->datalists, port);
     }
     if (src_label) {
          edge->src_label = wsregister_label(&mimo->datalists, src_label);
     }
     edge->next = src->edges;
     src->edges = edge;
     queue_add(mimo->edges, edge);
     return 1;
}

//TODO:  UNFINISHED FUNCTION 
static int ws_new_mimo_sink_edge(mimo_t * mimo, ws_proc_instance_t * src,
                          mimo_sink_t * dst, const char * src_label) {

     if (!src) {
          return 1;
     }
     //add edge to edge list
     if (!mimo->edges) {
          mimo->edges = queue_init();
     }
     ws_proc_edge_t * edge = 
          (ws_proc_edge_t *)calloc(1, sizeof(ws_proc_edge_t));
     if (!edge) {
          error_print("failed ws_new_mimo_sink_edge calloc of edge");
          return 0;
     }
     edge->sink = dst;
     if (src_label) {
          edge->src_label = wsregister_label(&mimo->datalists, src_label);
     }
     edge->next = src->edges;
     src->edges = edge;
     queue_add(mimo->edges, edge);
     return 1;
}


static void ws_new_edge(mimo_t * mimo, ws_proc_instance_t * src,
                 ws_proc_instance_t * dst, const char * port, const char * src_label,
                 int thread_trans, int thread_context, 
                 int twoD_placement) {

     if (!src) {
          return;
     }
     //add edge to edge list
     if (!mimo->edges) {
          mimo->edges = queue_init();
     }
     ws_proc_edge_t * edge = 
          (ws_proc_edge_t *)calloc(1, sizeof(ws_proc_edge_t));
     if (!edge) {
          error_print("failed ws_new_edge calloc of edge");
          return;
     }
     queue_add(mimo->edges, edge);
     edge->dst = dst;
     if (port) {
          edge->port = wsregister_label(&mimo->datalists, port);
     }
     if (src_label) {
          edge->src_label = wsregister_label(&mimo->datalists, src_label);
     }

     // Transfer thread info from edge to dst
     EDGE_TRANS(mimo, src, edge, dst, thread_trans, thread_context, twoD_placement);

//fprintf(stderr,"ws_new_edge:  dst->thread_id %d src->thread_id %d thread_trans %d\n",dst->thread_id,src->thread_id,thread_trans);

     edge->next = src->edges;
     src->edges = edge;
     return;
}

static int add_uniq_sub(ws_subscriber_t ** list, ws_subscriber_t * sub) {

     ws_subscriber_t * cursor;

     for (cursor = *list; cursor; cursor = cursor->next) {
          dprint("ii %x %x", cursor->input_index, sub->input_index);
          if ((cursor->proc_func == sub->proc_func) &&
              (cursor->input_index == sub->input_index) &&
              (cursor->port == sub->port) &&
              (cursor->src_label == sub->src_label) &&
              (cursor->proc_instance == sub->proc_instance) &&
              (cursor->local_instance == sub->local_instance) ) {
               return 0;
          }
          else {
               dprint("%p %p", cursor->port, sub->port);
               dprint("ii %x %x", cursor->input_index, sub->input_index);
               dprint("%p %p", cursor->proc_instance, sub->proc_instance);
          }
     }
     return 1;
}

static int ws_init_instance_input(mimo_t * mimo, ws_outtype_t * ocursor, 
                           wslabel_t * port, wslabel_t * src_label,
                           ws_proc_instance_t * dst, ws_proc_instance_t * src,
                           uint8_t flushmon) {

     ws_subscriber_t * sub;
     ws_proctype_t * ptcursor;
     proc_process_t proc_func;
     int rtn = 0;
     int ptfound = 0;

     if (src_label && (ocursor->label != src_label)) {
          if (ocursor->label) {
               if (mimo->verbose) {
                    status_print("mismatch resolve label %s with %s", 
                                 src_label->name,
                                 ocursor->label ? ocursor->label->name: "null");
               }
          }
     }

     //check existing input types..
     for (ptcursor = dst->input_list.head; ptcursor; ptcursor = ptcursor->next) {
          if ((ptcursor->dtype == ocursor->dtype) && (ptcursor->port == port)) {
               ptfound = 1;
               dprint("input exist added sub %s, %s", dst->name, 
                      ocursor->dtype->name);
               dprint("ptcursor input index %u", ptcursor->input_index);
               sub = (ws_subscriber_t*)ws_init_calloc(dst->thread_id, 1, 
                                                      sizeof(ws_subscriber_t));
               if (!sub) {
                    error_print("failed ws_init_instance_input calloc of sub");
                    return 0;
               }
               sub->proc_func = ptcursor->proc_func;
               sub->input_index = ptcursor->input_index;
               sub->port = port;
               sub->src_label = src_label;
               sub->proc_instance = dst;
               if(sub->src_label) {
                    sub->proc_instance->srclabel_name = sub->src_label->name;
               }
               sub->local_instance = dst->instance;
               sub->doutput = &dst->doutput;
               sub->thread_id = dst->thread_id;

               //attach subscriber...
               if(!src || (dst->thread_id == src->thread_id)) {
//fprintf(stderr, "rank %d SUB->PI->NAME '%s' ADDED TO LOCAL\n", nrank, sub->proc_instance->name);
                    if (add_uniq_sub(&ocursor->local_subscribers, sub)) {
                         queue_add(mimo->subs, sub);
                         sub->next = ocursor->local_subscribers;
                         ocursor->local_subscribers = sub;
                    }
                    else {
                         dprint("duplicate sub");
                         ws_free(sub, sizeof(ws_subscriber_t));
                    }
               }
               else {
//fprintf(stderr, "rank %d SUB->PI->NAME '%s' ADDED TO EXTERNAL\n", nrank, sub->proc_instance->name);
                    if (add_uniq_sub(&ocursor->ext_subscribers, sub)) {
                         queue_add(mimo->subs, sub);
                         sub->next = ocursor->ext_subscribers;
                         ocursor->ext_subscribers = sub;

                         if(!REGISTER_SHQ_WRITER(src->thread_id, sub->thread_id)) {
                              return 0;
                         }
                    }
                    else {
                         dprint("duplicate sub");
                         ws_free(sub, sizeof(ws_subscriber_t));
                    }
               }
               rtn = -1;
               break;
          }

     }

     if (!ptfound) {
          //save the proc_name in case we need it to enroll a stringhash table
          //there is now at least one kid that does this in proc_input_set
          save_proc_name(dst->name);

          proc_func = dst->module->proc_input_set_f(dst->instance,
                                                    ocursor->dtype,
                                                    port,
                                                    &dst->output_type_list,
                                                    dst->input_index,
                                                    &mimo->datalists);
          
//fprintf(stderr,"proc_input_set_f returned proc_func %p dst->name %s ocursor->dtype %p\n",proc_func,dst->name,ocursor->dtype);
          if (!proc_func) {
               if (mimo->input_validate && !dst->input_valid) {
                    ws_source_list_t * cursor;

                    //check if this kid is a source - no input
                    for (cursor = mimo->proc_sources; cursor; cursor = cursor->next) {
                         if (cursor->pinstance == dst) {
                              dst->input_valid = 1; // a source has no input so its valid
                              break;
                         }
                    }
               }
          }
          
          //this kid has valid input...
          else {
               rtn = 1;

               //...unless we are handling a flush or monitor type at this point
               if (mimo->input_validate && 0 == flushmon) {
                    dst->input_valid = 1;
               }

               dprint("new input added sub %s, %s", dst->name, 
                      ocursor->dtype->name);
               sub = (ws_subscriber_t*)ws_init_calloc(dst->thread_id, 1, 
                                                      sizeof(ws_subscriber_t));
               if (!sub) {
                    error_print("failed ws_init_instance_input calloc of sub");
                    return 0;
               }
               queue_add(mimo->subs, sub);
               sub->proc_func = proc_func;
               sub->input_index = dst->input_index;
               sub->port = port;
               sub->src_label = src_label;
               sub->proc_instance = dst;
               if(sub->src_label) {
                    sub->proc_instance->srclabel_name = sub->src_label->name;
               }
               sub->local_instance = dst->instance;
               sub->doutput = &dst->doutput;
               sub->thread_id = dst->thread_id;

               //attach subscriber..
               if(!src || (dst->thread_id == src->thread_id))
               { //local subscriber
//fprintf(stderr, "rank %d (2) SUB->PI->NAME '%s' ADDED TO LOCAL\n", nrank, sub->proc_instance->name);
                    sub->next = ocursor->local_subscribers;
                    ocursor->local_subscribers = sub;
               }
               else { //external subscriber
//fprintf(stderr, "rank %d (2) SUB->PI->NAME '%s' ADDED TO EXTERNAL\n", nrank, sub->proc_instance->name);
                    sub->next = ocursor->ext_subscribers;
                    ocursor->ext_subscribers = sub;

                    if(!REGISTER_SHQ_WRITER(src->thread_id, sub->thread_id)) {
                         return 0;
                    }
               }

               //attach input type.
               ws_proctype_t * pt =
                    (ws_proctype_t*)ws_init_calloc(dst->thread_id, 1, 
                                                   sizeof(ws_proctype_t));
               if (!pt) {
                    error_print("failed ws_init_instance_input calloc of pt");
                    return 0;
               }
               queue_add(mimo->subs, pt);
               pt->proc_func = proc_func;
               pt->dtype = ocursor->dtype;
               pt->port = port;
               pt->input_index = dst->input_index;
               dprint(" in ptcursor input index %u", pt->input_index);
               pt->next = dst->input_list.head;
               dst->input_list.head = pt;
               dst->input_index++;
          }
     }

     return rtn;
}

static void ws_init_flush(mimo_t * mimo, ws_proc_instance_t * inst,
                                 ws_proc_instance_t * src_inst) {

     if (!inst->flush_register) {
          inst->flush_register = 1;
          if (ws_init_instance_input(mimo, &mimo->flush.outtype_flush,
                                     NULL, NULL, inst, src_inst, 1) != 0) { 

               if (mimo->verbose) {
                    status_print("registered flusher on %s", inst->name);
               }
               if (mimo->flush.cnt < MAX_FLUSH_SUBS) {
                    if ( (!src_inst && inst) || (src_inst->thread_id == inst->thread_id) ) {
                         mimo->flush.sub[mimo->flush.cnt] =
                              mimo->flush.outtype_flush.local_subscribers;
                    }
                    else {
                         mimo->flush.sub[mimo->flush.cnt] =
                              mimo->flush.outtype_flush.ext_subscribers;
                    }
                    mimo->flush.cnt++;
               }
          }
          else {
               if (mimo->verbose) {
                    status_print("did not register flusher on %s", inst->name);
               }
          }
     }
     else {
          // XXX: this is the situation where we've arrive at an already-visited kid in a 
          //      breath-first search of a graph, so we desire to 'correct' the flush order
          //      by pushing this kid to be the current last flushed since another kid is 
          //      likely to feed it data during flushes.  In other words, take two actions: 
          //      1. ensure that the kid (inst) has a flusher and 
          //      2. if the kid has a flusher, then move it to the last position (i.e., 
          //         move it to the index of (mimo->flush.cnt-1) in mimo->flush.sub array
          if(mimo->flush.cnt <= 1 || inst == mimo->flush.sub[mimo->flush.cnt-1]->proc_instance) {
               // we are where we should be, simply return
               return;
          }

          //fprintf(stderr, "kid '%s.%d' previously seen; check if flushing\n", inst->name, inst->version);
          int i, j;
          for(i=0; i < mimo->flush.cnt; i++) {
               if(inst == mimo->flush.sub[i]->proc_instance) {
                    if (mimo->verbose) {
                         status_print("changing registered flush order: moving '%s.%d' to the end", 
                                 inst->name, inst->version);
                    }
                    break;
               }
          }

          if(i == mimo->flush.cnt) {
               // though kid has already been visited, it has no flusher...
               return;
          }

          // TODO: check to see if it is enough to simply put the kid in context at the end position
          //       or should we also keep track of the series of subsequent subscribers the kid has
          //       (complicated).  What about what to do if we had cycles in the graph???
          ws_subscriber_t * cursub = mimo->flush.sub[i];
          for(j=i; j < mimo->flush.cnt-1; j++) { 
               // slide all subscribers to the their left
               mimo->flush.sub[j] = mimo->flush.sub[j+1];
          }

          mimo->flush.sub[j] = cursub;
     }
}

static int ws_init_datatype_dst(mimo_t * mimo, ws_proc_instance_t * src, 
                         ws_outlist_t * olist, ws_proc_edge_t *edge) {

     q_node_t * qcursor;
     ws_outtype_t * ocursor;
     int rtn = 0;

     if (!olist->outtype_q) {
          dprint("null outq");
          return 0;
     }     
     for (qcursor = olist->outtype_q->head; qcursor; qcursor= qcursor->next) {
          ocursor= (ws_outtype_t*)qcursor->data;
          
          if (ws_init_instance_input(mimo, ocursor, edge->port,
                                        edge->src_label,
                                        edge->dst, src, 0) > 0) {
               rtn++;
          }
     }
     return rtn;
}

// may handle loops
int ws_init_from_instance(mimo_t * mimo, ws_proc_instance_t * src) {

     ws_proc_edge_t * edge;
     nhqueue_t * next_level = queue_init();
     if (!next_level) {
          error_print("failed queue_init of next_level");
          return 0;
     }
     ws_proc_instance_t * qpop;
     //ws_init_flush(mimo, src);
     ws_init_flush(mimo, src, NULL);
     for (edge = src->edges; edge; edge= edge->next) {
          //find out which data types are accepted..
          if (ws_init_datatype_dst(mimo, src, &src->output_type_list, edge)) {
               if (mimo->verbose) {
                    status_print("init edge %s.%d to %s.%d",
                                 src->name, src->version,
                                 edge->dst->name, edge->dst->version);
               }
               if (edge->src_label && mimo->verbose) {
                    status_print("  label %s", edge->src_label->name);
               }
               if (!queue_add(next_level, edge->dst)) {
                    error_print("failed queue_add alloc");
                    return 0;
               }
          }
          //ws_init_flush(mimo, edge->dst);
          ws_init_flush(mimo, edge->dst, src);

     }

     dprint("init from instance -- popping dst list");
     while ((qpop = queue_remove(next_level)) != NULL) {
          ws_init_from_instance(mimo, qpop);
     }
     queue_exit(next_level);

     return 1;
}

static int ws_init_from_monitors(mimo_t * mimo) {

     //init from proc_sources
     ws_source_list_t * mcursor;
     for (mcursor = mimo->proc_monitors; mcursor; mcursor = mcursor->next) {
          ws_proc_instance_t * cursor;
          for (cursor = mimo->proc_instance_head; cursor; cursor = cursor->next) {

               // cursor->pinstance is the src 'kid'
               if (ws_init_instance_input(mimo, &mcursor->outtype,
                                          NULL, NULL,
                                          cursor, mcursor->pinstance, 1) > 0) {
                    mimo->datalists.monitors++;
               }
          }

     }
     if (mimo->verbose) {
          status_print("number of kids that can be monitored %d", mimo->datalists.monitors);
     }

     return 1;
}

static int ws_init_from_sources(mimo_t * mimo) {

     ws_proc_edge_t * edge;
     nhqueue_t * next_level = queue_init();
     if (!next_level) {
          error_print("failed queue_init of next_level");
          return 0;
     }

     //init from mimo_sources
     mimo_source_t * mscursor;
     for (mscursor = mimo->sources; mscursor; mscursor = mscursor->next) {
          if (mimo->verbose) {
               status_print("found mimo source %s", mscursor->name);
          }
          /// check all edges of source.. find out who is subscribing
          for (edge = mscursor->edges; edge; edge = edge->next) {
               if (mimo->verbose) {
                    status_print("  found dst %s.%d", edge->dst->name,
                                 edge->dst->version);
               }

               if (edge->sink) {
                    error_print("not handling external sinks yet");
               }
               else if (edge->dst) {
                    if (ws_init_instance_input(mimo, &mscursor->outtype,
                                               edge->port, edge->src_label,
                                               edge->dst, NULL, 0) > 0) { // make it NULL here and assume that NULL is always local
                         if (!queue_add(next_level, edge->dst)) {
                              error_print("failed queue_add alloc");
                              return 0;
                         }
                    }
               }
          }

     }
     
     //init from proc_sources
     ws_source_list_t * cursor;
     for (cursor = mimo->proc_sources; cursor; cursor = cursor->next) {
          //ws_init_flush(mimo, cursor->pinstance);
          ws_init_flush(mimo, cursor->pinstance, NULL);
          if (mimo->verbose) {
               status_print("found source %s.%d", cursor->pinstance->name,
                            cursor->pinstance->version);
          }

          // mark this thread as a thread containing at least one source kid
          src_threads[cursor->pinstance->thread_id] = 1;

          /// check all edges of source.. find out who is subscribing
          for (edge = cursor->pinstance->edges; edge; edge= edge->next) {
               if (mimo->verbose) {
                    status_print("  found dst %s.%d", edge->dst->name,
                                 edge->dst->version);
               }

               if (edge->sink) {
                    error_print("not handling external sinks yet");
               }
               else if (edge->dst) {
                    // cursor->pinstance is the src 'kid'
                    if (ws_init_instance_input(mimo, &cursor->outtype,
                                               edge->port, edge->src_label,
                                               edge->dst, cursor->pinstance, 0) > 0) {
                         if (!queue_add(next_level, edge->dst)) {
                              error_print("failed queue_add alloc");
                              return 0;
                         }
                    }
               }
               ws_init_flush(mimo, edge->dst, cursor->pinstance);
          }
     }

     ws_proc_instance_t * qpop;
     dprint("init from sources -- popping dst list");
     while ((qpop = queue_remove(next_level)) != NULL) {
          ws_init_from_instance(mimo, qpop);
     }

     queue_exit(next_level);
     return 1;
}

static void ws_print_flush(mimo_t * mimo) {

     int i;

     for (i = 0; i < mimo->flush.cnt; i++) {
          fprintf(stderr, "flush order %d thread %d %s.%d\n",
                  i, mimo->flush.sub[i]->proc_instance->thread_id,
                  mimo->flush.sub[i]->proc_instance->name,
                  mimo->flush.sub[i]->proc_instance->version);
     }
}

int ws_init_proc_graph(mimo_t * mimo) {

     //walk instance list... assign modules
     const int nrank = GETRANK();
     ws_proc_instance_t * cursor;

     if (mimo->verbose) {
          fprintf(stderr, "processing proc graph\n");
     }

     WSPERF_BASE_TIME();

     // rebase all threads to their proper cpu (based on cpu_thread_mapper.cpu_for_thread)
     rebase_threads_to_cpu(mimo);

     if(0 == nrank) {
          src_threads = (uint32_t *)calloc(work_size, sizeof(uint32_t));
     }

     for (cursor = mimo->proc_instance_head; cursor; cursor = cursor->next) {

          BARRIER_WAIT(barrier1);

          // If this isn't on my thread, move on to the next proc
          if (cursor->thread_id != nrank) continue;

          // Set shared job queue pointers
#ifdef WS_PTHREADS
          cursor->doutput.shared_jobq_array = mimo->shared_jobq;
          cursor->doutput.mimo = mimo;
#endif // WS_PTHREADS

          //fprintf(stderr, " locating %s\n", cursor->name);
          //find module
          if (!cursor->module) {
               cursor->module = ws_proc_module_find(mimo, cursor->name);
               if (!cursor->module) {
                    fprintf(stderr,"module %s not found\n",
                            cursor->name);
                    return 0;
               }
               cursor->version = cursor->module->use_count;
               cursor->module->use_count++;
               if (!cursor->module) {
                    fprintf(stderr,"processing function %s not found\n",
                            cursor->name);
                    return 0;
               }
               
               //assign a unique id to each kid
               mimo->kid_uid++;
               cursor->kid.uid = mimo->kid_uid;
               char kidname [MAX_CHARS];
               sprintf(kidname,"%s.%d",cursor->name,cursor->version);
               if (!INIT_WSPERF_KID_NAME(cursor->kid.uid,kidname)) {
                    return 0;
               }

               //save the proc_name in case we need it to enroll a stringhash table
               save_proc_name(cursor->name);

               // init the module...
               if (mimo->verbose) {
                    fprintf(stderr,"initializing %s.%d, kid_uid %u, thread_id %u\n", 
                            cursor->name, cursor->version, cursor->kid.uid, 
                            cursor->thread_id);
               }
               optind = 1; // reset argument reader...
               //check if specialty kid
               if (cursor->module->pbkid) {
                    if (!wsprocbuffer_init(cursor->argc, cursor->argv,
                                           &cursor->instance, &mimo->datalists,
                                           cursor->module->pbkid)) {
                         error_print("wsprocbuffer_init() initializing kid: '%s'\n",
                                 cursor->name);
                         return 0;
                    }
               }
               else if (cursor->module->kskid) {
                    if (!wsprockeystate_init(cursor->argc, cursor->argv,
                                             &cursor->instance, &mimo->datalists,
                                             cursor->module->kskid)) {
                         error_print("wsprockeystate_init() initializing kid: '%s'\n",
                                 cursor->name);
                         return 0;
                    }
               }
               else {
                    ws_sourcev_t sourcev;
                    memset(&sourcev, 0, sizeof(ws_sourcev_t));
                    sourcev.mimo = mimo;
                    sourcev.proc_instance = cursor;
                    if (!cursor->module->proc_init_f(&cursor->kid, cursor->argc,
                                                     cursor->argv,
                                                     &cursor->instance,
                                                     &sourcev,
                                                     &mimo->datalists)) {
                         error_print("proc_init() initializing kid: '%s'\n",
                                 cursor->name);
                         return 0;
                    }
               }
          }
     } 

     // NOTE:  sync up here between preprocessing phases
     BARRIER_WAIT(barrier1);

     // Make a pass through all hash tables marked as shared, and demote to serial
     // any that belong to a single kid
     if ((0 == nrank) && !verify_shared_tables()) {
          error_print("failed verify_shared_tables()\n");
          return 0;
     }

     for (cursor = mimo->proc_instance_head; cursor; cursor = cursor->next) {

          // If this kid isn't on my thread, I won't do the init_finish
          if(cursor->thread_id != nrank) continue;

          // Only finish initializing non-special kids
          if (cursor->module->pbkid || cursor->module->kskid) {
               continue;
          }

          //save the proc_name in case we need it to enroll a stringhash table
          save_proc_name(cursor->name);

          if (NULL == cursor->module->proc_init_finish_f) continue;

          if (!cursor->module->proc_init_finish_f(cursor->instance)) {
               error_print("proc_init_finish() initializing kid: '%s'\n",
                       cursor->name);
               return 0;
          }
     }

     // Ensure final proc initialized before any source initialization occurs
     BARRIER_WAIT(barrier1);

     if(0 == nrank) {
          status_print("initializing sources");
          //start at sources... propagate edges
          if (!ws_init_from_sources(mimo)) {
               status_print("error: in ws_init_from_sources()");
               return 0;
          }
          if (mimo->verbose) {
               ws_print_flush(mimo);
          }
          ws_init_from_monitors(mimo);

          // compute the number of threads with sources
          int thread_index;
          for (thread_index = 0; thread_index < work_size; thread_index++) {
              num_src_threads += src_threads[thread_index];
          }
          dprint("source kids exist on %d threads", num_src_threads);
          free(src_threads);

          // All local/external subscriber lists have been assigned
          // so we know how many writers to external queues we have
          // at this point.  Shared queue writers are reported here and
          // queue types RESET to single-writer-single-reader as needed
          REPORT_SHQ_WRITERS(mimo);
     }

     // sync up here between preprocessing phases
     if (mimo->input_validate) {
          BARRIER_WAIT(barrier1);

          int graph_invalid = 0;
          for (cursor = mimo->proc_instance_head; cursor; cursor = cursor->next) {

               // If this kid isn't on my thread, I won't do the verify
               if(cursor->thread_id != nrank) continue;

               if (!cursor->input_valid) {
                    graph_invalid = 1;
                    error_print("no valid input to kid '%s.%d'", 
                                cursor->name, cursor->version);
               }
          }
          if (graph_invalid) {
               return 0;
          }
     }

     // Ensure all local/external subscriber lists have been assigned
     // before proceeding
     BARRIER_WAIT(barrier1);

     return 1;
}


static int collapse_edges(nhqueue_t *newedges, parse_edge_t * srcedge,
                          nhqueue_t * dstedges) {

     parse_edge_t * dstedge;
     parse_edge_t * newedge;
     q_node_t * qnode;
     
     for (qnode = dstedges->head; qnode; qnode = qnode->next) {
          dstedge = (parse_edge_t *)qnode->data;
          newedge = malloc(sizeof(parse_edge_t));
          if (!newedge) {
               error_print("failed collapse_edges malloc of newedge");
               return 0;
          }
          memcpy(newedge, dstedge, sizeof(parse_edge_t));
          newedge->src = srcedge->src;
          newedge->edgetype =
               (srcedge->edgetype & 0xF0) | (dstedge->edgetype & 0x0F);

          // pick up thread_trans from the destination, not the source
          newedge->thread_trans = dstedge->thread_trans;
          if (!queue_add(newedges, newedge)) {
               error_print("failed collapse_edges queue_add of newedge");
               return 0;
          }
     }
     return 1;
}

void collapse_parse_graph(parse_graph_t * pg) {

     parse_edge_t * edge;
     q_node_t * qnode, * qnode_next;
     wsstack_t * rnode = wsstack_init();
     parse_node_var_t * var;
     nhqueue_t * newedges = queue_init();
     //find list of vars..
     for (qnode = pg->edges->head; qnode; qnode = qnode->next) {
          edge = (parse_edge_t *)qnode->data;

          if ((edge->edgetype >> 4) == PARSE_NODE_TYPE_VAR) {
               var = (parse_node_var_t *)edge->src;
               if (!var->dst) {
                    var->dst = queue_init();
                    wsstack_add(rnode, var->dst);
               }
               queue_add(var->dst, edge);
          }
     }

     int move_edge;
     for (qnode = pg->edges->head; qnode; qnode = qnode->next) {
          edge = (parse_edge_t *)qnode->data;
          move_edge = 0;

          switch (edge->edgetype) {
          case PARSE_EDGE_TYPE_PROCPROC:
               queue_add(newedges, edge);
               move_edge = 1;
               break;
          case PARSE_EDGE_TYPE_PROCVAR:
               var = (parse_node_var_t *)edge->dst;
               if (var->dst) {
                    collapse_edges(newedges, edge, var->dst);
               }
               free(edge);
               move_edge = 1;
          case PARSE_EDGE_TYPE_VARPROC:
               //ignore
               break;
          }
          // if the edge moved, invalidate the old copy
          if (move_edge) {
               qnode->data = NULL;
          }
     }

     // delete old dst queues
     nhqueue_t * qqnode = (nhqueue_t *)wsstack_remove(rnode);
     while (qqnode) {
          queue_exit(qqnode);
          qqnode = (nhqueue_t *)wsstack_remove(rnode);
     }
     wsstack_destroy(rnode);

     // delete old edges
     qnode = pg->edges->head;
     while (qnode) {
          qnode_next = qnode->next;
          free(qnode);
          qnode = qnode_next;
     }
     free(pg->edges);

     // overwrite edge queue with the new one
     pg->edges = newedges;
}

static void local_init_parse_proc(void * vdata, void * vmimo) {

     mimo_t * mimo = (mimo_t*)vmimo;
     parse_node_proc_t * proc = (parse_node_proc_t*)vdata;
 
     if (!proc->mimo_source || !proc->mimo_sink) { 
          proc->pinst = ws_new_proc_instance(mimo, proc->name,
                                             proc->argc, proc->argv);

          SET_PINST_TID(mimo, proc);
     }
}

void local_free_parse_vars(void * vdata, void * vmimo) {

     parse_node_var_t * var = (parse_node_var_t*)vdata;

     if (var->name) {
          free((char*)var->name);
     }
 
}

void local_free_parse_proc(void * vdata, void * vmimo) {

     parse_node_proc_t * proc = (parse_node_proc_t*)vdata;
 
     int i;
     for (i = 0; i < proc->argc; i++) {
          if (proc->argv[i]) {
               free((char*)proc->argv[i]);
          }
     }
     if (proc->name) {
          free((char*)proc->name);
     }
     if (proc->argv) { 
          free(proc->argv);
     }
     if (proc->pinst) { 
          free(proc->pinst);
     }
}

//take in a config file and build proc_instance graph and list
int load_parsed_graph(mimo_t * mimo, parse_graph_t * pg) {

     parse_edge_t * edge;
     q_node_t * qnode;
     parse_node_proc_t * proc;
     parse_node_proc_t * dst;

     if (!ALLOC_THREADID_STUFF()) {
          return 0;
     }

     //walk all processors
     listhash_scour(pg->procs, local_init_parse_proc, mimo);

     if(NULL == pg->edges->head) {
          // then, we have a graph with no edge, i.e., a graph with only 
          // one kid so we'll set user_threadid_zero_exists as we will
          // never call ws_new_edge below
          user_threadid_zero_exists = 1;
     }

     //walk all edges form graph.. form proc_instance graph
     for (qnode = pg->edges->head; qnode; qnode = qnode->next) {
          edge = (parse_edge_t *)qnode->data;

          switch (edge->edgetype) {
          case PARSE_EDGE_TYPE_PROCPROC:
               proc = (parse_node_proc_t *)edge->src;
               dst = (parse_node_proc_t *)edge->dst;
               if (proc->mimo_source) {
                    if (!ws_new_mimo_source_edge(mimo,
                                mimo_lookup_source(mimo, proc->name),
                                dst->pinst,
                                edge->port,
                                edge->src_label)) {
                         return 0;
                    }
               }
               else if (dst->mimo_sink) {
                    if (!ws_new_mimo_sink_edge(mimo,
                                proc->pinst,          
                                mimo_lookup_sink(mimo, dst->name),
                                edge->src_label)) {
                         return 0;
                    }
               }
               else {
                    ws_new_edge(mimo, proc->pinst, dst->pinst,
                                edge->port,
                                edge->src_label,
                                edge->thread_trans,
                                edge->thread_context, edge->twoD_placement);
               }
               break;
          default:
               error_print("invalid edge to init");
          }
     }	

     // remove the redundant creation of additional thread
     REARRANGE_AND_REMOVE_INVALID_USERID(mimo);

     return 1;
}
