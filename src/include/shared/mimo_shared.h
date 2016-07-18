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

#ifndef _MIMO_SHARED_H
#define _MIMO_SHARED_H

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "waterslide_io.h"
#include "shared/shared_queue.h"
#include "mimo.h"
#include "shared/getrank.h"
#include "shared/barrier_init.h"
#include "shared/setup_startup.h"
#include "shared/wsprocess_shared.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

// Macros for noop serial functions
#ifndef WS_PTHREADS
#define MIMO_CREATE_THREADS(mimo) 1
#define MIMO_INIT_SHAREDQ(mimo) 1
#else // WS_PTHREADS
#define MIMO_CREATE_THREADS(mimo) mimo_create_threads(mimo)
#define MIMO_INIT_SHAREDQ(mimo) mimo_init_sharedq(mimo)

// Globals
extern uint32_t do_exit;
extern uint32_t ready_to_flush, ready_to_exit;
extern uint32_t spinning_on_jobs;
extern uint32_t flushes_aborted;
extern uint32_t work_size, num_src_threads;
extern pthread_mutexattr_t mutex_attr;
WS_MUTEX_EXTERN(exit_lock);
extern mimo_work_order_t ** arglist;
extern pthread_t ** thread;
extern __thread int thread_rank;
extern pthread_barrier_t *barrier1;


static inline int mimo_init_sharedq(mimo_t * mimo) {

     int i;

     mimo->shared_jobq = (shared_queue_t **)calloc(work_size, sizeof(shared_queue_t *));
     if (!mimo->shared_jobq) {
          error_print("failed mimo_init_sharedq calloc of mimo->shared_jobq");
          return 0;
     }

     for(i = 0; i < work_size; i++) {
          mimo->shared_jobq[i] = shared_queue_init();
          if (!mimo->shared_jobq[i]) {
               error_print("failed mimo_init_sharedq queue_init of mimo->shared_jobq[i]");
               return 0;
          }
     }

     //XXX: probably not the most logical place for this allocation of memory, but this'll do
     mimo->thread_in_cycle = (uint8_t *)calloc(work_size, sizeof(uint8_t));
     if(!mimo->thread_in_cycle) {
          error_print("failed mimo_init_sharedq calloc of mimo->thread_in_cycle");
          return 0;
     }

     WS_MUTEX_INIT(&mimo->lock, mutex_attr);

     return 1;
}

// only need to init failover q if at least one cycle exist in mimo
static inline int mimo_init_failoverq(mimo_t * mimo) {

     int i;

     if(mimo->tg->num_scc) {
          mimo->failoverq = (failoverqueue_t **)calloc(work_size, sizeof(failoverqueue_t *));
          if (!mimo->failoverq) {
               error_print("failed mimo_init_failoverq calloc of mimo->failoverq");
               return 0;
          }

          for(i = 0; i < work_size; i++) {
               mimo->failoverq[i] = fqueue_init();
               if(!mimo->failoverq[i]) {
                    error_print("failed mimo_init_failoverq queue_init of mimo->failoverq[i]");
                    return 0;
               }
          }
     }

     return 1;
}

static inline int mimo_thread_in_cycle(mimo_t * mimo, int tid) {
     return mimo->thread_in_cycle[tid];
}

static inline int mimo_cycle_deadlock_exist(mimo_t * mimo) {
     // XXX: should be mostly correct and sufficient...need synchronization mechanism to guarantee correctness
     int i, j, ret = 0;
     for(i=0; i < mimo->tg->num_scc; i++) {
          ret = 1; // assume that a cycle exist on an entry unless proven otherwise
          for(j = 0; j < mimo->mgc[i].nthreads; j++) {
               if(0 == mimo->mgc[i].fullshq[ mimo->mgc[i].threadlist[j] ]) {
                    ret = 0;
                    break;
               }
          }
          if(ret) {
               return ret;
          }
     }

     return ret;
}

static inline void * mimo_run_threaded_graph(mimo_work_order_t *arglist) {

     mimo_t * mimo;
     int nrank;

     thread_rank = nrank = arglist->rank;
     mimo = arglist->mimo;

     barrier_wait(barrier1);
     
     if (!mimo_compile_graph_internal(mimo) ) {
          error_print("unable to compile graph");
          exit(0);
     }

     // Synchronize all processes:
     // On short runs, we get a race-ahead condition here, which allowed the 
     // first-arrival to create, use, and destroy a shared hash table, before 
     // other nodes are done with it.
     if (mimo->verbose) {
          fprintf(stderr,"REACHED EXECUTE-GRAPH BARRIER on %d\n",nrank);
     }

     // setup spinning_on_jobs for use in avoiding deadlock that could arise when
     // a kid spits out several metadata that are aimed at the shared queues of 
     // other threads (shared_queues have fixed size and use blocking writes)
     if(0 == nrank) {
          // assumes that work_size has been set ... in this case, it should have
          // been done by mimo_compile_graph in the waterslide-parallel.c code prior
          // to calling this function, mimo_run_threaded_graph
          spinning_on_jobs = work_size;
     }
     barrier_wait(barrier1);

     if (mimo->verbose) {
          fprintf(stderr,"starting to execute graph on %d\n",nrank);
     }

     // Loop until control-C.
     setup_exit_signals();
     while (!do_exit) {
          if (!mimo_run_graph(mimo)) {
               if (mimo->verbose) {
                    status_print("end of source streams");
               }
               // must use the same lock here as in setup_exit()
               WS_MUTEX_LOCK(&exit_lock);
               num_src_threads--; // remove this thread from count of source candidates
               if(0 == num_src_threads) {
                    if (!do_exit) {
                         do_exit = 1; // eventually, all threads will see this value
                    }
               }
               WS_MUTEX_UNLOCK(&exit_lock);
               break;
          }
     }

     // unfortunately, with 'signal' semantics, not all threads receive a
     // SIGXXXX (e.g. SIGINT) at the same time, thus we need an indicator of
     // when all threads have stopped their (possibly) respective sources
     WS_MUTEX_LOCK(&(mimo->lock));
     ready_to_flush++;
     WS_MUTEX_UNLOCK(&(mimo->lock));

     // execute jobs from external queue until all threads have
     // cutoff their proc_sources (source kids)
     if (work_size > 1) {
          do {
               if (ready_to_flush == work_size) {
                    break;
               }

               //job_cnt = ws_execute_only_external_jobs(mimo);
               ws_execute_only_external_jobs(mimo);

          } while (1);
     }

     // ensure that no thread hangs because it cannot enqueue/write metadata in
     // the shared (external) queue of a thread who may be in a barrier_wait
     // in the function "ws_empty_allqueues_synchronously"
     int toggle = 0, job_cnt = 0;
     while( 1 ) {
          if( 0 == spinning_on_jobs ) {
               // all threads have successfully pushed out their metadata without
               // being blocked out because of limited size in shared queues
               break;
          }

          job_cnt = ws_execute_only_external_jobs(mimo);
          if(0 == job_cnt) {
               if(0 == toggle) {
                    WS_MUTEX_LOCK(&(mimo->lock));
                    spinning_on_jobs--;
                    WS_MUTEX_UNLOCK(&(mimo->lock));
                    toggle = 1;
               }
          }
          else {
               if(1 == toggle) {
                    WS_MUTEX_LOCK(&(mimo->lock));
                    spinning_on_jobs++;
                    WS_MUTEX_UNLOCK(&(mimo->lock));
                    toggle = 0;
               }
          }
     }

     // Now, walk through local+external jobs of threads, one at a
     // time.  In this process, other threads must continue to spin
     // on their external queues to avoid deadlock.  We repeat this
     // process until no jobs are queue'd.
     ws_empty_allqueues_synchronously(mimo);
          
     // At this point, all queues are empty of jobs, but there could
     // still be jobs in the state tables of kids that will be flushed out
     if(0 == nrank) {
          // initialized to zero; when set, this indicates that the
          // flush routine did not execute as expected on a set of kids
          flushes_aborted = 0;

          // set all outputs to thread 0's local queue. we will henceforth
          // process the flushes as a serial process
          //
          // step 1. set all kid's output to go to thread 0's local jobq
          ws_proc_instance_t * cursor;
          ws_outtype_t * ocursor;
          q_node_t * qcursor;
          ws_subscriber_t * tailsub; 

          for (cursor = mimo->proc_instance_head; cursor; cursor = cursor->next) {
               cursor->doutput.local_jobq = mimo->jobq[nrank];
               cursor->doutput.local_job_freeq = mimo->jobq_freeq[nrank];
               // we are done with shared_jobq_array, so nullify them (seg fault will
               // be an obvious sign of some jobs not going to the local jobq[nrank]
               // as intended)
               cursor->doutput.shared_jobq_array = NULL;

               if(!cursor->output_type_list.outtype_q) continue;

               for (qcursor = cursor->output_type_list.outtype_q->head; qcursor;
                         qcursor = qcursor->next) {
                    ocursor = (ws_outtype_t*)qcursor->data;

                    if(ocursor->ext_subscribers) {
                         // add these external subscribers to the tail of the local subscribers
                         if(ocursor->local_subscribers) {
                              tailsub = ocursor->local_subscribers; 
                              while(tailsub->next) {
                                   // continue until we get to the tail subscriber
                                   tailsub = tailsub->next;
                              }

                              // sub is at the tail of local_subscribers
                              tailsub->next = ocursor->ext_subscribers;
                         }
                         else {
                              ocursor->local_subscribers = ocursor->ext_subscribers;
                         }

                         ocursor->ext_subscribers = NULL;
                    }
               }
          }

          // step 2. begin the actual flush
          if (!mimo_flush_graph(mimo)) {
               // the flush has gone wrong (e.g., bad wsd_flush alloc)
               flushes_aborted = 1;
          }
     }

     // Sync up before checking flush status
     barrier_wait(barrier1);

     if(flushes_aborted) {
          // the flush was aborted
          exit(0);
     }

     // Free memory, destroy barriers, locks, etc., as necessary.
     if (!ws_cleanup(mimo)) {
          exit(0);
     }

     return NULL;
}

static inline int mimo_create_threads(mimo_t * mimo) {

     // Initialize the barrier, now that we know work_size.
     if (!barrier_init(&barrier1, work_size)) {
          error_print("unable to init barrier1");
          return 0;
     }

     // Create threads, have them initialize fields, and then conditionally
     // wait until the master signals them to get to work (mimo_run_graph).
     // This is necessary to allow us serialize the critical sections first.
     // Also, set up mimo for the threads in global memory array.
     size_t universal_stacksize = (1<<18); // + sizeof(mimo_t);

     if (mimo->verbose) {
          fprintf(stderr, "universal_stacksize = %" PRIu64 "\n", (uint64_t)universal_stacksize);
     }

     pthread_attr_t attr;

     pthread_attr_init(&attr);
     pthread_attr_setstacksize(&attr, universal_stacksize);

     // setup and create worker threads

     // main() thread will also execute mimo_threaded_run_graph, but 
     // outside this for-loop (in driver code)
     arglist[0]->rank = 0;
     arglist[0]->mimo = mimo;
     int i;
     for (i = 1; i < work_size; i++) {
          arglist[i]->rank = i;
          arglist[i]->mimo = mimo;
          pthread_create(thread[i], &attr, (void *(*)(void *))mimo_run_threaded_graph,
                         arglist[i]);
     }

     return 1;
}
#endif // WS_PTHREADS

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _MIMO_SHARED_H
