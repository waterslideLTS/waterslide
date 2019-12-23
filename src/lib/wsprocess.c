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
// main execution thread for pipeline.. polls all sources and runs to completion
//#define DEBUG 1
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sched.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "waterslide_io.h"
#include "mimo.h"
#include "wsprocess.h"
#include "init.h"
#include "shared/getrank.h"
#include "wsperf.h"
#include "setup_exit.h"
#include "shared/wsprocess_shared.h"
#include "shared/shared_queue.h"
#include "datatypes/wsdt_flush.h"

//Globals
uint32_t  hop_to_next_thread_locext; // next thread to do local+external
extern uint32_t work_size;
extern uint32_t deadlock_firehose_shutoff;


int ws_add_local_job_source(mimo_t * mimo, wsdata_t * data, ws_subscriber_t * sub) {
     ws_job_t * job;

     const int nrank = GETRANK();
     if ((job = queue_remove(mimo->jobq_freeq[nrank])) == NULL)
     {

          // okay with pthreads too!
          job = (ws_job_t*)malloc(sizeof(ws_job_t));
          if (!job) {
               error_print("failed ws_add_local_job_source malloc of job");
               return 0;
          }
     }
     job->data = data;
     job->subscribers = sub;
     if (!queue_add(mimo->jobq[nrank], job)) 
     {
          return 0;
     }
     return 1;
}

static int ws_do_local_jobs(mimo_t * mimo, nhqueue_t * jobq, nhqueue_t * jobq_freeq) {
     //fprintf(stderr,"wsprocess: do_job\n");
     ws_job_t * job;
     ws_subscriber_t * sub;
     int cnt = 0;
     WSPERF_NRANK();
     WSPERF_LOCAL_INIT();

     //fprintf(stderr,"walking jobs\n");
     while ((job = queue_remove(jobq)) !=NULL)
     {
          cnt++;
          // call processor
          // each job is dequeued from the appropriate thread's jobq

          /*fprintf(stderr,"wsprocessor: doing job %s.%d\n",
                  job->instance->name, job->instance->version); */
          //walk list of subscribers
          for (sub = job->subscribers; sub; sub = sub->next) {
               if ((!sub->src_label ||
                    wsdata_check_label(job->data, sub->src_label))) {
                    dprint("running data %s thru %s", job->data->dtype->name,
                                 sub->proc_instance->name);
                    //fprintf(stderr,"wsprocess: do_job sub\n");
                    WSPERF_TIME0(sub->proc_instance->kid.uid-1);
                    sub->proc_func(sub->local_instance, job->data,
                                       sub->doutput, sub->input_index);
                    WSPERF_PROC_COUNT(sub->proc_instance->kid.uid-1);
                    WSPERF_TIME(sub->proc_instance->kid.uid-1);
                    //fprintf(stderr,"wsprocess: do_job sub addjob\n");

                    dprint("after proc_func");
               }
          }

          //remove reference to data
          wsdata_delete(job->data);

          //if (!queue_add(mimo->jobq_freeq, job)) 
          if (!queue_add(jobq_freeq, job)) 
          {
               error_print("failed queue_add alloc");
               return 0;
          }
     }
     return cnt;
}

//return 0 if no sources had data..
int ws_execute_graph(mimo_t * mimo) {
     //process all mimo sources

     //poll all proc_sources.. 
     ws_source_list_t * cursor = NULL;
     wsdata_t * data = NULL;
     int src_out = 0;

     // needed to know who really should check for valid source
     int rank_has_valid_source = 0; 
     static int empty_src_cnt = 0;

     //see if mimo external source was added
     const int nrank = GETRANK();
     WSPERF_LOCAL_INIT();

#ifdef WS_PTHREADS
     ws_subscriber_t * scursor = NULL;
     int ext_out = 0;
     if(deadlock_firehose_shutoff) {
          if( !(ext_out = WS_DO_EXTERNAL_JOBS(mimo, mimo->shared_jobq[nrank])) ) {
               // try emptying all external jobs, then jobs in failover queue, before turning back on
               // the firehose (i.e., proc_sources or input source to be executed)
               int i, allclear;
               // attempt to empty out all failoverq before proceeding
               if(fqueue_remove(mimo->failoverq[nrank], (void**)&data, (void**)&scursor)) {
                    if(!shared_queue_add(mimo->shared_jobq[scursor->proc_instance->thread_id], data, scursor)) {
                         // add data back to head NOT tail to preserve metadata order in queue
                         fqueue_add_front(mimo->failoverq[nrank], data, scursor);
                    }

                    WS_MUTEX_LOCK(&(mimo->lock));
                    // check all failover queues and restore normal processing if all queues are empty
                    allclear = 1;
                    for(i = 0; i < work_size; i++) {
                         if(fqueue_size(mimo->failoverq[i]) != 0) {
                              allclear = 0;
                              break;
                         }
                    }
                    dprint("allclear = %d - %s\n", allclear, (allclear == 0 ? 
                           "leave input sources off (still recovering from deadlock)" : "re-enable input sources (deadlock gone)"));
                    if(allclear) {
                         deadlock_firehose_shutoff = 0;
                    }
                    WS_MUTEX_UNLOCK(&(mimo->lock));
               }
          }
          else {
               return ext_out;
          }
     }
#endif // WS_PTHREADS

     //fprintf(stderr,"wsprocess: walking sources\n");
     for (cursor = mimo->proc_sources; cursor; cursor = cursor->next) {
#ifdef WS_PTHREADS
          if(deadlock_firehose_shutoff) {
               // a deadlock-induced temporary shutoff of all input sources has occured
               if(!src_out) {
                    // fake that a metadata was emitted to avoid exiting the WS graph
                    src_out = 1;
               }
               break;
          }
          if (cursor->pinstance->thread_id != nrank) continue;
#endif // WS_PTHREADS
          if (cursor->proc_func) {
               //alloc a data type
               data = wsdata_alloc(cursor->outtype.dtype);
               if (!data) {
                    return 0;
               }
               wsdata_add_reference(data);
               //fprintf(stderr,"wsprocess: processing\n");
#ifdef WS_PTHREADS
               rank_has_valid_source = 1;
#endif // WS_PTHREADS
               WSPERF_TIME0(cursor->pinstance->kid.uid-1);
               if (cursor->proc_func(cursor->pinstance->instance, data,
                                     &cursor->pinstance->doutput,
                                     cursor->input_index)) {
                    src_out++;
               }
               WSPERF_PROC_COUNT(cursor->pinstance->kid.uid-1);
               WSPERF_TIME(cursor->pinstance->kid.uid-1);
               //fprintf(stderr,"wsprocess: dereferencing data\n");
               wsdata_delete(data);
          }
     }
     /****  removed unused code for monitoring
     for (cursor = mimo->proc_monitors; cursor; cursor = cursor->next) {
#ifdef WS_PTHREADS
          if (cursor->pinstance->thread_id != nrank) continue;
#endif // WS_PTHREADS
          if (cursor->proc_func) {
               //alloc a data type
               //fprintf(stderr,"wsprocess: processing\n");
               WSPERF_TIME0(cursor->pinstance->kid.uid-1);
               if (cursor->proc_func(cursor->pinstance->instance, NULL,
                                     &cursor->pinstance->doutput,
                                     mimo->datalists.monitors)) {
                    //something
               }
               WSPERF_PROC_COUNT(cursor->pinstance->kid.uid-1);
               WSPERF_TIME(cursor->pinstance->kid.uid-1);
          }
     }
     *****/



#ifdef WS_PTHREADS
     if (rank_has_valid_source && !src_out)
#else // !WS_PTHREADS
     if (!src_out)
#endif // WS_PTHREADS
     {
          dprint("ws_execute_graph: no more input on a source kid");
          return 0;
     }
     else {
          int jcnt = 0;

          //pop and handle local jobq until empty...
          jcnt += ws_do_local_jobs(mimo, mimo->jobq[nrank], mimo->jobq_freeq[nrank]);

          //pop and handle external (shared) jobq until empty...
          jcnt += WS_DO_EXTERNAL_JOBS(mimo, mimo->shared_jobq[nrank]);

          if (!jcnt) {
               if (empty_src_cnt < 100) {
                    empty_src_cnt++;
                    sched_yield();
               }
               else if (empty_src_cnt < 1000) {
                    empty_src_cnt++;
                    usleep(10);
               }
               else if (empty_src_cnt < 10000) {
                    empty_src_cnt++;
                    usleep(100);
               }
               else {
                    usleep(5000);
               }
               
          }
          else {
               //reset empty source counter
               empty_src_cnt = 0;
          }
     }

     if(0 == rank_has_valid_source) {
          // threads without sources will always run until
          // explicitly interrupted by user (e.g., Ctrl-C)
          // or stopped by threads with sources (when no 
          // more metadata exists to ingest into WS)
          return 1;
     }

     return src_out;
}

//turn off proc_sources and execute jobs out
//of local and external queue until there's no more jobs left in 
//queue...then return 0
int ws_execute_exiting_graph(mimo_t * mimo) {
     int jobs_cnt = 0;
     const int nrank = GETRANK();

#ifdef WS_PTHREADS
     if(mimo->mgc) {
          ws_subscriber_t * scursor = NULL;
          wsdata_t * data = NULL;

          // attempt to empty out all failoverq before proceeding
          if(fqueue_remove(mimo->failoverq[nrank], (void**)&data, (void**)&scursor)) {
               if(!shared_queue_add(mimo->shared_jobq[scursor->proc_instance->thread_id], data, scursor)) {
                    // add data back to head NOT tail to preserve metadata order in queue
                    fqueue_add_front(mimo->failoverq[nrank], data, scursor);
               }
          }
     }
#endif // WS_PTHREADS

     //pop and handle local jobq until empty...
     jobs_cnt += ws_do_local_jobs(mimo, mimo->jobq[nrank], mimo->jobq_freeq[nrank]);

     //pop and handle external (shared) jobq until empty...
     jobs_cnt += WS_DO_EXTERNAL_JOBS(mimo, mimo->shared_jobq[nrank]);

     if (!jobs_cnt) {
          sched_yield();
     }

     return jobs_cnt;
}

#define MAX_FLUSHES 0x7FFFFFFF

int mimo_flush_graph(mimo_t * mimo) {

     if (mimo->no_flush_on_exit) {
          return 1;
     }

     int jobs;
     uint32_t flushes;
     int i;
     const int nrank = GETRANK();
     WSPERF_LOCAL_INIT();

     wsdata_t * wsd_flush = wsdata_alloc(mimo->flush.outtype_flush.dtype);
     if (!wsd_flush) {
          return 0;
     }
     wsdt_flush_t * fl = (wsdt_flush_t*)wsd_flush->data; 
     fl->flag = WSDT_FLUSH_EXIT_MSG;
     wsdata_add_reference(wsd_flush);

     if(0 != nrank) {
          fprintf(stderr, "Arbitrary enforcement: (mimo_flush_graph) should be called by thread 0\n");
          return 0;
     }
     status_print("attempting to flush");

     for (i = 0; i < mimo->flush.cnt; i++) {
          ws_subscriber_t * sub = mimo->flush.sub[i];
          if (mimo->verbose) {
               status_print("flushing %s", sub->proc_instance->name);
          }
          flushes = 0;
          do {
               jobs = 0;
               flushes++;
               WSPERF_TIME0(sub->proc_instance->kid.uid-1);
               sub->proc_func(sub->local_instance, wsd_flush, sub->doutput,
                              sub->input_index);
               WSPERF_FLUSH_COUNT(sub->proc_instance->kid.uid-1);
               WSPERF_TIME(sub->proc_instance->kid.uid-1);
 
               //pop from jobq until empty...
               while(mimo->jobq[nrank]->size) {
                    jobs += ws_do_local_jobs(mimo, mimo->jobq[nrank], mimo->jobq_freeq[nrank]);
               }
          } while((jobs >= 1) && (flushes < MAX_FLUSHES));

          // we do have the possibility of metadata in previous flushers' proc's (e.g., proc_flush)
          if ((flushes > 1) && (flushes < MAX_FLUSHES)) {
               i = 0; // reset to start afresh
          }
     }

     wsdata_delete(wsd_flush);

     return 1;
}

// at least exit out of every module...
void ws_destroy_graph(mimo_t * mimo) {
     //walk instance list... destroy modules
     ws_proc_instance_t * cursor;
     const int nrank = GETRANK();

     //This is serialized so that output is not scrambled by multiple threads
     //that are simultaneously reporting their kids' statistics.
     WS_MUTEX_LOCK(&endgame_lock);
     if (work_size > 1) 
          fprintf(stderr,"\nWS Kid Summary Statistics for Rank %d:\n", nrank);

     for (cursor = mimo->proc_instance_head; cursor; cursor = cursor->next) {
          if (cursor->thread_id == nrank) {
               if (cursor->module && cursor->module->proc_destroy_f) {
                    cursor->module->proc_destroy_f(cursor->instance);
               }
          }
          if((0 == nrank) && cursor->output_type_list.outtype_q) {
               queue_exit(cursor->output_type_list.outtype_q);
          }
     }
     WS_MUTEX_UNLOCK(&endgame_lock);
}
