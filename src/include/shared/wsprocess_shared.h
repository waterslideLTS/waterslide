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

#ifndef _WSPROCESS_SHARED_H
#define _WSPROCESS_SHARED_H

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
#include "wsperf.h"
#include "shared/getrank.h"
#include "setup_exit.h"
#include "shared/barrier_init.h"
#include "shared/shared_queue.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

//Globals
#ifdef WS_PTHREADS
extern uint32_t  hop_to_next_thread_locext; // next thread to do local+external
extern uint32_t work_size, ready_to_exit;
extern uint32_t spinning_on_jobs;


static inline int ws_add_external_job_source(mimo_t * mimo, wsdata_t * data, ws_subscriber_t * sub) {
     ws_subscriber_t * scursor;
     for(scursor = sub; scursor; scursor = scursor->next) { //sub is the external subscriber(s)
          if (!scursor->src_label ||
               wsdata_check_label(data, scursor->src_label)) {

               // TODO; since mimo_emit_data_copy is currently not used, which in turn calls this
               //       function (ws_add_external_job_source), we currently don't worry about 
               //       potential deadlock from this code...implement if needed.
               while(!shared_queue_add(mimo->shared_jobq[scursor->thread_id], data, scursor)) {}
          }
     }
     return 1;
}

static inline int ws_do_external_jobs(mimo_t * mimo, shared_queue_t * jobq) {

#define MAX_EXTJOBS_LIMIT     (4)
     if (1 == work_size || 0 == jobq) {
          return 0;
     }
     //fprintf(stderr,"wsprocess: do_job\n");
     int cnt = 0;
     WSPERF_NRANK();
     WSPERF_LOCAL_INIT();

     //fprintf(stderr,"walking jobs\n");
     void * vdata;
     void * vsub;
     while (jobq->shared_queue_remove_nonblock(jobq, &vdata, &vsub))
     {
          cnt++;
          // call processor
          // each job is dequeued from the appropriate thread's jobq

          /*fprintf(stderr,"wsprocessor: doing job %s.%d\n",
                  job->instance->name, job->instance->version); */
          //walk list of subscribers
          wsdata_t * data = (wsdata_t*)vdata;
          ws_subscriber_t * sub  = (ws_subscriber_t*)vsub;

          dprint("running data %s thru %s", data->dtype->name, sub->proc_instance->name);

          WSPERF_TIME0(sub->proc_instance->kid.uid-1);
          sub->proc_func(sub->local_instance, data, sub->doutput, sub->input_index);

          WSPERF_PROC_COUNT(sub->proc_instance->kid.uid-1);
          WSPERF_TIME(sub->proc_instance->kid.uid-1);
          //fprintf(stderr,"wsprocess: do_job sub addjob\n");

          dprint("after proc_func");

          //remove reference to data
          wsdata_delete(data);

          if(cnt >= MAX_EXTJOBS_LIMIT) {
               //XXX: We need to do this after the wsdata_delete for
               //     correct decrementation of data reference
               break;
          }
     }

     return cnt;
}

// this only executes external jobs from invoking thread; this is typically
// used during flushing stage to prevent deadlock due to a filled external
// queue.
// returns number of external jobs serviced
static inline int ws_execute_only_external_jobs(mimo_t * mimo) {
     int jobs_cnt = 0;
     const int nrank = GETRANK();

     if(mimo->mgc) {
          ws_subscriber_t * scursor = NULL;
          wsdata_t * data = NULL;

          // attemp to empty out all failoverq before proceeding
          if(fqueue_remove(mimo->failoverq[nrank], (void**)&data, (void**)&scursor)) {
               if(!shared_queue_add(mimo->shared_jobq[scursor->proc_instance->thread_id], data, scursor)) {
                    // add data back to head NOT tail to preserve metadata order in queue
                    fqueue_add_front(mimo->failoverq[nrank], data, scursor);
               }
          }
     }

     //pop and handle external jobq until empty...
     //prevents potential deadlock from external queue overload
     jobs_cnt += ws_do_external_jobs(mimo, mimo->shared_jobq[nrank]);

     if (!jobs_cnt) {
          sched_yield();
     }

     return jobs_cnt;
}

// expects the globals - ready_to_exit, barrier1, hop_to_next_thread_locext,
// and work_size - to be defined in this scope by the time we get to this
// function
//
static inline void ws_empty_allqueues_synchronously(mimo_t * mimo) {

     int job_cnt = 0, 
         ithread = 0,
         toggle = 0; // prevents constantly adding or decrementing spinning_on_jobs
     const int nrank = GETRANK();

     while (ready_to_exit < work_size) {
          for(ithread = 0; ithread < work_size; ithread++) {

               toggle = 0;
               // These barriers are executed by thread 0 in mimo_flush_graph()
               barrier_wait(barrier1);
               if(0 == nrank) {
                    hop_to_next_thread_locext = 0;
                    // thread with nrank equal to ithread does not participate in controlling this 
                    // variable (hence the '-1')
                    spinning_on_jobs = work_size - 1;
               }
               barrier_wait(barrier1);

               if(ithread == nrank) {
                    if( (job_cnt = mimo_run_exiting_graph(mimo)) ) {
                         ready_to_exit = 0; // recheck for possible jobs
                    }
                    else {
                         ready_to_exit++; // get ready to indicate all queues are empty
                    }

                    hop_to_next_thread_locext = 1;
               }
               else {
                    while( 1 ) {

                         if( (0 != hop_to_next_thread_locext) && (0 == spinning_on_jobs) ) {
                              // it's time to try the next thread in line, if needed
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
               }
          }
          // ensure all threads see the same value of ready_to_exit before 
          // checking the while-loop to exit out
          barrier_wait(barrier1);
     }
}
#endif // WS_PTHREADS

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WSPROCESS_SHARED_H
