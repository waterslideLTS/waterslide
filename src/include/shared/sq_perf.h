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

#ifndef _SQ_PERF_H
#define _SQ_PERF_H

#include "mimo.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

// Macros for noop SQ_PERF function
#if !defined(SQ_PERF) || !defined(WS_PTHREADS)
#define PRINT_SQ_PERF(mimo) 
#else // SQ_PERF && WS_PTHREADS
#define PRINT_SQ_PERF(mimo) print_sq_perf(mimo)

// Globals
extern uint32_t * num_shq_writers, ** shq_writers, work_size;
extern uint64_t * os, *osf;

static inline void print_sq_perf(mimo_t * mimo) {

     if (work_size == 1) return;

     uint32_t i;

     WS_MUTEX_LOCK(&endgame_lock);
     fprintf(stderr,"\n****************************************************\n");
     fprintf(stderr,"WS Shared Queue State Summary:\n\n");

     for (i = 0; i < work_size; i++) {
          if (!strncmp(mimo->shared_jobq[i]->type, "swsr", 4)) {
               fprintf(stderr," Thread %d %s queue inbound stores %"PRIu64" inbound stores failed %"PRIu64"\n", 
                       i, mimo->shared_jobq[i]->type, 
                       mimo->shared_jobq[i]->nstore, 
                       mimo->shared_jobq[i]->ncantstore);
               if (mimo->shared_jobq[i]->nstore > 0) {
                    fprintf(stderr,"               avg. queue length %10.3f\n", 
                            ((double)mimo->shared_jobq[i]->ntotlength)/
                            ((double)mimo->shared_jobq[i]->nstore + 
                             (double)mimo->shared_jobq[i]->ncantstore)); 
                    //fprintf(stderr," total queue length %d\n", mimo->shared_jobq[i]->ntotlength);
               }
               else {
                    fprintf(stderr,"\n");
               }
               fprintf(stderr,"               read idle %"PRIu64"\n", 
                       mimo->shared_jobq[i]->nidle);
          }
          else if (!strncmp(mimo->shared_jobq[i]->type, "mwsr", 4)) {
               uint32_t j = 0;
               fprintf(stderr," Thread %d %s queue %d inbound stores %"PRIu64" inbound stores failed %"PRIu64"\n", 
                       i, mimo->shared_jobq[i]->type, j, 
                       mimo->shared_jobq[i]->nstore, 
                       mimo->shared_jobq[i]->ncantstore);
               if (mimo->shared_jobq[i]->nstore > 0) {
                    fprintf(stderr,"               avg. queue length %10.3f\n", 
                            ((double)mimo->shared_jobq[i]->ntotlength)/
                            ((double)mimo->shared_jobq[i]->nstore + 
                             (double)mimo->shared_jobq[i]->ncantstore)); 
               }
               fprintf(stderr,"               read idle %"PRIu64"\n", 
                       mimo->shared_jobq[i]->nidle);
          }
          if (os[i] || osf[i]) {
               fprintf(stderr,"               outbound stores %"PRIu64" outbound stores failed %"PRIu64"\n", 
                       os[i], osf[i]);
          }
          if (mimo->shared_jobq[i]->nsched_yield_add || mimo->shared_jobq[i]->nsched_yield_rm) {
               fprintf(stderr,"               sched yield add %"PRIu64" sched yield rm %"PRIu64"\n", 
                       mimo->shared_jobq[i]->nsched_yield_add,
                       mimo->shared_jobq[i]->nsched_yield_rm); 
          }
     }
     fprintf(stderr,"\n****************************************************\n");
     WS_MUTEX_UNLOCK(&endgame_lock);
}
#endif // SQ_PERF && WS_PTHREADS

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _SQ_PERF_H
