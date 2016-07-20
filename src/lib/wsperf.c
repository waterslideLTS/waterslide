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

#include <sys/time.h>
#include <assert.h>
#include "waterslide.h"
#include "shared/getrank.h"
#include "shared/lock_init.h"
#include "wsperf.h"
#include "parse_graph.h"

#define DEFAULT_PORT "8080"
#define MAX_CHARS 25

// Globals
uint64_t ** kidcycle;
uint64_t ** kidcount, ** flushcount, * numcalls;
char ** kidname;
uint32_t num_kids  = 0, max_kids = 10;
extern uint32_t work_size;
mimo_t * global_mimo;
int update_interval;

uint64_t * wsbase, * wstot;
uint64_t hertz; // same for all threads


uint64_t get_cycle_count(void) {

     uint32_t i;
     struct timespec N;

#if defined(__FreeBSD__)
// unfortunately, CLOCK_PROCESS_CPUTIME_ID is not defined for FreeBSD, 
// so we explicitly define it to the best of our knowledge
#define CLOCK_PROCESS_CPUTIME_ID CLOCK_REALTIME_PRECISE
#endif // __FreeBSD__

#ifdef WS_PTHREADS
     i = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &N);
#else // !WS_PTHREADS
     i = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &N);
#endif // WS_PTHREADS
     if (i) {
          fprintf(stderr,"ERROR: get_cycle_count failed\n");
          fprintf(stderr,"Turn off HASWSPERF=1 at build time to avoid this!\n");
          exit(-111);
     }
     uint64_t j = N.tv_sec*NANOTICKS + N.tv_nsec;

     return j;
}

int init_wsperf(void) {

     uint32_t i;

     // assert that WSPERF_INTERVAL is a positive integer
     assert(WSPERF_INTERVAL >= 1 && (WSPERF_INTERVAL - (int)WSPERF_INTERVAL) == 0);

     kidname = (char **)calloc(max_kids, sizeof(char *));
     if (!kidname) {
          error_print("failed init_wsperf calloc of kidname");
          return 0;
     }
     for (i = 0; i < max_kids; i++) {
          kidname[i] = (char *)calloc(MAX_CHARS, sizeof(char));
          if (!kidname[i]) {
               error_print("failed init_wsperf calloc of kidname[i]");
               return 0;
          }
     }

     numcalls = (uint64_t *)calloc(max_kids, sizeof(uint64_t));
     if (!numcalls) {
          error_print("failed init_wsperf calloc of numcalls");
          return 0;
     }

     kidcycle = (uint64_t **)calloc(work_size, sizeof(uint64_t *));
     if (!kidcycle) {
          error_print("failed init_wsperf calloc of kidcycle");
          return 0;
     }
     kidcount = (uint64_t **)calloc(work_size, sizeof(uint64_t *));
     if (!kidcount) {
          error_print("failed init_wsperf calloc of kidcount");
          return 0;
     }
     flushcount = (uint64_t **)calloc(work_size, sizeof(uint64_t *));
     if (!flushcount) {
          error_print("failed init_wsperf calloc of flushcount");
          return 0;
     }
     for (i = 0; i < work_size; i++) {
          kidcycle[i] = (uint64_t *)calloc(max_kids, sizeof(uint64_t));
          if (!kidcycle[i]) {
               error_print("failed init_wsperf calloc of kidcycle[i]");
               return 0;
          }
          kidcount[i] = (uint64_t *)calloc(max_kids, sizeof(uint64_t));
          if (!kidcount[i]) {
               error_print("failed init_wsperf calloc of kidcount[i]");
               return 0;
          }
          flushcount[i] = (uint64_t *)calloc(max_kids, sizeof(uint64_t));
          if (!flushcount[i]) {
               error_print("failed init_wsperf calloc of flushcount[i]");
               return 0;
          }
     }

     wsbase = (uint64_t *)calloc(work_size, sizeof(uint64_t));
     if (!wsbase) {
          error_print("failed init_wsperf calloc of wsbase");
          return 0;
     }

     wstot = (uint64_t *)calloc(work_size, sizeof(uint64_t));
     if (!wstot) {
          error_print("failed init_wsperf calloc of wstot");
          return 0;
     }

     return 1;
}

void free_wsperf(void) {

     uint32_t i;

     for (i = 0; i < max_kids; i++) {
          free(kidname[i]);
     }
     free(kidname);
     free(numcalls);
     for (i = 0; i < work_size; i++) {
          free(kidcycle[i]);
          free(kidcount[i]);
          free(flushcount[i]);
     }
     free(kidcycle);
     free(kidcount);
     free(flushcount);
     free(wsbase);
     free(wstot);
}

int init_wsperf_kid_name(uint32_t uid, char *name) {

     uint32_t i, cpsz = sizeof(*name);

     num_kids  = uid;

     // NOTE:  memory added by realloc is uninitialized by the system, so we
     //        need to initialize it

     // Expand kidname if necessary
     if (uid >= max_kids) {
          kidname = (char **)realloc(kidname, (max_kids+10)*sizeof(char *));
          if (!kidname) {
               error_print("failed init_wsperf_kid_name realloc of kidname");
               return 0;
          }
          for (i = max_kids; i < max_kids+10; i++) {
               kidname[i] = (char *)calloc(MAX_CHARS, sizeof(char));
               if (!kidname[i]) {
                    error_print("failed init_wsperf_kid_name calloc of kidname[i]");
                    return 0;
               }
          }

          numcalls = (uint64_t *)realloc(numcalls, (max_kids+10)*sizeof(uint64_t));
          if (!numcalls) {
               error_print("failed init_wsperf_kid_name realloc of numcalls");
               return 0;
          }
          for (i = max_kids; i < max_kids+10; i++) {
               numcalls[i] = 0;
          }

          // Also expand kidcycle, kidcount and flushcount
          for (i = 0; i < work_size; i++) {
               kidcycle[i] = (uint64_t *)realloc(kidcycle[i], 
                                                    (max_kids+10)*sizeof(uint64_t));
               if (!kidcycle[i]) {
                    error_print("failed init_wsperf_kid_name realloc of kidcycle[i]");
                    return 0;
               }
               memset(&kidcycle[i][max_kids], 0, 10*sizeof(uint64_t));
               kidcount[i] = (uint64_t *)realloc(kidcount[i], 
                                                    (max_kids+10)*sizeof(uint64_t));
               if (!kidcount[i]) {
                    error_print("failed init_wsperf_kid_name realloc of kidcount[i]");
                    return 0;
               }
               memset(&kidcount[i][max_kids], 0, 10*sizeof(uint64_t));
               flushcount[i] = (uint64_t *)realloc(flushcount[i], 
                                                    (max_kids+10)*sizeof(uint64_t));
               if (!flushcount[i]) {
                    error_print("failed init_wsperf_kid_name realloc of flushcount[i]");
                    return 0;
               }
               memset(&flushcount[i][max_kids], 0, 10*sizeof(uint64_t));
          }
          max_kids += 10;
     }

     if (cpsz <= MAX_CHARS) {
          strcpy(kidname[uid-1], name);
     } else {
          strncpy(kidname[uid-1], name, MAX_CHARS);
     }

     return 1;
}

int print_wsperf(mimo_t * mimo) {

     double finaltime, kidtime = 0., time0;
     uint64_t wstotal;
     int i;
     uint32_t * kidindx;
     double * kidtot;
     double max_pct = 0.0;
     int max_num_calls = 0;

     kidindx = (uint32_t *)calloc(num_kids, sizeof(uint32_t));
     if (!kidindx) {
          error_print("failed print_wsperf calloc of kidindx");
          return 0;
     }
     kidtot = (double *)calloc(num_kids, sizeof(double));
     if (!kidtot) {
          error_print("failed print_wsperf calloc of kidtot");
          return 0;
     }

     const int nrank = GETRANK();
     uint64_t wsbase_loc = wsbase[nrank];

     // set up the hertz rate
     hertz = NANOTICKS; // nanosecond timer

// final ws timing
     wstotal = GET_CYCLE_COUNT() - wsbase_loc;

     wstot[nrank] = wstotal;

     WS_MUTEX_LOCK(&endgame_lock);
     fprintf(stderr,"\nWS Kid Profile (Sorted) for Rank %d:\n", nrank);

     for (i = 0; i < num_kids; i++) {
          time0 = ((double)kidcycle[nrank][i])/((double)hertz);
          kidtime += time0;
          kidtot[i] = time0;
          kidindx[i] = i;
     }
     // sort the kid run times
     InsertionSort(kidtot, kidindx, num_kids);
     mimo->parsed_graph->total_calls = 0;

#if (WSPERF_INTERVAL == 1) 
     // print the sorted times 
     for (i = num_kids-1; i > -1; i--) {
          uint32_t j = kidindx[i];
          if (kidcount[nrank][j] > 0 || flushcount[nrank][j]) {
               fprintf(stderr,"kid %4u is %-24s:  proc_func calls  = %12lu, time = %10.3f,\n",
                       j+1, kidname[j], kidcount[nrank][j], kidtot[i]);
               if (flushcount[nrank][j]) {
                    fprintf(stderr,"                                       proc_flush calls = %12lu\n", 
                            flushcount[nrank][j]);
               }
          }
          char * end = kidname[j];
          for(;*end;++end) {}
          int kidname_len = end - kidname[j];
          parse_node_proc_t * proc = listhash_find(mimo->parsed_graph->procs,kidname[j],kidname_len);
          proc->num_calls = kidcount[nrank][j];
          if(proc->num_calls > max_num_calls) { max_num_calls = proc->num_calls; } 
          mimo->parsed_graph->total_calls += proc->num_calls;
          proc->time = kidtot[i];
          proc->rank = num_kids-i;
     }
     finaltime = ((double)wstotal)/((double)hertz);
     fprintf(stderr,"\nWS Total Kid Time:   %10.3f\n", kidtime);
     fprintf(stderr,"\nWS Unaccounted Time: %10.3f\n", finaltime-kidtime);

#else // (WSPERF_INTERVAL != 1)
     // print the sorted percentages
     for (i = num_kids-1; i > -1; i--) {
          uint32_t j = kidindx[i];
          if (kidcount[nrank][j] > 0 || flushcount[nrank][j]) {
               fprintf(stderr,"kid %4u is %-24s:  proc_func calls  = %12"PRIu64", pct = %10.3f\n", 
                       j+1, kidname[j], kidcount[nrank][j], kidtot[i]/kidtime*100.);
               if (flushcount[nrank][j]) {
                    fprintf(stderr,"                                       proc_flush calls = %12"PRIu64"\n", 
                            flushcount[nrank][j]);
               }
          }
          char * end = kidname[j];
          for(;*end;++end) {}
          int kidname_len = end - kidname[j];
          parse_node_proc_t * proc = listhash_find(mimo->parsed_graph->procs,kidname[j],kidname_len);
          proc->num_calls = kidcount[nrank][j];
          if( proc->num_calls > max_num_calls) { max_num_calls = proc->num_calls; }
          mimo->parsed_graph->total_calls += proc->num_calls;
          proc->time = kidtot[i];
          proc->rank = num_kids-i;
          proc->pct = kidtot[i]/kidtime*100.;
          if (proc->pct > max_pct) { max_pct = proc->pct; }
     }
     mimo->parsed_graph->max_pct = max_pct;
     mimo->parsed_graph->max_num_calls = max_num_calls;
     finaltime = ((double)wstotal)/((double)hertz);
#endif // (WSPERF_INTERVAL == 1)
     fprintf(stderr,"\nWS Total Time:               %10.3f\n\n", finaltime);

     WS_MUTEX_UNLOCK(&endgame_lock);

     free(kidindx);
     free(kidtot);

     return 1;
}

