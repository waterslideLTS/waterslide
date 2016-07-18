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

#ifndef _WSPERF_GLOBAL_H
#define _WSPERF_GLOBAL_H

#include <sys/time.h>
#include "waterslide.h"
#include "mimo.h"
#include "shared/getrank.h"
#include "insertion_sort.h"
#include "wsperf.h"
#include <time.h>

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

// Macros for noop WSPERF functions
#if !defined(WS_PERF) || !defined(WS_PTHREADS)
#define PRINT_WSPERF_GLOBAL_SUMMARY(mimo) 1
#else // WS_PERF && WS_PTHREADS
#define PRINT_WSPERF_GLOBAL_SUMMARY(mimo) print_wsperf_global_summary(mimo)

// Globals
extern uint64_t * wstot, * wsbase;
extern uint32_t num_kids, work_size;


// Executed by rank 0; prints the global summary results and frees memory
static inline int print_wsperf_global_summary(mimo_t * mimo) {

     uint64_t * kidcycle_tot, * cycle_tot;
     uint32_t * kidcount_tot, * flushcount_tot;
     double ** cycle_pct, * cycle_pct_tot;

     double finaltime, kidtime = 0.;
     int i, j;
     uint32_t * kidindx;
     double * kidtot;

     if (work_size == 1) return 1;

     kidindx = (uint32_t *)calloc(num_kids,sizeof(uint32_t));
     if (!kidindx) {
          error_print("failed print_wsperf_global_summary calloc of kidindx");
          return 0;
     }
     kidtot = (double *)calloc(num_kids,sizeof(double));
     if (!kidtot) {
          error_print("failed print_wsperf_global_summary calloc of kidtot");
          return 0;
     }

     cycle_tot = (uint64_t *)calloc(num_kids,sizeof(uint64_t));
     if (!cycle_tot) {
          error_print("failed print_wsperf_global_summary calloc of cycle_tot");
          return 0;
     }
     cycle_pct_tot = (double *)calloc(num_kids,sizeof(double));
     if (!cycle_pct_tot) {
          error_print("failed print_wsperf_global_summary calloc of cycle_pct_tot");
          return 0;
     }
     kidcycle_tot = (uint64_t *)calloc(num_kids,sizeof(uint64_t));
     if (!kidcycle_tot) {
          error_print("failed print_wsperf_global_summary calloc of kidcycle_tot");
          return 0;
     }
     kidcount_tot = (uint32_t *)calloc(num_kids,sizeof(uint32_t));
     if (!kidcount_tot) {
          error_print("failed print_wsperf_global_summary calloc of kidcount_tot");
          return 0;
     }
     flushcount_tot = (uint32_t *)calloc(num_kids,sizeof(uint32_t));
     if (!flushcount_tot) {
          error_print("failed print_wsperf_global_summary calloc of flushcount_tot");
          return 0;
     }
     cycle_pct = (double **)calloc(work_size, sizeof(double *));
     if (!cycle_pct) {
          error_print("failed print_wsperf_global_summary calloc of cycle_pct");
          return 0;
     }
     for (i = 0; i < work_size; i++) {
          cycle_pct[i] = (double *)calloc(num_kids, sizeof(double));
          if (!cycle_pct[i]) {
               error_print("failed print_wsperf_global_summary calloc of cycle_pct[i]");
               return 0;
          }
     }

     uint64_t wstotal = 0;
     uint64_t wsfinal = GET_CYCLE_COUNT();

     for (j = 0; j < work_size; j++) {

          // final ws timing - if print_wsperf was not executed
          if (!mimo->verbose) {
               wstotal += wsfinal - wsbase[j];
          }
          else {
               wstotal += wstot[j];
          }

          for (i = 0; i < num_kids; i++) {
               cycle_tot[j] += kidcycle[j][i];
          }
          for (i = 0; i < num_kids; i++) {
               cycle_pct[j][i] = ((double)kidcycle[j][i])/((double)cycle_tot[j]);
          }
     }

     for (j = 0; j < work_size; j++) {
          for (i = 0; i < num_kids; i++) {
               cycle_pct_tot[i] += cycle_pct[j][i];
               kidcycle_tot[i] += kidcycle[j][i];
               kidcount_tot[i] += kidcount[j][i];
               flushcount_tot[i] += flushcount[j][i];
          }
     }

     double dbl_ratio = ((double)1.)/((double)(work_size));
     double dbl_time_ratio = dbl_ratio/((double)hertz);
     for (i = 0; i < num_kids; i++) {
          kidtime += ((double)kidcycle_tot[i])*dbl_time_ratio;
          if (WSPERF_INTERVAL == 1) {
               kidtot[i] = ((double)kidcycle_tot[i])*dbl_time_ratio;
          }
          else {
               kidtot[i] = ((double)cycle_pct_tot[i])*dbl_ratio;
          }
          kidindx[i] = i;
     }

     // sort the kid cycle times
     InsertionSort(kidtot, kidindx, num_kids);

     fprintf(stderr,"\nWS Kid Global Averages for %d Threads:\n",work_size);
     if (WSPERF_INTERVAL == 1) {

          // print the sorted times
          for (i = num_kids-1; i > -1; i--) {
               uint32_t k = kidindx[i];
	       fprintf(stderr,"kid %4u is %-24s:  avg func calls = %12u, avg time = %10.3f\n", 
                       k+1, kidname[k], kidcount_tot[k]/(work_size), kidtot[i]);
          }
          fprintf(stderr,"\nWS Avg. Total Kid Time:   %10.3f\n", kidtime);

          finaltime = wstotal*dbl_time_ratio;
          fprintf(stderr,"\nWS Average Unaccounted Time: %10.3f\n", 
                  finaltime-kidtime);
          fprintf(stderr,"\nWS Average Total Time:       %10.3f\n\n", 
                  finaltime);
     }
     else {

          if (kidtime == 0.) {
               fprintf(stderr,"NOTE:  insufficient sampling to get individual kid runtime percentages;\n"); 
               fprintf(stderr,"       only functions calls are reported below\n"); 
               for (i = num_kids-1; i > -1; i--) {
                    uint32_t k = kidindx[i];
                    fprintf(stderr,"kid %4u is %-24s:  avg func calls   = %12u\n", 
                            k+1, kidname[k], kidcount_tot[k]/(work_size));
               }
          }
          else {

               // print the sorted sampled percentages
               for (i = num_kids-1; i > -1; i--) {
                    uint32_t k = kidindx[i];
                    fprintf(stderr,"kid %4u is %-24s:  avg func calls   = %12u, avg pct = %10.3f\n", 
                            k+1, kidname[k], kidcount_tot[k]/(work_size), kidtot[i]*100.);
               }
          }

          finaltime = wstotal*dbl_time_ratio;
          fprintf(stderr,"\nWS Average Total Time:       %10.3f\n\n", finaltime);
     }
     fflush(stderr);

     // Free locals
     free(kidindx);
     free(kidtot);
     free(cycle_tot);
     free(cycle_pct_tot);
     free(kidcycle_tot);
     free(kidcount_tot);
     free(flushcount_tot);
     for (i = 0; i < work_size; i++) {
          free(cycle_pct[i]);
     }
     free(cycle_pct);

     return 1;
}
#endif // WS_PERF && WS_PTHREADS

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WSPERF_GLOBAL_H
