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

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include "waterslide.h"
#include "mimo.h"
#include "so_loader.h"
#include "shared/getrank.h"
#include "shared/barrier_init.h"
#include "wsperf.h"
#include "shared/lock_init.h"
#include "shared/ws_init_threading.h"
#include "shared/wsperf_global.h"
#include "shared/sq_perf.h"
#include "setup_exit.h"

// Globals
extern uint32_t do_exit;


void clean_exit(int sig) {

     status_print("exit signal %d called", sig);

     // Note:  we use pthreads mutex lock calls here; this is not
     //        guaranteed to work within a signal handler.  Risky, 
     //        but if we want to increment do_exit to force termination
     //        when, e.g., there have been 4 ctrl-C's, it seems like 
     //        we are forced into this.
     WS_MUTEX_LOCK(&exit_lock)
     do_exit++;
     WS_MUTEX_UNLOCK(&exit_lock)

     // do_exit > 0 breaks the run out of the main while processing loop.
     // do_exit >= 3 forces an exit (e.g. 3 ctrl-C's) when the run is hung.
     if (do_exit >= 3) {
          _exit(0);
     }
}

void setup_exit_signals(void) {

     // make waterslide behave nicely when signals come along

     // signal has been replaced by sigaction as recommended
     // (see signal man page).  Also, pthreads behavior is 
     // undefined in signal handlers, so it is now avoided
     // here and in the clean_exit function.
     struct sigaction * action;
     action = (struct sigaction *)calloc(1,sizeof(struct sigaction));
     action->sa_handler = clean_exit;

     sigaction(SIGTERM, action, NULL);      // kill -15
     sigaction(SIGINT,  action, NULL);      // kill -2
     sigaction(SIGQUIT, action, NULL);      // kill -3
     sigaction(SIGABRT, action, NULL);      // kill -6

     free(action);
}

int ws_cleanup(mimo_t * mimo) {

     const int nrank = GETRANK();

     // This barrier makes for coherent printing
     if (mimo->verbose) {
          fprintf(stderr,"REACHED LOOP-LIMIT BARRIER on %d\n",nrank);
     }
     BARRIER_WAIT(barrier1);

     // Summarize shared queue use before mimo_destroy() starts freeing them
     // (if activated internally by SQ_PERF)
     if (0 == nrank) {
          PRINT_SQ_PERF(mimo);
     }
     BARRIER_WAIT(barrier1);

     // grab and print expiration counters for the stringhash tables
     // and print the stringhash table registry summary
     if (!nrank && sht_perf) {
          get_sht_expire_cnt();
          get_sht_shared_expire_cnt();
          print_sht_registry(1, mimo->srand_seed);
     }

     // free hash table performance data
     if (0 == nrank) {
          free_sht_registry();
     }

     // print profiling summary (if activated internally by WSPERF)
     if (!PRINT_WSPERF(mimo)) {
          return 0;
     }
     BARRIER_WAIT(barrier1);

     if (0 == nrank && !PRINT_WSPERF_GLOBAL_SUMMARY(mimo)) {
          return 0;
     }

     if(mimo->graphviz_p_fp) {
          wsprint_graph_dot(mimo->parsed_graph,mimo->graphviz_p_fp);
     }

     // save verbose and valgrind_dbg flags for use after mimo is destroyed
     uint32_t verbose = mimo->verbose;
     uint32_t valgrind_dbg = mimo->valgrind_dbg;

     // ensure that all processes are done with shared hash tables before 
     // mimo_destroy()
     BARRIER_WAIT(barrier1);

     mimo_destroy(mimo);

     // ensure that all processes have finished their output
     BARRIER_WAIT(barrier1);

     // for cleanup purposes, want rank 0 to leave here last
     if (verbose) {
          int i;
          for (i = work_size-1; i >= 0; i--) {
               if (nrank == i) {
                    fprintf(stderr,"FINISHED FINAL BARRIER on %d\n",nrank);
                    break;
               }
          }
     }

     if (0 == nrank) {
          if (verbose) {
               fprintf(stderr,"waterslide destroying the locks\n");
          }
          LOCK_DESTROY();

          if (verbose) {
               fprintf(stderr,"waterslide freeing shared variables\n");
          }

          FREE_THREADID_STUFF();
          FREE_WSPERF();

          // free dlopen file handles...unless we are trying to run Valgrind
          // in which case we need the trace information for the datatypes
          // and proc modules
          if (!valgrind_dbg) {
               free_dlopen_file_handles();
          }

          if (verbose) {
               fprintf(stderr,"waterslide exiting...\n");
          }
     }

     // All done!  Use pthread_exit to clean up.
     // Thread 0 will exit in the main function.
#ifdef WS_PTHREADS
     if (nrank) {
          int status = 0;
          pthread_exit(&status);
     }
#endif // WS_PTHREADS

     return 1;
}
