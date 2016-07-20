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
#ifndef _WS_INIT_THREADING_H
#define _WS_INIT_THREADING_H

#include <sched.h>
#include "parse_graph.h"
#include "shared/getrank.h"
#include "shared/barrier_init.h"
#include "shared/mimo_shared.h"

#ifdef USE_HWLOC
#include "shared/ws_select_cpus.h"
#endif

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

// Macros for noop serial functions
#ifndef WS_PTHREADS
#define ALLOC_THREADID_STUFF() (1)
#define FREE_THREADID_STUFF()
#define REGISTER_SHQ_WRITER(src_tid,sub_tid) 1
#define REPORT_SHQ_WRITERS(mimo)
#define REARRANGE_AND_REMOVE_INVALID_USERID(mimo)
#define SET_PINST_TID(mimo,proc)
#define EDGE_TRANS(mimo,src,edge,dst,thread_trans,thread_context,twoD_placement)
#else // WS_PTHREADS
#define ALLOC_THREADID_STUFF() alloc_threadid_stuff()
#define FREE_THREADID_STUFF() free_threadid_stuff()
#define REGISTER_SHQ_WRITER(src_tid,sub_tid) register_shq_writer(src_tid,sub_tid)
#define REPORT_SHQ_WRITERS(mimo) report_shq_writers(mimo)
#define REARRANGE_AND_REMOVE_INVALID_USERID(mimo) rearrange_and_remove_invalid_userid(mimo)
#define SET_PINST_TID(mimo,proc) set_pinst_tid(mimo,proc)
#define EDGE_TRANS(mimo,src,edge,dst,thread_trans,thread_context,twoD_placement) edge_trans(mimo,src,edge,dst,thread_trans,thread_context,twoD_placement)

/* This structure handles mappings in the following fashion:
 * array_length = How long the arrays are.  Approximately equal to # of threads in the system
 * cpu_for_thread[i]    OS thread ID (i) runs on OS CPU ID cpu_for_thread[i]
 * utid_for_thread[i]   OS thread ID (i) maps to user's specified thread ID utid_for_thread[i]
 */
typedef struct _cpu_thread_info_t_ {
     int32_t * cpu_for_thread;
     int32_t * utid_for_thread;
     uint32_t array_length;
} cpu_thread_info_t;

// Globals
extern cpu_thread_info_t cpu_thread_mapper; // will hold each thread's cpu id
extern int user_threadid_zero_exists;
extern uint32_t * num_shq_writers, ** shq_writers;
extern uint64_t * os, * osf;
extern uint32_t graph_has_cycle;


static inline int alloc_threadid_stuff(void) {

     // Assume 1 thread to start, will realloc as additional threads are identified
     cpu_thread_mapper.cpu_for_thread = (int32_t *)calloc(1, sizeof(int32_t));
     if (0 == cpu_thread_mapper.cpu_for_thread) {
          error_print("failed alloc_threadid_stuff calloc of cpu_for_thread");
          return 0;
     }
     cpu_thread_mapper.utid_for_thread = (int32_t *)calloc(1, sizeof(int32_t));
     if (0 == cpu_thread_mapper.utid_for_thread) {
          error_print("failed alloc_threadid_stuff calloc of utid_for_thread");
          return 0;
     }

     cpu_thread_mapper.array_length = 1;

     num_shq_writers = (uint32_t *)calloc(1, sizeof(uint32_t));
     if (0 == num_shq_writers) {
          error_print("failed alloc_threadid_stuff calloc of num_shq_writers");
          return 0;
     }
     shq_writers = (uint32_t **)calloc(1, sizeof(uint32_t *));
     if (0 == shq_writers) {
          error_print("failed alloc_threadid_stuff calloc of shq_writers");
          return 0;
     }
     shq_writers[0] = (uint32_t *)calloc(1, sizeof(uint32_t));
     if (0 == shq_writers[0]) {
          error_print("failed alloc_threadid_stuff calloc of shq_writers[0]");
          return 0;
     }
#ifdef SQ_PERF
     os = (uint64_t *)calloc(1, sizeof(uint64_t));
     if (0 == os) {
          error_print("failed alloc_threadid_stuff calloc of os");
          return 0;
     }
     osf = (uint64_t *)calloc(1, sizeof(uint64_t));
     if (0 == osf) {
          error_print("failed alloc_threadid_stuff calloc of osf");
          return 0;
     }
#endif // SQ_PERF

     return 1;
}

static inline void free_threadid_stuff(void) {

     int i;

     free(cpu_thread_mapper.cpu_for_thread);
     free(cpu_thread_mapper.utid_for_thread);
     free(num_shq_writers);
     for (i = 0; i < work_size; i++) {
          free(shq_writers[i]);
     }
     free(shq_writers);
#ifdef SQ_PERF
     free(os);
     free(osf);
#endif // SQ_PERF
}

static inline int realloc_threadid_stuff(void) {

     uint32_t cpulen = ++cpu_thread_mapper.array_length;

     cpu_thread_mapper.cpu_for_thread = 
          (int32_t *)realloc(cpu_thread_mapper.cpu_for_thread, 
          cpulen*sizeof(int32_t));

     if (NULL == cpu_thread_mapper.cpu_for_thread) {
          error_print("failed realloc_threadid_stuff realloc of cpu_for_thread");
          return 0;
     }

     cpu_thread_mapper.utid_for_thread =
          (int32_t *)realloc(cpu_thread_mapper.utid_for_thread,
               cpulen*sizeof(int32_t));

     if (NULL == cpu_thread_mapper.utid_for_thread) {
          error_print("failed realloc_threadid_stuff realloc of utid_for_thread");
          return 0;
     }

     // newly created entries in user_threadid to real_threadid mapper are
     // initialized to -1 to indicate invalid id's
     // initialize the new entry
     cpu_thread_mapper.cpu_for_thread[cpulen-1] = -1;
     cpu_thread_mapper.utid_for_thread[cpulen-1] = -1;

     num_shq_writers = 
          (uint32_t *)realloc(num_shq_writers,
          cpulen*sizeof(uint32_t));

     if (NULL == num_shq_writers) {
          error_print("failed realloc_threadid_stuff realloc of num_shq_writers");
          return 0;
     }

     // initialize the new entry
     num_shq_writers[cpulen-1] = 0;

     shq_writers = (uint32_t **)realloc(shq_writers, 
                   cpulen*sizeof(uint32_t *));
     if (NULL == shq_writers) {
          error_print("failed realloc_threadid_stuff realloc of shq_writers");
          return 0;
     }

     shq_writers[cpulen-1] = 
                 (uint32_t *)calloc(1, sizeof(uint32_t));
     if (0 == shq_writers[cpulen-1]) {
          error_print("failed realloc_threadid_stuff calloc of shq_writers[%d]", cpulen-1);
          return 0;
     }

#ifdef SQ_PERF
     os = (uint64_t *)realloc(os, cpulen*sizeof(uint64_t));
     if (NULL == os) {
          error_print("failed realloc_threadid_stuff realloc of os");
          return 0;
     }
     // initialize the new entry
     os[cpulen-1] = 0;

     osf = (uint64_t *)realloc(osf, cpulen*sizeof(uint64_t));
     if (NULL == osf) {
          error_print("failed realloc_threadid_stuff realloc of osf");
          return 0;
     }
     // initialize the new entry
     osf[cpulen-1] = 0;
#endif // SQ_PERF

     return 1;
}

static inline int realloc_shq_writer(uint32_t ** shq_writers, int i, int len) {

     shq_writers[i] = (uint32_t *)realloc(shq_writers[i], len*sizeof(uint32_t));
     if (NULL == shq_writers[i]) {
          error_print("failed realloc_shq_writer realloc of shq_writers[%d]", i);
          return 0;
     }

     // the new entry is set in the calling function
     return 1;
}

static inline int register_shq_writer(uint32_t src_tid, uint32_t sub_tid) {
     uint32_t i, reg = 0;

     // see if this writer has already been registered
     for (i = 0; i < num_shq_writers[sub_tid]; i++) {
          if (shq_writers[sub_tid][i] == src_tid) {
               reg = 1;
               break;
          }
     }

     // register that a writer exists for the destination's shared queue
     if (0 == reg) {
          num_shq_writers[sub_tid]++;
          if (num_shq_writers[sub_tid] > 1) {
               if (!realloc_shq_writer(shq_writers, sub_tid,
                                  num_shq_writers[sub_tid])) {
                    return 0;
               }
          }
          shq_writers[sub_tid][num_shq_writers[sub_tid]-1] = src_tid;
     }
     return 1;
}

static inline void report_shq_writers(mimo_t * mimo) {
     if (work_size > 1) {
          uint32_t dst, i, j;
          int index1, index2;
          for(dst = 0; dst < work_size; dst++) {
               if (mimo->verbose) {
                    fprintf(stderr, "Thread %d has %d writers into its external queue\n", 
                            dst, num_shq_writers[dst]);
               }
               if(num_shq_writers[dst] <= 1) {
                    reset_shq_type(mimo->shared_jobq[dst]);
               }

               // build directed graph for use in detecting the existence of waiting cycles, a pre-condition for processing
               // deadlock. The number of nodes = number of threads, while the edges = sharedqueue-provided links between
               // threads that'll be used in this graph
               //
               //fprintf(stderr, "Thread %d has the following external writers...\n", dst);
               for(i=0; i < num_shq_writers[dst]; i++) {
                    uint32_t *dstId = (uint32_t *)malloc(sizeof(uint32_t));
                    *dstId = dst; // really, just for ensuring heap memory content in queue (not function stack memory content!)
                    queue_add(mimo->tg->digraph[shq_writers[dst][i]], dstId);
                    //fprintf(stderr, " %d", shq_writers[dst][i]);
               }
               //fprintf(stderr, "\n");
          }

          // display information about connected components
          fprintf(stderr, "\n\n");
          compute_strongly_connected_components(mimo->tg);
          if(mimo->tg->num_scc) {
               fprintf(stderr, "SPECIFIED WS GRAPH HAS AT LEAST %d-CYCLE(S) IN THE GRAPH...ENABLING DEADLOCK RECOVERY\n",
                       mimo->tg->num_scc);
               // this WS graph will need to initialize failover queues for use when there is deadlock
               graph_has_cycle = 1; // processing graph has a cycle among external threads
               if(!mimo_init_failoverq(mimo)) {
                    exit(-111);
               }

               j = 0;
               mimo->mgc = (mimo_graph_cycle_t*)calloc(mimo->tg->num_scc, sizeof(mimo_graph_cycle_t));
               for(index1=queue_size(mimo->tg->scc_list)-1; index1 >= 0; index1--) {
                    nhqueue_t * list = (nhqueue_t *)queue_get_at(mimo->tg->scc_list, index1);
                    if(queue_size(list) > 1) {
                         mimo->mgc[j].nthreads = queue_size(list);
                         mimo->mgc[j].fullshq = (uint8_t *)calloc(work_size, sizeof(uint8_t));
                         mimo->mgc[j].threadlist = (int *)malloc(sizeof(int) * mimo->mgc[j].nthreads);
                         for(index2=mimo->mgc[j].nthreads-1; index2 >= 0; index2--) {
                              mimo->mgc[j].threadlist[index2] = *(int *)queue_get_at(list, index2);
                              mimo->thread_in_cycle[ mimo->mgc[j].threadlist[index2] ] = 1; // note: we could set this more than once, but it is okay
                         }
                         j++;
                    }
               }
          }
     }
}

// returns '0' if the thread_context has not been visited before
// else returns '1'
static inline int isvisited_utid(int thread_context) {
     int i;
     for(i=0; i < cpu_thread_mapper.array_length; i++) {
          if(cpu_thread_mapper.utid_for_thread[i] == thread_context) {
               return 1;
          }
     }
     return 0;
}

// returns the next unused user thread id
static inline int next_available_utid(void) {
     int i = 0;
     int max_utid_num = -1;
     for(i=0; i < cpu_thread_mapper.array_length; i++) {
          if(cpu_thread_mapper.utid_for_thread[i] > max_utid_num) {
               max_utid_num = cpu_thread_mapper.utid_for_thread[i];
          }
     }

     //obtain next available user thread id
     int j, found_i = 0;
     for(i=0; i < max_utid_num; i++) {
          // reset found_i
          found_i = 0;
          for(j=0; j < cpu_thread_mapper.array_length; j++) {
               if (i == cpu_thread_mapper.utid_for_thread[j]) {
                  found_i = 1;
                  break;
               }
          }

          if(0 == found_i) { // 'i' is unique and has not been used
               return i;
          }
     }

     for(i=0; i < cpu_thread_mapper.array_length; i++) {
         if(cpu_thread_mapper.utid_for_thread[i] == -1) {
              // an unused entry has been found
              break;
         }
     }

     if (i >= cpu_thread_mapper.array_length) {
          realloc_threadid_stuff();
     }

     cpu_thread_mapper.utid_for_thread[i] = max_utid_num + 1;

     return (max_utid_num + 1);
}

static inline void edge_trans(mimo_t * mimo, ws_proc_instance_t * src, 
                              ws_proc_edge_t * edge, ws_proc_instance_t * dst, 
                              int thread_trans, int thread_context, 
                              int twoD_placement) {

     // We need this to allocate subscribers as either local or external
     // in ws_init_instance_input. The thread_trans and thread_context
     // stuff will never be executed for non-pthreads inits, since the
     // thread_context and thread_trans values should be zero.
     edge->thread_trans = thread_trans;

     if (-1 != thread_context) { // we have a set thread_context, which could be 0 as well.
          if(0 == dst->tid_assigned) {
               dst->thread_id = thread_context;
          }

          if( !isvisited_utid(thread_context) ) {
               // we have a new thread context
               mimo->thread_id++;

               if (mimo->thread_id >= cpu_thread_mapper.array_length) {
                    realloc_threadid_stuff();
               }

               if(twoD_placement) { //assumes a Tile-like structure
                    error_print("2-D placement currently assumes a Tile(ra) architecture...defaulting to 1-D");
                    cpu_thread_mapper.utid_for_thread[mimo->thread_id] = thread_context;
               }
               else { // we have a 1-D prescription
                    cpu_thread_mapper.utid_for_thread[mimo->thread_id] = thread_context;
               }
          }
     }
     // if tid_assigned is already set, this is a secondary edge - do not create
     // another thread at this point
     else if (thread_trans) {
          if (0 == dst->tid_assigned) {
               dst->thread_id = next_available_utid();
               mimo->thread_id++;

               if (mimo->thread_id >= cpu_thread_mapper.array_length) {
                    realloc_threadid_stuff();
               }

               cpu_thread_mapper.utid_for_thread[mimo->thread_id] = dst->thread_id;
          }
     }
     else {
          dst->thread_id = src->thread_id;
     }

     // we indicate that we have visited this kid and assigned it a thread id
     // this is important when making decisions in thread_context section of 
     // ws_new_edge
     dst->tid_assigned = 1; 

     if(0 == src->thread_id || 0 == dst->thread_id) {
          // user thread id of zero exists
          user_threadid_zero_exists = 1;
     }

     if(cpu_thread_mapper.utid_for_thread[mimo->thread_id] == -1) { // not previously set
          cpu_thread_mapper.utid_for_thread[mimo->thread_id] = dst->thread_id;
     }
}

static inline uint32_t get_thread_realid(int user_threadid) {
     int i = 0;
     uint32_t retval = 0; // default all unmapped user_threadid to real_threadid of '0'
     for(i=0; i < cpu_thread_mapper.array_length; i++) {
          if ( cpu_thread_mapper.utid_for_thread[i] == user_threadid ) {
               retval = i;
               break;
          }
     }

     return retval;
}

// just for debugging purposes...prints out the assigned values in user-real id mapper
static inline void display_assigned_utr_values(void) {
     int i;
     for(i=0; i < cpu_thread_mapper.array_length; i++) {
          if(cpu_thread_mapper.cpu_for_thread[i] != -1) {
               fprintf(stderr, "user_tid %d maps to real_tid %d on core %d\n",
                       cpu_thread_mapper.utid_for_thread[i],
                       i, cpu_thread_mapper.cpu_for_thread[i]);
          }
     }
}

// ofsset using "-T #" option in waterslide-parallel
// this remaps cpus using the offset specified
static inline void offset_cpus_and_uid_assignments(const uint32_t nThreads, int offset) {
     uint32_t i = 0;
#ifdef USE_HWLOC
     if ( offset != -1 ) {
          status_print("User selected to enforce configuration thread mappings, overriding hwloc choices.\n");
     } else {
          HWInfo_t info;
          int ok = getHardwareInfo(&info);
          if ( ok ) {
               uint32_t *map = (uint32_t*)malloc(sizeof(uint32_t)*nThreads);
               uint32_t *mapp = map;
               //ok = getFreeCores(&info, 95.0f, nThreads, (uint32_t*)cpu_thread_mapper.cpu_for_thread);
               ok = getFreeCores(&info, 95.0f, nThreads, map);
               for(i=0; i < cpu_thread_mapper.array_length; i++) {
                    if(cpu_thread_mapper.utid_for_thread[i] != -1) {
                         cpu_thread_mapper.cpu_for_thread[i] = *mapp++;
                         dprint("Mapped OS Thread %d to user thread %d and core %d\n",
                                   i, cpu_thread_mapper.utid_for_thread[i], cpu_thread_mapper.cpu_for_thread[i]);
                    }
               }
               free(map);
               freeHardwareInfo(&info);
               if ( ok ) return;
          }
          offset = 0;
          fprintf(stderr, "WARNING:  Failed to intelligently map threads to cores.  Falling back to legacy strategy!\n");
     }
#endif
     const int max_cpus = (int)sysconf(_SC_NPROCESSORS_ONLN);
     for(i=0; i < cpu_thread_mapper.array_length; i++) {
          if(cpu_thread_mapper.utid_for_thread[i] != -1) {
               cpu_thread_mapper.cpu_for_thread[i] = (cpu_thread_mapper.utid_for_thread[i] + offset) % max_cpus;
          }
     }
}

// this is really only used when we need to remove userid = 0 from the list of valid utr's
// as parsing the entire graph indicated that it was not used
static inline void rearrange_and_remove_invalid_userid(mimo_t * mimo) {
     if(0 == user_threadid_zero_exists) {
          mimo->thread_id--;
          int i;
          for(i = 0; i < cpu_thread_mapper.array_length-1; i++) {
               cpu_thread_mapper.utid_for_thread[i] = cpu_thread_mapper.utid_for_thread[i+1];
          }
          // reset and invalidate unused (bogus) entry
          cpu_thread_mapper.utid_for_thread[i] = -1;
     }
}

static inline void set_cpu_for_thread(uint32_t mimotid, uint32_t thread_context)
{
     if(mimotid <= 1) {
          // a single value is already in a ascending order
          cpu_thread_mapper.utid_for_thread[mimotid] = thread_context;
          return;
     }

     if(thread_context > cpu_thread_mapper.utid_for_thread[mimotid-1]) {
          // just append to the end to put in ascending order
          cpu_thread_mapper.utid_for_thread[mimotid] = thread_context;
          return;
     }

     int i;
     for(i = 0; i < mimotid; i++) {
          if(thread_context < cpu_thread_mapper.utid_for_thread[i]) break;
     }

     // move contents of utid_for_thread one step ahead
     int j;
     for(j = mimotid; j > i; j--) {
          cpu_thread_mapper.utid_for_thread[j] = cpu_thread_mapper.utid_for_thread[j-1];
     }

     // assign thread_context to the correct thread_id
     cpu_thread_mapper.utid_for_thread[i] = thread_context;
}

static inline void set_pinst_tid(mimo_t * mimo, parse_node_proc_t * proc) {
     if (-1 != proc->thread_context) {
          proc->pinst->thread_id = proc->thread_context;

          if (!isvisited_utid(proc->thread_context)) {
               mimo->thread_id++;
               realloc_threadid_stuff();
               set_cpu_for_thread(mimo->thread_id, proc->thread_context);
          }

          proc->pinst->tid_assigned = 1;
     }
}

static inline void ws_recast_proc_ids(mimo_t * mimo) {
     ws_proc_instance_t * cursor;

     for (cursor = mimo->proc_instance_head; cursor; cursor = cursor->next) {
          cursor->thread_id = get_thread_realid(cursor->thread_id);
          cursor->doutput.local_jobq = mimo->jobq[cursor->thread_id];
          cursor->doutput.local_job_freeq = mimo->jobq_freeq[cursor->thread_id];
     }

}

// pins threads to their respective cpus.
static inline void rebase_threads_to_cpu(mimo_t * mimo) {

     const int offset_value = mimo->thread_global_offset;
     const int nrank = GETRANK();

     if(0 == nrank) {
          offset_cpus_and_uid_assignments(mimo->thread_id+1, offset_value);
          display_assigned_utr_values();
     }
     // need to sync up to ensure that no thread speeds off before nrank0 
     // gets a chance to set the global cpu_thread_mapper.utid_for_thread[] values
     BARRIER_WAIT(barrier1);

     int max_cpus = (int)sysconf(_SC_NPROCESSORS_ONLN);
     int my_id = cpu_thread_mapper.cpu_for_thread[nrank]; // id of thread

     cpu_set_t thread_cpu;
     CPU_ZERO(&thread_cpu);

     CPU_SET(my_id, &thread_cpu);
     if(pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &thread_cpu))
     {
          error_print("pthread_setaffinity_np failed");
          error_print("my_id = %d, NUM_CPUS = %d", my_id, max_cpus);
          exit(-111);
     }

     status_print("Thread %d (User thread %d) set to run on CPU: %d",
               nrank, cpu_thread_mapper.utid_for_thread[nrank], my_id);

     if(0 == nrank) {
          ws_recast_proc_ids(mimo);
     }
     BARRIER_WAIT(barrier1);
}
#endif // WS_PTHREADS

#ifndef WS_PTHREADS
static inline void rebase_threads_to_cpu(mimo_t * mimo) { 
     // need to set the local_jobq and local_job_freeq correctly
     // to avoid memory leaks!
     //
     // ws_recast_proc_ids(mimo);
     ws_proc_instance_t * cursor;

     for (cursor = mimo->proc_instance_head; cursor; cursor = cursor->next) {
          cursor->doutput.local_jobq = mimo->jobq[0];
          cursor->doutput.local_job_freeq = mimo->jobq_freeq[0];
     }
}
#endif // WS_PTHREADS

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WS_INIT_THREADING_H
