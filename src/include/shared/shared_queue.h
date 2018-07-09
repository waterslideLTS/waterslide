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

#ifndef _SHARED_QUEUE_H
#define _SHARED_QUEUE_H

#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#if !(defined(__FreeBSD__) || defined(__APPLE__))
#include <malloc.h>
#endif
#include <assert.h>
#include "shared/getrank.h"
#include "error_print.h"

#define MAX_SQUEUE_LEN	(16) // these number of metadata pointers should be enough
                             // for occasional spikes by proc_kids writing to ext. queue
#define SHQ_ADD_ATTEMPT_LIMIT (1000)

#define VERIFY_POSIX_MEMALIGN_SUCCESS(x) { if (0 != (x)) {  \
               fprintf(stderr, "ERROR! posix_memalign failed in %s:%d...", __FILE__, __LINE__); \
                              if(EINVAL == (x)) { \
                                   fprintf(stderr, "alignment field is not a power of two or not a multiple of sizeof(void*)."); \
                              } \
                              else if(ENOMEM == (x)) { \
                                   fprintf(stderr, "available memory is insufficient."); \
                              } \
                              fprintf(stderr, "\n"); \
                              exit(-111); \
               } \
         }

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#ifdef WS_PTHREADS

#define USE_SCHEDYIELD

#ifdef USE_SCHEDYIELD
#define SCHED_YIELD() sched_yield()
#else
#define SCHED_YIELD()
#endif // USE_SCHEDYIELD

#ifdef SQ_PERF

#ifdef USE_ATOMICS
#define INCR_STORE() {__sync_fetch_and_add(&q->nstore,1); __sync_fetch_and_add(&os[nrank],1);}
#define INCR_CANT_STORE() {__sync_fetch_and_add(&q->ncantstore,1); __sync_fetch_and_add(&osf[nrank],1);}
#define INCR_IDLE() {__sync_fetch_and_add(&q->nidle,1);}

#define INCR_TOTAL_LENGTH() {__sync_fetch_and_add(&q->ntotlength,q->length);}
#define INCR_LENGTH() {__sync_fetch_and_add(&q->length,1);}
#define DECR_LENGTH() {__sync_fetch_and_add(&q->length,-1);}
#define QTYPE_SWSR() strcpy(q->type,"swsr");
#define QTYPE_MWSR() strcpy(q->type,"mwsr");
#define SQPERF_NRANK() const int nrank = GETRANK();
#ifdef USE_SCHEDYIELD
#define INCR_SCHED_YIELD_ADD() q->nsched_yield_add++;
#define INCR_SCHED_YIELD_RM() q->nsched_yield_rm++;
#else
#define INCR_SCHED_YIELD_ADD()
#define INCR_SCHED_YIELD_RM()
#endif // USE_SCHEDYIELD

#else //!USE_ATOMICS
#define INCR_STORE() q->nstore++; os[nrank]++;
#define INCR_CANT_STORE() q->ncantstore++; osf[nrank]++;
#define INCR_IDLE() q->nidle++;
#define INCR_TOTAL_LENGTH() q->ntotlength+=q->length;
#define INCR_LENGTH() q->length++;
#define DECR_LENGTH() q->length--;
#define QTYPE_SWSR() strcpy(q->type,"swsr");
#define QTYPE_MWSR() strcpy(q->type,"mwsr");
#define SQPERF_NRANK() const int nrank = GETRANK();
#ifdef USE_SCHEDYIELD
#define INCR_SCHED_YIELD_ADD() q->nsched_yield_add++;
#define INCR_SCHED_YIELD_RM() q->nsched_yield_rm++;
#else
#define INCR_SCHED_YIELD_ADD()
#define INCR_SCHED_YIELD_RM()
#endif // USE_SCHEDYIELD
#endif // USE_ATOMICS

#else // !SQ_PERF

#define INCR_STORE()
#define INCR_CANT_STORE()
#define INCR_IDLE()
#define INCR_TOTAL_LENGTH()
#define INCR_LENGTH()
#define DECR_LENGTH()
#define QTYPE_SWSR()
#define QTYPE_MWSR()
#define SQPERF_NRANK()
#define INCR_SCHED_YIELD_ADD()
#define INCR_SCHED_YIELD_RM()

#endif // SQ_PERF

// Globals
extern uint64_t * os, * osf;

// macros used in atomic single-writer single-reader queues
#define COMPILER_FENCE() do { asm volatile ("":::"memory"); } while (0)
#define MACHINE_FENCE() do { __sync_synchronize(); } while (0)


#include "wsfree_list.h"

#ifdef USE_ATOMICS
typedef struct _shared_queue_data_t {
     wsfree_list_node_t node;
     void *buffer1;
     void *buffer2;
     struct _shared_queue_data_t *next;
} shared_queue_data_t;

typedef struct _shared_queue_t_ {
     // beginning of swmr stuff
     shared_queue_data_t *elements;
     uint32_t head;
     uint32_t tail;
     // end of swmr stuff

     //define function pointers
     int (*shared_queue_add_nonblock)(void* /* the queue*/, void*, void*);
     int (*shared_queue_remove_nonblock)(void* /* the queue*/, void*, void*);

     // beginning of mwmr stuff
     volatile long length;
     shared_queue_data_t *work_queue_head;
     shared_queue_data_t *work_queue_tail;
     wsfree_list_t *free_list;
     // end of mwmr stuff
#ifdef SQ_PERF
     uint64_t nstore, ncantstore, nidle, dequeue;
     uint64_t nsched_yield_add, nsched_yield_rm;
     int64_t ntotlength;
     char type[5];
#endif // SQ_PERF
     int max_length;
} shared_queue_t;

static inline void* shared_queue_data_alloc(void *arg)
{
     void *dptr = calloc(1, sizeof(shared_queue_data_t));
     if (!dptr) {
          error_print("shared_queue_data_alloc failed:  out of memory in calloc of dptr");
          return NULL;
     }

     return dptr;
}

static inline shared_queue_data_t * shared_queue_swap(shared_queue_data_t *volatile *addr,
                                                      shared_queue_data_t *newval)
{
    shared_queue_data_t *oldval = NULL;

    do {
         oldval = *addr;
    } while (!__sync_bool_compare_and_swap(addr, oldval, newval));
    return oldval;
}

// function prototype declaration
static inline int swsr_shared_queue_add_nonblock(shared_queue_t*, void*, void*);
static inline int swsr_shared_queue_remove_nonblock(shared_queue_t *, void**, void**);
static inline int mwsr_shared_queue_add_nonblock(shared_queue_t*, void*, void*);
static inline int mwsr_shared_queue_remove_nonblock(shared_queue_t *, void**, void**);

static inline shared_queue_t * sized_shared_queue_init(int queue_length)
{
     shared_queue_t *q = NULL;
     void *buf = NULL;

     int pm_retval = 0;
     pm_retval = posix_memalign((void**)&buf, 64, sizeof(shared_queue_t));
     VERIFY_POSIX_MEMALIGN_SUCCESS(pm_retval);

     q = (shared_queue_t *)buf;
     assert(q);
     memset(q, 0, sizeof(shared_queue_t));

     q->free_list = wsfree_list_init(queue_length, shared_queue_data_alloc, NULL);
     if(!q->free_list) {
          free(q);
          error_print("sized_shared_queue_init failed in wsfree_list_init of q->free_list");
          return NULL;
     }

     q->elements = (shared_queue_data_t *)calloc(queue_length, sizeof(shared_queue_data_t)); // used for swsr
     if(!q->elements) {
          free(q);
          error_print("sized_shared_queue_init failed:  out of memory in calloc of q->elements");
          return NULL;
     }
     q->max_length = queue_length;

     // assign the function pointers
     q->shared_queue_add_nonblock = (int (*)(void*, void*, void*))&mwsr_shared_queue_add_nonblock;
     q->shared_queue_remove_nonblock = (int (*)(void*, void*, void*))&mwsr_shared_queue_remove_nonblock;
     QTYPE_MWSR();

     return q;
}


static inline shared_queue_t * shared_queue_init(void)
{
     return sized_shared_queue_init(MAX_SQUEUE_LEN);
}

static inline void shared_queue_exit(shared_queue_t * q) {
     shared_queue_data_t *data, *next;

     wsfree_list_element_destroy(q->free_list);
     data = q->work_queue_head;
     while (data) {
          next = data->next;
          free(data);
          data = next;
     }
     free(q->elements);
     free(q);
}

static inline void reset_shq_type(shared_queue_t * q) {
     assert(q != NULL);

     // swsr case - reset function pointers & reassign the queue type
     q->shared_queue_add_nonblock = (int (*)(void*, void*, void*))&swsr_shared_queue_add_nonblock;
     q->shared_queue_remove_nonblock = (int (*)(void*, void*, void*))&swsr_shared_queue_remove_nonblock;
     QTYPE_SWSR();
}

//nonblocking versions

// single-writer, single-reader nonblock add implementation
static inline int swsr_shared_queue_add_nonblock(shared_queue_t *q, void *data1, void *data2)
{
     uint32_t cur_tail = q->tail;
     uint32_t next_tail = (cur_tail + 1) % q->max_length;
     COMPILER_FENCE();
     if (next_tail == q->head) {
          return 0;
     }

     q->elements[cur_tail].buffer1 = data1;
     q->elements[cur_tail].buffer2 = data2;
     INCR_LENGTH();
     MACHINE_FENCE();
     q->tail = next_tail;
     return 1;
}

// multiple-writer, single-reader nonblock add implementation
static inline int mwsr_shared_queue_add_nonblock(shared_queue_t * q, void * data1, void * data2) {
     shared_queue_data_t *data = NULL, *prev = NULL;

     /* pull element off the free list */
     data = (shared_queue_data_t*) wsfree_list_alloc(q->free_list);
     if (!data) {
          return 0;  //stack is full
     }

     data->buffer1 = data1;
     data->buffer2 = data2;
     data->next = NULL;

     /* put element on the work queue. */
     prev = shared_queue_swap(&q->work_queue_tail, data);
     if (NULL == prev) {
          q->work_queue_head = data;
     } else {
          prev->next = data;
     }
     __sync_fetch_and_add(&q->length, 1);

     return 1;
}


// single-writer, single-reader nonblock remove implementation
static inline int swsr_shared_queue_remove_nonblock(shared_queue_t *q, void **data1, void **data2)
{
     uint32_t cur_head = q->head;
     COMPILER_FENCE();
     if (cur_head == q->tail) {
          return 0;
     }
     *data1 = q->elements[cur_head].buffer1;
     *data2 = q->elements[cur_head].buffer2;
     DECR_LENGTH();
     COMPILER_FENCE(); // MIGHT need to be a MACHINE_FENCE(), but I don't think so
     q->head = (cur_head + 1) % q->max_length;
     return 1;
}

// multiple-writer, single-reader nonblock remove implementation
static inline int mwsr_shared_queue_remove_nonblock(shared_queue_t * q, void ** data1, void ** data2) {
     shared_queue_data_t *element, *old;

     /* find first element in list */
     __sync_synchronize();
     if (NULL == (element = q->work_queue_head)) {
          return 0;
     }

     /* remove first element from queue.  Single reader only */
     if (NULL != element->next) {
          q->work_queue_head = element->next;
     } else {
          q->work_queue_head = NULL;
          old = __sync_val_compare_and_swap(&q->work_queue_tail, element, NULL);
          if (old != element) {
               while (element->next == NULL) {
                    INCR_SCHED_YIELD_ADD();
                    SCHED_YIELD();
                    __sync_synchronize();
               }
               q->work_queue_head = element->next;
          }
     }

     *data1 = element->buffer1;
     *data2 = element->buffer2;
     __sync_fetch_and_add(&q->length, -1);

     //assert(NULL != *data1 && NULL != *data2);
     // in some cases, we only want one valid data value
     // in our shared queue (see workbalance and workreceive
     // kids implementation)
     assert(NULL != *data1 || NULL != *data2);

     wsfree_list_free(q->free_list, element);
     return 1;
}


static inline int shared_queue_length(shared_queue_t *q)
{
     if(q) {
           return q->length;
     }

     return 0;
}

#endif // USE_ATOMICS



#ifdef USE_ATOMICS
//a blocking write to a queue...sort of ;)
//   we attempt SHQ_ADD_ATTEMPT_LIMIT number of times and if unsuccessful, we return
//   to the calling further ensure blocking by interpreting the return value correctly.
//   RETURN VALUES
//    1 --> successfully added using the blocking call
//    0 --> unsuccessful at adding due to hitting the SHQ_ADD_ATTEMPT_LIMIT attempts
static inline int shared_queue_add(shared_queue_t * q, void * data1, void * data2) {
     SQPERF_NRANK();
     uint32_t attempt_limit = 0;
     int ret = 0;
     while (1) {
          if(q->shared_queue_add_nonblock(q, data1, data2)) {
               ret = 1;
               INCR_STORE();
               INCR_TOTAL_LENGTH();
               break;
          }

          attempt_limit++;
          if(attempt_limit > SHQ_ADD_ATTEMPT_LIMIT) {
               // limit on attempts for add has been reached...returning
               break;
          }

          INCR_CANT_STORE();
          INCR_SCHED_YIELD_ADD();
          SCHED_YIELD();
     }

     return ret;
}

//a blocking read from a queue
static inline void shared_queue_remove(shared_queue_t * q, void ** data1, void ** data2) {
     while (1) {
          if (q->shared_queue_remove_nonblock(q, data1, data2)) {
               return;
          }
          INCR_IDLE();
          INCR_SCHED_YIELD_RM();
          SCHED_YIELD();
     }
}

#else // !USE_ATOMICS
typedef struct _shared_queue_t_ {
     void ** buffer1;
     void ** buffer2;
     int length;
     int max_length;
     int head;
     int tail;

     //define function pointers
     int (*shared_queue_add_nonblock)(void* /* the queue */, void*, void*);
     int (*shared_queue_remove_nonblock)(void* /* the queue */, void*, void*);

#ifdef SQ_PERF
     uint64_t nstore, ncantstore, nidle, dequeue;
     uint64_t nsched_yield_add, nsched_yield_rm;
     int64_t ntotlength;
     char type[5];
#endif // SQ_PERF
     pthread_mutex_t mutex;
     pthread_cond_t  cond_hasdata;
     pthread_cond_t  cond_spaceavail;
} shared_queue_t;


// function prototype declaration
static inline int shared_queue_add_nonblock(shared_queue_t*, void*, void*);
static inline int shared_queue_remove_nonblock(shared_queue_t *, void**, void**);

static inline shared_queue_t * sized_shared_queue_init(int queue_length) {

     assert(queue_length > 0);
     shared_queue_t * q = (shared_queue_t *)calloc(1, sizeof(shared_queue_t));
     if (!q) {
          error_print("shared_queue_init failed:  out of memory in calloc of q");
          return NULL;
     }

     q->buffer1 = (void**)calloc(queue_length, sizeof(void *));
     if (!q->buffer1) {
          free(q);
          error_print("shared_queue_init failed:  out of memory in calloc of q->buffer1");
          return NULL;
     }
     q->buffer2 = (void**)calloc(queue_length, sizeof(void *));
     if (!q->buffer2) {
          free(q->buffer1);
          free(q);
          error_print("shared_queue_init failed:  out of memory in calloc of q->buffer2");
          return NULL;
     }

     q->max_length = queue_length;
     //q->max_length = length;

     // assign the function pointers
     q->shared_queue_add_nonblock = (int (*)(void*, void*, void*))&shared_queue_add_nonblock;
     q->shared_queue_remove_nonblock = (int (*)(void*, void*, void*))&shared_queue_remove_nonblock;

     //init pthread
     pthread_mutex_init(&q->mutex, NULL);
     pthread_cond_init(&q->cond_hasdata, NULL);
     pthread_cond_init(&q->cond_spaceavail, NULL);

     return q;
}


static inline shared_queue_t * shared_queue_init(void) {

     return sized_shared_queue_init(MAX_SQUEUE_LEN);
}

static inline void shared_queue_exit(shared_queue_t * q) {
     free(q->buffer1);
     free(q->buffer2);
     free(q);
}

// this function in the non-ATOMICS is intentionally left blank; it's also the code seen
// by SERIAL (as well as the less efficient mutex-lock based PTHREADS)
static inline void reset_shq_type(shared_queue_t * q) {

}

//a blocking write to a queue...sort of :)
//   RETURN VALUES
//    1 --> successfully added using the blocking call
//    0 --> unsuccessful at adding due to hitting the SHQ_ADD_ATTEMPT_LIMIT attempts
static inline int shared_queue_add(shared_queue_t * q, void * data1, void * data2) {
     SQPERF_NRANK();
     uint32_t attempt_limit = 0;
     pthread_mutex_lock(&q->mutex);
     while (q->length == q->max_length) {

          attempt_limit++;
          if(attempt_limit > SHQ_ADD_ATTEMPT_LIMIT) {
               // limit on attempts for add has been reached...returning
               pthread_mutex_unlock(&q->mutex);
               return 0;
          }

          INCR_CANT_STORE();
          pthread_cond_wait(&q->cond_spaceavail, &q->mutex);
     }
     q->buffer1[q->head] = data1;
     q->buffer2[q->head] = data2;
     q->head++;
     q->head %= q->max_length;
     q->length++;
     INCR_STORE();
     INCR_TOTAL_LENGTH();
     pthread_mutex_unlock(&q->mutex);
     pthread_cond_broadcast(&q->cond_hasdata);
     return 1;
}

//a blocking read from a queue
static inline void shared_queue_remove(shared_queue_t * q, void ** data1, void ** data2) {
     pthread_mutex_lock(&q->mutex);
     while (q->length == 0) {
          INCR_IDLE();
          pthread_cond_wait(&q->cond_hasdata, &q->mutex);
     }
     *data1 = q->buffer1[q->tail];
     *data2 = q->buffer2[q->tail];
     q->tail++;
     q->tail %= q->max_length;
     q->length--;
     pthread_mutex_unlock(&q->mutex);
     pthread_cond_broadcast(&q->cond_spaceavail);
}

//nonblocking versions
static inline int shared_queue_add_nonblock(shared_queue_t * q, void * data1, void * data2) {
     SQPERF_NRANK();
     pthread_mutex_lock(&q->mutex);
     if (q->length == q->max_length){
          INCR_CANT_STORE();
          pthread_mutex_unlock(&q->mutex);
          return 0;
     }
     q->buffer1[q->head] = data1;
     q->buffer2[q->head] = data2;
     q->head++;
     q->head %= q->max_length;
     q->length++;
     INCR_STORE();
     INCR_TOTAL_LENGTH();
     pthread_mutex_unlock(&q->mutex);
     pthread_cond_broadcast(&q->cond_hasdata);
     return 1;
}

static inline int shared_queue_remove_nonblock(shared_queue_t * q, void ** data1, void ** data2) {
     pthread_mutex_lock(&q->mutex);
     if (q->length == 0) {
          pthread_mutex_unlock(&q->mutex);
          return 0;
     }
     *data1 = q->buffer1[q->tail];
     *data2 = q->buffer2[q->tail];
     q->tail++;
     q->tail %= q->max_length;
     q->length--;
     pthread_mutex_unlock(&q->mutex);
     pthread_cond_broadcast(&q->cond_spaceavail);
     return 1;
}

static inline int shared_queue_length(shared_queue_t * q) {
     if(!q) {
          return 0;
     }

     int length;
     pthread_mutex_lock(&q->mutex);
     length = q->length;
     pthread_mutex_unlock(&q->mutex);

     return length;
}
#endif // !USE_ATOMICS

#endif // WS_PTHREADS

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _SHARED_QUEUE_H
