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
// A multiple writer, multiple reader lock-based queue, where the lock is based on pthread_mutexes

#ifndef _MWMR_QUEUE_H
#define _MWMR_QUEUE_H

#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#if !defined(__FreeBSD__)
#include <malloc.h>
#endif
#include <assert.h>
#include "shared/getrank.h"
#include "error_print.h"

#define MAX_SQUEUE_LEN	(16) // these number of metadata pointers should be enough
                             // for occasional spikes by proc_kids writing to ext. queue
#define SHQ_ADD_ATTEMPT_LIMIT (1000)

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#ifdef WS_PTHREADS

#define USE_SCHEDYIELD

#ifdef SQ_PERF

#define INCR_STORE() q->nstore++; os[nrank]++;
#define INCR_CANT_STORE() q->ncantstore++; osf[nrank]++;
#define INCR_IDLE() q->nidle++;
#define INCR_TOTAL_LENGTH() q->ntotlength+=q->length;
#define SQPERF_NRANK() const int nrank = GETRANK();

#else // !SQ_PERF

#define INCR_STORE()
#define INCR_CANT_STORE()
#define INCR_IDLE()
#define INCR_TOTAL_LENGTH()
#define SQPERF_NRANK()

#endif // SQ_PERF

// Globals
extern uint64_t * os, * osf;


#include "wsfree_list.h"

typedef struct _mwmr_queue_t_ {
     void ** buffer1;
     void ** buffer2;
     int length;
     int max_length;
     int head;
     int tail;

     //define function pointers
     int (*mwmr_queue_add_nonblock)(void* /* the queue */, void*, void*);
     int (*mwmr_queue_remove_nonblock)(void* /* the queue */, void*, void*);

#ifdef SQ_PERF
     uint64_t nstore, ncantstore, nidle, dequeue;
     uint64_t nsched_yield_add, nsched_yield_rm;
     int64_t ntotlength;
     char type[5];
#endif // SQ_PERF
     pthread_mutex_t mutex;
     pthread_cond_t  cond_hasdata;
     pthread_cond_t  cond_spaceavail;
} mwmr_queue_t;


// function prototype declaration
static inline int mwmr_queue_add_nonblock(mwmr_queue_t*, void*, void*);
static inline int mwmr_queue_remove_nonblock(mwmr_queue_t *, void**, void**);

static inline mwmr_queue_t * sized_mwmr_queue_init(int queue_length) {

     assert(queue_length > 0);
     mwmr_queue_t * q = (mwmr_queue_t *)calloc(1, sizeof(mwmr_queue_t));
     if (!q) {
          error_print("mwmr_queue_init failed:  out of memory in calloc of q");
          return NULL;
     }

     q->buffer1 = (void**)calloc(queue_length, sizeof(void *));
     if (!q->buffer1) {
          free(q);
          error_print("mwmr_queue_init failed:  out of memory in calloc of q->buffer1");
          return NULL;
     }
     q->buffer2 = (void**)calloc(queue_length, sizeof(void *));
     if (!q->buffer2) {
          free(q->buffer1);
          free(q);
          error_print("mwmr_queue_init failed:  out of memory in calloc of q->buffer2");
          return NULL;
     }

     q->max_length = queue_length;
     //q->max_length = length;

     // assign the function pointers
     q->mwmr_queue_add_nonblock = (int (*)(void*, void*, void*))&mwmr_queue_add_nonblock;
     q->mwmr_queue_remove_nonblock = (int (*)(void*, void*, void*))&mwmr_queue_remove_nonblock;

     //init pthread
     pthread_mutex_init(&q->mutex, NULL);
     pthread_cond_init(&q->cond_hasdata, NULL);
     pthread_cond_init(&q->cond_spaceavail, NULL);

     return q;
}


static inline mwmr_queue_t * mwmr_queue_init(void) {

     return sized_mwmr_queue_init(MAX_SQUEUE_LEN);
}

static inline void mwmr_queue_exit(mwmr_queue_t * q) {
     free(q->buffer1);
     free(q->buffer2);
     free(q);
}

//a blocking write to a queue...sort of :)
//   RETURN VALUES
//    1 --> successfully added using the blocking call
//    0 --> unsuccessful at adding due to hitting the SHQ_ADD_ATTEMPT_LIMIT attempts
static inline int mwmr_queue_add(mwmr_queue_t * q, void * data1, void * data2) {
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
static inline void mwmr_queue_remove(mwmr_queue_t * q, void ** data1, void ** data2) {
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
static inline int mwmr_queue_add_nonblock(mwmr_queue_t * q, void * data1, void * data2) {
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

static inline int mwmr_queue_remove_nonblock(mwmr_queue_t * q, void ** data1, void ** data2) {
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

static inline int mwmr_queue_length(mwmr_queue_t * q) {
     if(!q) {
          return 0;
     }

     int length; 
     pthread_mutex_lock(&q->mutex);
     length = q->length;
     pthread_mutex_unlock(&q->mutex);

     return length;
}

#endif // WS_PTHREADS

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _MWMR_QUEUE_H
