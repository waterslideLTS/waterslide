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

#ifndef _CREATE_SPIN_BARRIER_H
#define _CREATE_SPIN_BARRIER_H

#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#ifndef WS_PTHREADS
#define BARRIER_INIT(barrier,nprocs) 1
#define BARRIER_WAIT(barrier) 

#else // WS_PTHREADS
#include <pthread.h>
extern pthread_barrier_t *barrier1;
#define BARRIER_TYPE_T pthread_barrier_t
extern uint32_t work_size;

// Macros for noop serial functions
#define BARRIER_INIT(barrier,nprocs) barrier_init(barrier,nprocs) 
#define BARRIER_WAIT(barrier) barrier_wait(barrier) 

// Prototypes
static inline int barrier_init(BARRIER_TYPE_T **, uint32_t);
static inline void barrier_wait(void *);


// note that barrier_init() will only be called once for pthreads;
static inline int barrier_init(BARRIER_TYPE_T **barrier, uint32_t nprocs) {
     //pthread_barrier_init returns zero if successful
     return (pthread_barrier_init(*barrier, NULL, work_size) == 0);
}

static inline void barrier_wait(void *barrier) {
     pthread_barrier_wait(barrier);
}
#endif // WS_PTHREADS

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _CREATE_SPIN_BARRIER_H
