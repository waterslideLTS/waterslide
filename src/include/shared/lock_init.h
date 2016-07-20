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

#ifndef _LOCK_INIT_H
#define _LOCK_INIT_H

#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

// Macros for noop serial functions
#ifndef WS_PTHREADS
#define LOCK_DESTROY()
#define WS_SPINLOCK_DECL(spin)
#define WS_SPINLOCK_EXTERN(spin)
#define WS_SPINLOCK_INIT(spin)
#define WS_SPINLOCK_DESTROY(spin)
#define WS_SPINLOCK_LOCK(spin)
#define WS_SPINLOCK_UNLOCK(spin)
#define WS_MUTEX_DECL(mutex) 
#define WS_MUTEX_EXTERN(mutex) 
#define WS_MUTEX_INIT(mutex,attr)
#define WS_MUTEX_DESTROY(mutex)
#define WS_MUTEX_LOCK(mutex)
#define WS_MUTEX_UNLOCK(mutex)

#else // WS_PTHREADS

#define LOCK_DESTROY() lock_destroy()

#define __USE_UNIX98 1
#include <pthread.h>
#include <sched.h>
#include <errno.h>
#include "error_print.h"

#define WS_SPINLOCK_DECL(spin) pthread_spinlock_t spin;
#define WS_SPINLOCK_EXTERN(spin) extern pthread_spinlock_t spin;
#define WS_SPINLOCK_INIT(spin) pthread_spin_init(spin, PTHREAD_PROCESS_PRIVATE);
#define WS_SPINLOCK_DESTROY(spin) pthread_spin_destroy(spin);

#define WS_SPINLOCK_LOCK(spin)                                               \
     {                                                                       \
          switch (pthread_spin_lock(spin)) {                                 \
               case EINVAL: error_print("spin lock not initialized: %s:%d",  \
                                        __FILE__, __LINE__);                 \
                            abort();                                         \
               case EDEADLK: error_print("double spin lock found: %s:%d",    \
                                         __FILE__, __LINE__);                \
                             abort();                                        \
          }                                                                  \
     }
#define WS_SPINLOCK_UNLOCK(spin)                                             \
     {                                                                       \
          switch (pthread_spin_unlock(spin)) {                               \
               case EINVAL: error_print("invalid spin unlock: %s:%d",        \
                                        __FILE__, __LINE__);                 \
                            abort();                                         \
               case EPERM: error_print("bad spin unlock: %s:%d",             \
                                       __FILE__, __LINE__);                  \
                           abort();                                          \
          }                                                                  \
     }

#define WS_MUTEX_DECL(mutex) pthread_mutex_t mutex;
#define WS_MUTEX_EXTERN(mutex) extern pthread_mutex_t mutex;
#ifdef WS_LOCK_DBG
#define WS_MUTEX_ATTR(attr) pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
#define WS_MUTEX_INIT(mutex,attr) pthread_mutex_init(mutex, &attr);
#else
#define WS_MUTEX_INIT(mutex,attr) pthread_mutex_init(mutex, NULL);
#define WS_MUTEX_ATTR(attr)
#endif // WS_LOCK_DBG
#define WS_MUTEX_DESTROY(mutex) pthread_mutex_destroy(mutex);

#define WS_MUTEX_LOCK(mutex)                                                 \
     {                                                                       \
          switch (pthread_mutex_lock(mutex)) {                               \
               case EINVAL: error_print("mutex lock not initialized: %s:%d", \
                                        __FILE__, __LINE__);                 \
                            abort();                                         \
               case EDEADLK: error_print("double mutex lock found: %s:%d",   \
                                         __FILE__, __LINE__);                \
                             abort();                                        \
          }                                                                  \
     }

#define WS_MUTEX_UNLOCK(mutex)                                               \
     {                                                                       \
          switch (pthread_mutex_unlock(mutex)) {                             \
               case EPERM: error_print("bad mutex unlock: %s:%d",            \
                                       __FILE__, __LINE__);                  \
                           abort();                                          \
          }                                                                  \
     }


WS_MUTEX_EXTERN(startlock)
WS_MUTEX_EXTERN(endgame_lock)
WS_MUTEX_EXTERN(exit_lock)
extern pthread_mutexattr_t mutex_attr;

// Create and destroy a set of locks for parallel execution

static inline int lock_init(void) {

     // Note that mutex_attr is only used if WS_LOCK_DBG is turned on.
     WS_MUTEX_ATTR(mutex_attr)

     // Allocate mutex locks
     WS_MUTEX_INIT(&startlock, mutex_attr)
     //endgame_lock used in ws_destroy_graph() and sht_print_registry()
     WS_MUTEX_INIT(&endgame_lock, mutex_attr)
     //exit_lock used in clean_exit()
     WS_MUTEX_INIT(&exit_lock, mutex_attr)

     //test the mutex locks
#ifdef WS_LOCK_DBG
     WS_MUTEX_LOCK(&startlock)
     WS_MUTEX_UNLOCK(&startlock)
     WS_MUTEX_LOCK(&endgame_lock)
     WS_MUTEX_UNLOCK(&endgame_lock)
     WS_MUTEX_LOCK(&exit_lock)
     WS_MUTEX_UNLOCK(&exit_lock)
#endif // WS_LOCK_DBG

     return 1;
}

static inline void lock_destroy(void) {

// Do not destroy exit_lock!
     WS_MUTEX_DESTROY(&startlock)
     WS_MUTEX_DESTROY(&endgame_lock)
}
#endif // WS_PTHREADS

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _LOCK_INIT_H

