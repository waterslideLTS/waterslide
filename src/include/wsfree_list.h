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

#include "waterslide.h"

#ifndef _WSFREE_LIST_H
#define _WSFREE_LIST_H

#include <assert.h>
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

typedef void* (*wsfree_list_allocate_t)(void*);

#ifndef WS_PTHREADS

#include "wsstack.h"

struct wsfree_list_node_t {
     uint32_t dummy;
};
typedef struct wsfree_list_node_t wsfree_list_node_t;

struct wsfree_list_t {
     wsstack_t *stack;
     wsfree_list_allocate_t allocator;
     void* allocator_data;
     uint32_t max_allocated;
     uint32_t allocated_count;
};
typedef struct wsfree_list_t wsfree_list_t;


static inline wsfree_list_t* wsfree_list_init(unsigned int max_allocated,
                                              wsfree_list_allocate_t allocator,
                                              void *allocator_data)
{
     wsfree_list_t *fl = (wsfree_list_t*) calloc(1, sizeof(wsfree_list_t));
     if (NULL == fl) {
          error_print("failed wsfree_list_init calloc of fl");
          return NULL;
     }

     fl->allocator = allocator;
     fl->allocator_data = allocator_data;
     fl->max_allocated = max_allocated;

     fl->stack = wsstack_init();
     if (NULL == fl->stack) {
          error_print("failed wsfree_list_init calloc of fl->stack");
          free(fl);
          return NULL;
     }

     return fl;
}

static inline void wsfree_list_element_destroy(wsfree_list_t *fl)
{
     void *node = wsstack_remove(fl->stack);
     while (node) {
          free(node);
          node = wsstack_remove(fl->stack);
     }
     wsstack_destroy(fl->stack);
     free(fl);
}


static inline void wsfree_list_destroy(wsfree_list_t *fl)
{
     wsstack_destroy(fl->stack);
     free(fl);
}


static inline void* wsfree_list_alloc(wsfree_list_t *fl)
{
     void *node = wsstack_remove(fl->stack);
     if (NULL == node) {
          if (fl->max_allocated == 0 || fl->allocated_count < fl->max_allocated) {
               node = fl->allocator(fl->allocator_data);
               if (NULL != node) fl->allocated_count++;
          }
     }
     
     return node;
}


static inline int wsfree_list_free(wsfree_list_t *fl, void *data)
{
     return wsstack_add(fl->stack, data);
}


static inline unsigned int wsfree_list_size(wsfree_list_t *fl)
{
     return wsstack_size(fl->stack);
}


static inline unsigned int wsfree_list_allocated(wsfree_list_t *fl)
{
     return fl->allocated_count;
}

#elif defined(USE_UNHOMED_TLS_FREE_LIST)
// this code is buggy! DO NOT USE USE_UNHOMED_TLS_FREE_LIST.
//#warning USE_UNHOMED_TLS_FREE_LIST 

#include "wsstack.h"

#define BLOCK_SIZE 16

typedef struct wsfree_list_node_t {
    struct wsfree_list_node_t *next;
    struct wsfree_list_node_t *block_tail;
} wsfree_list_node_t;

/* per thread pool */
typedef struct wsfree_list_local_cache_t {
    wsfree_list_node_t *cache;
    uint_fast16_t       count;
    uint8_t            *block;
    uint_fast32_t       i;

    struct wsfree_list_local_cache_t *next; // for cleanup
} wsfree_list_local_cache_t;

struct wsfree_list_t {
     pthread_key_t local_cache_key;
     wsfree_list_node_t *globalpool;
     uint32_t pool_size;
     WS_SPINLOCK_DECL(lock);

     wsfree_list_allocate_t allocator;
     void* allocator_data;
     uint32_t max_allocated;
     uint32_t allocated_count;

     wsfree_list_local_cache_t *caches; // for cleanup
};
typedef struct wsfree_list_t wsfree_list_t;

static inline wsfree_list_t* wsfree_list_init(unsigned int max_allocated,
                                              wsfree_list_allocate_t allocator,
                                              void *allocator_data)
{
     wsfree_list_t *fl = (wsfree_list_t*) calloc(1, sizeof(wsfree_list_t));
     if (NULL == fl) {
          error_print("failed wsfree_list_init calloc of fl");
          return NULL;
     }

     fl->allocator = allocator;
     fl->allocator_data = allocator_data;
     fl->max_allocated = max_allocated;

     WS_SPINLOCK_INIT(&fl->lock);
     pthread_key_create(&fl->local_cache_key, NULL);

     return fl;
}


// Identical to wsfree_list_destroy for this case
static inline void wsfree_list_element_destroy(wsfree_list_t *fl)
{
    wsfree_list_local_cache_t *free_cache;
    wsfree_list_node_t *free_node;

    while (NULL != fl->caches) {
	free_cache = fl->caches;
	fl->caches = free_cache->next;
	while (NULL != free_cache->cache) {
	    free_node = free_cache->cache;
	    free_cache->cache = free_node->next;
	    free(free_node);
	}
	free(free_cache);
    }

    while (NULL != fl->globalpool) {
	free_node = fl->globalpool;
	fl->globalpool = free_node->next;
	free(free_node);
    }

     WS_SPINLOCK_DESTROY(&fl->lock);
     pthread_key_delete(fl->local_cache_key);
     free(fl);
}

static inline void wsfree_list_destroy(wsfree_list_t *fl)
{
    wsfree_list_local_cache_t *free_cache;
    wsfree_list_node_t *free_node;

    while (NULL != fl->caches) {
	free_cache = fl->caches;
	fl->caches = free_cache->next;
	while (NULL != free_cache->cache) {
	    free_node = free_cache->cache;
	    free_cache->cache = free_node->next;
	    free(free_node);
	}
	free(free_cache);
    }

    while (NULL != fl->globalpool) {
	free_node = fl->globalpool;
	fl->globalpool = free_node->next;
	free(free_node);
    }

     WS_SPINLOCK_DESTROY(&fl->lock);
     pthread_key_delete(fl->local_cache_key);
     free(fl);
}

static inline wsfree_list_local_cache_t *wsfree_list_get_cache(wsfree_list_t *fl)
{
    wsfree_list_local_cache_t *tc = (wsfree_list_local_cache_t *)pthread_getspecific(fl->local_cache_key);
    if (NULL == tc) {
	tc = (wsfree_list_local_cache_t *)calloc(1, sizeof(wsfree_list_local_cache_t));
        if (NULL == tc) {
             error_print("failed wsfree_list_init calloc of tc");
             // assertion below will correctly kill this process
        }
	//tc = (wsfree_list_local_cache_t *)memalign(64, sizeof(wsfree_list_local_cache_t));
	assert(tc);
	//memset(tc, 0, sizeof(wsfree_list_local_cache_t));
	do {
	    tc->next = fl->caches;
	} while (!__sync_bool_compare_and_swap(&fl->caches, tc->next, tc));
	pthread_setspecific(fl->local_cache_key, tc);
    }
    return tc;
}

static inline void* wsfree_list_alloc(wsfree_list_t *fl)
{
     wsfree_list_local_cache_t *tls;
     wsfree_list_node_t *node = NULL;
     unsigned int cnt;

     tls = wsfree_list_get_cache(fl);

     if (tls->cache) {
	 node = tls->cache;
	 tls->cache = node->next;
	 --tls->count;
	 return (void*)node;
     } else {
	 cnt = 0;
	 // check the central pool
	 if (fl->globalpool) {
	     WS_SPINLOCK_LOCK(&fl->lock);
	     if (fl->globalpool) {
		 node = fl->globalpool;
		 fl->globalpool = node->block_tail->next;
		 node->block_tail->next = NULL;
		 cnt = BLOCK_SIZE;
		 fl->pool_size -= BLOCK_SIZE;
	     }
	     WS_SPINLOCK_UNLOCK(&fl->lock);
	 }
	 if (NULL == node) {
	     // allocate more
	     if (fl->max_allocated == 0 || fl->allocated_count < fl->max_allocated) {
		 node = (wsfree_list_node_t *)fl->allocator(fl->allocator_data);
		 if (NULL != node) fl->allocated_count++;
	     }
	 } else {
	     tls->cache = node->next;
	     tls->count = BLOCK_SIZE - 1;
	 }
     }

     return node;
}


static inline int wsfree_list_free(wsfree_list_t *fl, void *data)
{
     wsfree_list_node_t *node = (wsfree_list_node_t *)data;
     wsfree_list_node_t *cache;
     wsfree_list_local_cache_t *tls;
     unsigned int cnt;

     tls = wsfree_list_get_cache(fl);
     cache = tls->cache;
     cnt = tls->count;
     if (cache) {
	 node->next = cache;
	 node->block_tail = cache->block_tail;
     } else {
	 node->next = NULL;
	 node->block_tail = node;
     }
     cnt ++;
     if (cnt == (BLOCK_SIZE * 2)) {
	 // push to central pool
	 wsfree_list_node_t *toglobal = node->block_tail->next;
	 node->block_tail->next = NULL;
	 WS_SPINLOCK_LOCK(&fl->lock);
	 toglobal->block_tail->next = fl->globalpool;
	 fl->globalpool = toglobal;
	 fl->pool_size += BLOCK_SIZE;
	 WS_SPINLOCK_UNLOCK(&fl->lock);
	 cnt -= BLOCK_SIZE;
     } else if (cnt == (BLOCK_SIZE + 1)) {
	 node->block_tail = node;
     }
     tls->cache = node;
     tls->count = cnt;
     return 1;
}


static inline unsigned int wsfree_list_size(wsfree_list_t *fl)
{
    unsigned int tally = fl->pool_size;
    wsfree_list_local_cache_t *cursor = fl->caches;
    while (cursor) {
	tally += cursor->count;
	cursor = cursor->next;
    }
     return tally;
}


static inline unsigned int wsfree_list_allocated(wsfree_list_t *fl)
{
     return fl->allocated_count;
}

#elif defined(USE_ATOMIC_STACK)
//#warning USE_ATOMIC_STACK

#include "shared/wsstack_atomic.h"

struct wsfree_list_node_t {
};
typedef struct wsfree_list_node_t wsfree_list_node_t;

struct wsfree_list_t {
     wsstack_atomic_t *stack;
     wsfree_list_allocate_t allocator;
     void* allocator_data;
     uint32_t max_allocated;
     uint32_t allocated_count;
};
typedef struct wsfree_list_t wsfree_list_t;


static inline wsfree_list_t* wsfree_list_init(unsigned int max_allocated,
                                              wsfree_list_allocate_t allocator,
                                              void *allocator_data)
{
     wsfree_list_t *fl = (wsfree_list_t*) calloc(1, sizeof(wsfree_list_t));
     if (NULL == fl) {
          error_print("failed wsfree_list_init calloc of fl");
          return NULL;
     }

     fl->allocator = allocator;
     fl->allocator_data = allocator_data;
     fl->max_allocated = max_allocated;

     fl->stack = wsstack_atomic_init();
     if (NULL == fl->stack) {
          free(fl);
          return NULL;
     }

     return fl;
}


static inline void wsfree_list_element_destroy(wsfree_list_t *fl)
{
     void *node = wsstack_atomic_remove(fl->stack);
     while (node) {
          free(node);
          node = wsstack_atomic_remove(fl->stack);
     }
     wsstack_atomic_destroy(fl->stack);
     free(fl);
}


static inline void wsfree_list_destroy(wsfree_list_t *fl)
{
     wsstack_atomic_destroy(fl->stack);
     free(fl);
}


static inline void* wsfree_list_alloc(wsfree_list_t *fl)
{
     void *node = wsstack_atomic_remove(fl->stack);
     if (NULL == node) {
          if (fl->max_allocated == 0 || fl->allocated_count < fl->max_allocated) {
               node = fl->allocator(fl->allocator_data);
               if (NULL != node) (void)__sync_fetch_and_add(&fl->allocated_count, 1);
          }
     }
     
     return node;
}


static inline int wsfree_list_free(wsfree_list_t *fl, void *data)
{
     return wsstack_atomic_add(fl->stack, data);
}


static inline unsigned int wsfree_list_size(wsfree_list_t *fl)
{
     return wsstack_atomic_size(fl->stack);
}


static inline unsigned int wsfree_list_allocated(wsfree_list_t *fl)
{
     __sync_synchronize();
     return fl->allocated_count;
}


#elif defined(USE_MUTEX_HOMED_FREE_LIST)
//#warning USE_MUTEX_HOMED_FREE_LIST

#include <pthread.h>

struct wsfree_list_local_cache_t;

struct wsfree_list_node_t {
     struct wsfree_list_node_t *next;
     struct wsfree_list_local_cache_t *home;
};
typedef struct wsfree_list_node_t wsfree_list_node_t;

/* per thread pool */
struct wsfree_list_local_cache_t {
     struct wsfree_list_local_cache_t *next; /* must hold free list mutex */
     wsfree_list_node_t *queue_head;
     WS_SPINLOCK_DECL(lock);
     uint32_t length;
     uint32_t allocated_count;
};
typedef struct wsfree_list_local_cache_t wsfree_list_local_cache_t;

struct wsfree_list_t {
     wsfree_list_allocate_t allocator;
     void *allocator_data;
     uint32_t max_allocated;

     wsfree_list_local_cache_t *caches;
     pthread_key_t local_cache_key;
     WS_SPINLOCK_DECL(lock);
};
typedef struct wsfree_list_t wsfree_list_t;


static inline wsfree_list_local_cache_t*
wsfree_list_get_cache(wsfree_list_t *fl)
{
     wsfree_list_local_cache_t *cache = 
          (wsfree_list_local_cache_t*) pthread_getspecific(fl->local_cache_key);
     /* if our thread doesn't have a cache, create one */
     if (NULL == cache) {
          cache = (wsfree_list_local_cache_t*)
               calloc(1, sizeof(wsfree_list_local_cache_t));
          if (NULL == cache) {
               error_print("failed wsfree_list_init calloc of cache");
               return NULL;
          }
          pthread_setspecific(fl->local_cache_key, cache);
          WS_SPINLOCK_INIT(&cache->lock);

          WS_SPINLOCK_LOCK(&fl->lock);
          cache->next = fl->caches;
          fl->caches = cache;
          WS_SPINLOCK_UNLOCK(&fl->lock);
     }

     return cache;
}


static inline wsfree_list_t* wsfree_list_init(unsigned int max_allocated,
                                              wsfree_list_allocate_t allocator,
                                              void *allocator_data)
{
     wsfree_list_t *fl = (wsfree_list_t*) calloc(1, sizeof(wsfree_list_t));
     if (NULL == fl) {
          error_print("failed wsfree_list_init calloc of fl");
          return NULL;
     }

     fl->allocator = allocator;
     fl->allocator_data = allocator_data;
     fl->max_allocated = max_allocated;
     pthread_key_create(&fl->local_cache_key, NULL);
     WS_SPINLOCK_INIT(&fl->lock);

     return fl;
}


static inline void wsfree_list_element_destroy(wsfree_list_t *fl)
{
     wsfree_list_local_cache_t *tmp, *cache = fl->caches;
     wsfree_list_node_t *element;

     while (NULL != cache) {
          element = cache->queue_head;
          while (NULL != element) {
               cache->queue_head = element->next;
               cache->length--;
               free(element);
               element = cache->queue_head;
          }
          tmp = cache->next;
	  WS_SPINLOCK_DESTROY(&cache->lock);
          free(cache);
          cache = tmp;
     }

     WS_SPINLOCK_DESTROY(&fl->lock);
     pthread_key_delete(fl->local_cache_key);
     free(fl);
}


static inline void wsfree_list_destroy(wsfree_list_t *fl)
{
     wsfree_list_local_cache_t *tmp, *cache = fl->caches;

     while (NULL != cache) {
          tmp = cache->next;
	  WS_SPINLOCK_DESTROY(&cache->lock);
          free(cache);
          cache = tmp;
     }

     WS_SPINLOCK_DESTROY(&fl->lock);
     pthread_key_delete(fl->local_cache_key);
     free(fl);
}


static inline void* wsfree_list_alloc(wsfree_list_t *fl)
{
     wsfree_list_node_t *element;
     wsfree_list_local_cache_t *cache;

     if (NULL == fl) return NULL;

     cache = wsfree_list_get_cache(fl);

     WS_SPINLOCK_LOCK(&cache->lock);
     element = cache->queue_head;
     if (NULL != element) {
          cache->queue_head = element->next;
          cache->length--;
     }
     WS_SPINLOCK_UNLOCK(&cache->lock);

     if (NULL == element) {
          if (fl->max_allocated == 0 || cache->allocated_count < fl->max_allocated) {
              element = (wsfree_list_node_t*) fl->allocator(fl->allocator_data);
              if (NULL != element) {
                  /* note that we (being the owning thread) are the
                     only ones who can ever write to allocated_count
                     (as opposed to queue_head, which is written by
                     other threads during the return phase), so we
                     don't need a lock here. */
                  cache->allocated_count++;
                  element->home = cache;
              }
          }
     }

     return (void*) element;
}


static inline int wsfree_list_free(wsfree_list_t *fl, void *data)
{
     wsfree_list_node_t *element = (wsfree_list_node_t*) data;
     wsfree_list_local_cache_t *cache = element->home;

     if (NULL == fl) return 0;

     WS_SPINLOCK_LOCK(&cache->lock);
     element->next = cache->queue_head;
     cache->queue_head = element;
     cache->length++;
     WS_SPINLOCK_UNLOCK(&cache->lock);

     return 1;
}


static inline unsigned int wsfree_list_size(wsfree_list_t *fl)
{
     unsigned int tmp = 0;
     wsfree_list_local_cache_t *cache = fl->caches;

     WS_SPINLOCK_LOCK(&fl->lock);
     while (NULL != cache) {
          WS_SPINLOCK_LOCK(&cache->lock);
          tmp += cache->length;
          WS_SPINLOCK_UNLOCK(&cache->lock);
          cache = cache->next;
     }
     WS_SPINLOCK_UNLOCK(&fl->lock);

     return tmp;
}


static inline unsigned int wsfree_list_allocated(wsfree_list_t *fl)
{
     unsigned int tmp = 0;
     wsfree_list_local_cache_t *cache = fl->caches;

     WS_SPINLOCK_LOCK(&fl->lock);
     while (NULL != cache) {
          WS_SPINLOCK_LOCK(&cache->lock);
          tmp += cache->allocated_count;
          WS_SPINLOCK_UNLOCK(&cache->lock);
          cache = cache->next;
     }
     WS_SPINLOCK_UNLOCK(&fl->lock);

     return tmp;
}

#elif defined(USE_ATOMIC_HOMED_FREE_LIST)
// this code is buggy! DO NOT USE USE_ATOMIC_HOMED_FREE_LIST.
#warning USE_ATOMIC_HOMED_FREE_LIST 

#include <pthread.h>

struct wsfree_list_local_cache_t;

struct wsfree_list_node_t {
     struct wsfree_list_node_t *next;
     struct wsfree_list_local_cache_t *home;
};
typedef struct wsfree_list_node_t wsfree_list_node_t;

/* per thread pool */
struct wsfree_list_local_cache_t {
     struct wsfree_list_local_cache_t *next; /* must hold free list mutex */
     wsfree_list_node_t *queue_head;
     wsfree_list_node_t *queue_tail;
     uint32_t length;
};
typedef struct wsfree_list_local_cache_t wsfree_list_local_cache_t;

struct wsfree_list_t {
     wsfree_list_allocate_t allocator;
     void *allocator_data;
     uint32_t max_allocated;
     uint32_t allocated_count;

     wsfree_list_local_cache_t *caches;
     pthread_key_t local_cache_key;
     WS_SPINLOCK_DECL(lock);
};
typedef struct wsfree_list_t wsfree_list_t;


static inline wsfree_list_node_t * wsfree_list_swap(wsfree_list_node_t *volatile *addr,
                                                    wsfree_list_node_t *newval)
{
    wsfree_list_node_t *oldval;

    do {
        oldval = *addr;
    } while (!__sync_bool_compare_and_swap(addr, oldval, newval));
    return oldval;
}


static inline wsfree_list_local_cache_t*
wsfree_list_get_cache(wsfree_list_t *fl)
{
     wsfree_list_local_cache_t *cache = 
          (wsfree_list_local_cache_t*) pthread_getspecific(fl->local_cache_key);
     /* if our thread doesn't have a cache, create one */
     if (NULL == cache) {
          cache = (wsfree_list_local_cache_t*)
               calloc(1, sizeof(wsfree_list_local_cache_t));
          if (NULL == cache) {
               error_print("failed wsfree_list_init calloc of cache");
               return NULL;
          }
          pthread_setspecific(fl->local_cache_key, cache);

          WS_SPINLOCK_LOCK(&fl->lock);
          cache->next = fl->caches;
          fl->caches = cache;
          WS_SPINLOCK_UNLOCK(&fl->lock);
     }

     return cache;
}


static inline wsfree_list_t* wsfree_list_init(unsigned int max_allocated,
                                              wsfree_list_allocate_t allocator,
                                              void *allocator_data)
{
     wsfree_list_t *fl = (wsfree_list_t*) calloc(1, sizeof(wsfree_list_t));
     if (NULL == fl) {
          error_print("failed wsfree_list_init calloc of fl");
          return NULL;
     }

     fl->allocator = allocator;
     fl->allocator_data = allocator_data;
     fl->max_allocated = max_allocated;
     pthread_key_create(&fl->local_cache_key, NULL);
     WS_SPINLOCK_INIT(&fl->lock);

     return fl;
}


static inline void wsfree_list_element_destroy(wsfree_list_t *fl)
{
     wsfree_list_local_cache_t *tmp, *cache = fl->caches;
     wsfree_list_node_t *element;

     while (NULL != cache) {
          element = cache->queue_head;
          while (NULL != element) {
               cache->queue_head = element->next;
               cache->length--;
               free(element);
               element = cache->queue_head;
          }
          tmp = cache->next;
          free(cache);
          cache = tmp;
     }

     WS_SPINLOCK_DESTROY(&fl->lock);
     pthread_key_delete(fl->local_cache_key);
     free(fl);
}


static inline void wsfree_list_destroy(wsfree_list_t *fl)
{
     wsfree_list_local_cache_t *tmp, *cache = fl->caches;

     while (NULL != cache) {
          tmp = cache->next;
          free(cache);
          cache = tmp;
     }

     /* BWB: FIX ME: free mutex */
     pthread_key_delete(fl->local_cache_key);
     WS_SPINLOCK_DESTROY(&fl->lock);
     free(fl);
}


static inline void* wsfree_list_alloc(wsfree_list_t *fl)
{
     wsfree_list_node_t *element, *old;
     wsfree_list_local_cache_t *cache;

     if (NULL == fl) return NULL;

     cache = wsfree_list_get_cache(fl);

     /* find the first element in list */
     __sync_synchronize();
     if (NULL == (element = cache->queue_head)) {
          /* allocate new */
          if (fl->max_allocated == 0 || fl->allocated_count < fl->max_allocated) {
               element = (wsfree_list_node_t*) fl->allocator(fl->allocator_data);
               if (NULL != element) {
                    (void)__sync_fetch_and_add(&fl->allocated_count, 1);
                    element->home = cache;
               }
          }
          return (void*) element;
     }

     /* remove first element from queue.  single reader only */
     if (NULL != element->next) {
          cache->queue_head = element->next;
     } else {
          cache->queue_head = NULL;
          old = __sync_val_compare_and_swap(&cache->queue_tail, element, NULL);
          if (old != element) {
               while (element->next == NULL) { sched_yield(); }
               cache->queue_head = element->next;
          }
     }
     (void) __sync_fetch_and_sub(&cache->length, 1);
     __sync_synchronize();

     return (void*) element;
}

static inline int wsfree_list_free(wsfree_list_t *fl, void *data)
{
     wsfree_list_node_t *element = (wsfree_list_node_t*) data;
     wsfree_list_node_t *prev;
     wsfree_list_local_cache_t *cache = element->home;

     if (NULL == fl) return 0;

     prev = wsfree_list_swap(&cache->queue_tail, element);
     if (NULL == prev) {
          cache->queue_head = element;
     } else {
          prev->next = element;
     }
     (void)__sync_fetch_and_add(&cache->length, 1);

     return 1;
}

static inline unsigned int wsfree_list_size(wsfree_list_t *fl)
{
     unsigned int tmp = 0;
     wsfree_list_local_cache_t *cache = fl->caches;

     WS_SPINLOCK_LOCK(&fl->lock);
     while (NULL != cache) {
          __sync_synchronize();
          tmp += cache->length;
          cache = cache->next;
     }
     WS_SPINLOCK_UNLOCK(&fl->lock);

     return tmp;
}

static inline unsigned int wsfree_list_allocated(wsfree_list_t *fl)
{
     __sync_synchronize();
     return fl->allocated_count;
}

#endif // WS_PTHREADS

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WSFREE_LIST_H
