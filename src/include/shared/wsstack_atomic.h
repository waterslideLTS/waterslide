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

#ifndef _WSSTACK_ATOMIC_H
#define _WSSTACK_ATOMIC_H

#ifdef WS_PTHREADS
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "error_print.h"
#include <assert.h>

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#define CTR_MASK (15)
#define PTR(x)   ((wsstack_atomic_node_t*)((uintptr_t)(x) & ~(uintptr_t)CTR_MASK))
#define CTR(x)   ((uintptr_t)(x) & CTR_MASK)
#define INCR(x)  ((wsstack_atomic_node_t*)((uintptr_t)PTR(x) |((CTR(x)+1) & CTR_MASK)))

/* stack implemented as single-linked list */
typedef struct _wsstack_atomic_node_t
{
     void *data;
     struct _wsstack_atomic_node_t * next;
} wsstack_atomic_node_t;

typedef struct _wsstack_atomic_t
{
     wsstack_atomic_node_t * head;  /* records removed from here */
     wsstack_atomic_node_t * freeq;
     int size;
} wsstack_atomic_t;

/* allocates & initializes stack memory */
static inline wsstack_atomic_t* wsstack_atomic_init(void)
{
     wsstack_atomic_t * s;

     s = (wsstack_atomic_t*) calloc(1, sizeof(wsstack_atomic_t));
     if (!s) {
          error_print("wsstack_atomic_init failed -- calloc");
     }

     return s;
}

/* frees stack memory */
static inline void wsstack_atomic_destroy(wsstack_atomic_t * s)
{
     wsstack_atomic_node_t * cursor;
     wsstack_atomic_node_t * next;

     if(s == NULL) {
          return;
     }

     /* free all nodes */
     next = s->head;
     while(next != NULL) {
          cursor = next;
          next = PTR(next)->next;
          free(PTR(cursor));
     }

     next = s->freeq;
     while(next != NULL) {
          cursor = next;
          next = PTR(next)->next;
          free(PTR(cursor));
     }

     /* free stack */
     free(s);
}

/***** 
 * adds new node to the stack with pointer to data
 * allocates memory for this node
 * returns pointer to node or NULL on error
 *****/
static inline int wsstack_atomic_add(wsstack_atomic_t *s, void *data)
{

     if (s == NULL) return 0;
     
     wsstack_atomic_node_t *new_node = NULL;

     while ((new_node = s->freeq) != NULL) {
         if (__sync_bool_compare_and_swap(&s->freeq, new_node, PTR(new_node)->next)) {
             break;
         }
     }
     if (NULL == new_node) {
          /* allocate memory and initialize */
	  int mem_rtn = posix_memalign((void**)&new_node, sizeof(wsstack_atomic_node_t), 16);
          if (0 != mem_rtn || !new_node) {
               return 0;
          }
          new_node->data = NULL;
     }
     assert(PTR(new_node)->data == NULL);
     PTR(new_node)->data = data;

     do {
         PTR(new_node)->next = s->head;
     } while (!__sync_bool_compare_and_swap(&s->head, PTR(new_node)->next, new_node));

     (void)__sync_fetch_and_add(&s->size, 1);

     return 1;
}

/***** 
 * removes node from the stack
 * frees memory allocated for this node
 * returns pointer to node data or NULL on error
 *****/
static inline void* wsstack_atomic_remove(wsstack_atomic_t *s)
{
     void     *retval  = NULL;
     wsstack_atomic_node_t *rm_node = NULL;

     if((s == NULL) || (s->head == NULL)) {
          return NULL;
     }

     while ((rm_node = s->head) != NULL) {
         if (__sync_bool_compare_and_swap(&s->head, rm_node, PTR(rm_node)->next)) {
             break;
         }
     }
     if (rm_node == NULL) return NULL;

     retval = PTR(rm_node)->data;
     PTR(rm_node)->data = NULL;
     rm_node = INCR(rm_node); // increment before tossing into the freeq

     do {
         PTR(rm_node)->next = s->freeq;
     } while (!__sync_bool_compare_and_swap(&s->freeq, PTR(rm_node)->next, rm_node));

     (void)__sync_fetch_and_sub(&s->size, 1);

     return retval;
}

static inline int wsstack_atomic_clear(wsstack_atomic_t *s)
{
     wsstack_atomic_node_t *qnode;
     wsstack_atomic_node_t *this_node;
     int cnt = 0;

     if(s == NULL) {
          return 0;
     }

     /* remove the entire list from head in one atomic operation.  At
        that point, no other thread can interact with the list
        referenced by qnode, so only need inserts back into the freeq
        to be atomic. */
     while ((qnode = s->head) != NULL) {
         if (__sync_bool_compare_and_swap(&s->head, qnode, NULL)) {
             break;
         }
     }
     if (qnode == NULL) return 0;


     while (qnode != NULL) {
         this_node = INCR(qnode);
         qnode = PTR(qnode)->next;

         do {
             PTR(this_node)->next = s->freeq;
         } while (!__sync_bool_compare_and_swap(&s->freeq, PTR(this_node)->next, this_node));
         cnt++;
     }

     (void)__sync_fetch_and_sub(&s->size, cnt);

     return cnt;
}

static inline int wsstack_atomic_size(wsstack_atomic_t *s)
{
     return s->size;
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // WS_PTHREADS

#endif // _WSSTACK_ATOMIC_H
