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

#ifndef _WSSTACK_H
#define _WSSTACK_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "error_print.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

/* stack implemented as single-linked list */
typedef struct _wsstack_node_t
{
     void *data;
     struct _wsstack_node_t *next;
} wsstack_node_t;

typedef struct _wsstack_t
{
     wsstack_node_t     *head;  /* records removed from here */
     wsstack_node_t     *freeq;
     unsigned int  size;
     unsigned int  max;
} wsstack_t;

/* allocates & initializes stack memory */
static inline wsstack_t* wsstack_init(void)
{
     wsstack_t * s;

     s = (wsstack_t*) calloc(1, sizeof(wsstack_t));
     if (!s) {
          error_print("failed wsstack_init calloc of s");
          return NULL;
     }

     return s;
}

/* frees stack memory */
static inline void wsstack_destroy(wsstack_t * s)
{
     wsstack_node_t * cursor;
     wsstack_node_t * next;

     if(s == NULL) {
          return;
     }

     /* free all nodes */
     next = s->head;
     while(next != NULL) {
          cursor = next;
          next = next->next;
          free(cursor);
     }

     next = s->freeq;
     while(next != NULL) {
          cursor = next;
          next = next->next;
          free(cursor);
     }

     /* free stack */
     free(s);
}

/***** 
 * adds new node to the stack with pointer to data
 * allocates memory for this node
 * returns pointer to node or NULL on error
 *****/
static inline int wsstack_add(wsstack_t *s, void *data)
{

     if ((s == NULL) || (s->max && (s->size >= s->max))) {
          return 0;
     }
     
     wsstack_node_t *new_node = NULL;

     if (s->freeq) {
          new_node = s->freeq;
          s->freeq = new_node->next;
     }
     else {
          /* allocate memory and initialize */
          new_node = (wsstack_node_t*)malloc(sizeof(wsstack_node_t));
          if (!new_node) {
               error_print("failed wsstack_add malloc of new_node");
               return 0;
          }
     }
     new_node->data = data;

     /* maintain stack */
     s->size++;

     new_node->next = s->head;
     s->head = new_node;

     return 1;
}

/***** 
 * removes node from the stack
 * frees memory allocated for this node
 * returns pointer to node data or NULL on error
 *****/
static inline void* wsstack_remove(wsstack_t *s)
{
     void     *retval  = NULL;
     wsstack_node_t *rm_node = NULL;

     if((s == NULL) || (s->head == NULL)) {
          return NULL;
     }

     /* check for head == NULL? */
     retval = s->head->data;

     /* maintain stack */
     s->size--;

     rm_node = s->head;
     s->head = s->head->next;

     rm_node->next = s->freeq;
     s->freeq = rm_node;

     return retval;
}

static inline unsigned int wsstack_size(wsstack_t *s)
{
     if(s == NULL) {
          return 0;
     }

     return s->size;
}

static inline int wsstack_clear(wsstack_t *s)
{
     wsstack_node_t *qnode;
     wsstack_node_t *this_node;

     if(s == NULL) {
          return 0;
     }
     int cnt = 0;

     /* place all nodes in freeq */
     qnode = s->head;
     while(qnode != NULL) {
          this_node = qnode;	
          qnode = qnode->next;
          this_node->next = s->freeq;
          s->freeq = this_node;
          cnt++;
     }

     s->size = 0;
     s->head = NULL;

     return cnt;
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WSSTACK_H
