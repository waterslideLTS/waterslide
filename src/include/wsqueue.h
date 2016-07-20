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

#ifndef _WSQUEUE_H
#define _WSQUEUE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "shared/lock_init.h"
#include "error_print.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

/* queue implemented as doubly-linked list */
typedef struct _q_node_t
{
     void             *data;
     struct _q_node_t *prev;
     struct _q_node_t *next;
} q_node_t;

typedef struct _nhqueue_t
{
     q_node_t     *head;  /* records removed from here */
     q_node_t     *tail;  /* records added here */
     q_node_t     *freeq;
     unsigned int  size;

     // these are declared here for the shared version
     WS_MUTEX_DECL(queue_lock);
     void * sharedata;
     char * sharelabel;
     void * v_type_table;
} nhqueue_t;

/* function declarations */
static inline int queue_extract(nhqueue_t*, q_node_t*);


/* allocates & initializes queue memory */
static inline nhqueue_t* queue_init(void)
{
     nhqueue_t *new_queue;

     new_queue = (nhqueue_t*) calloc(1, sizeof(nhqueue_t));
     if (!new_queue) {
          error_print("failed queue_init calloc of new_queue");
          return NULL;
     }

     return new_queue;
}

/* frees queue memory */
static inline void queue_exit(nhqueue_t *qp)
{
     q_node_t *qnode, *this_node;

     if(qp == NULL) {
          return;
     }

     /* free all nodes and their data */
     qnode = qp->head;
     while(qnode != NULL) {
          if (qnode->data) {
               free(qnode->data);
               qnode->data = NULL;
          }
          this_node = qnode;	
          qnode = qnode->next;
          free(this_node);
     }

     /* free all freeq nodes */
     qnode = qp->freeq;
     while(qnode != NULL) {
          this_node = qnode;	
          qnode = qnode->next;
          free(this_node);
     }

     /* free queue */
     free(qp);
}


/***** 
 * adds new node to the queue with pointer to data
 * allocates memory for this node
 * returns pointer to node or NULL on error
 *****/
static inline q_node_t* queue_add(nhqueue_t *qp, void *data)
{
     q_node_t *new_node = NULL;

     if(qp == NULL) {
          return new_node;
     }

     if (qp->freeq) {
          new_node = qp->freeq;
          qp->freeq = new_node->next;
          memset(new_node, 0, sizeof(q_node_t));
     }
     else {
          /* allocate memory and initialize */
          new_node = (q_node_t*)calloc(1, sizeof(q_node_t));
          if (!new_node) {
               error_print("failed queue_add calloc of new_node");
               return NULL;
          }
     }
     new_node->data = data;

     /* maintain queue */
     qp->size++;

     if(qp->head == NULL) {
          qp->head = new_node;
          qp->tail = new_node;
     }
     else {
          qp->tail->next = new_node;
          new_node->prev = qp->tail;
          qp->tail = new_node;
     }

     return new_node;
}

/***** 
 * removes node from the queue
 * frees memory allocated for this node
 * returns pointer to node data or NULL on error
 *****/
static inline void* queue_remove(nhqueue_t *qp)
{
     void     *retval  = NULL;
     q_node_t *rm_node = NULL;

     if((qp == NULL) || (qp->head == NULL)) {
          return NULL;
     }

     /* check for head == NULL? */
     retval = qp->head->data;

     /* maintain queue */
     qp->size--;

     rm_node = qp->head;
     qp->head = qp->head->next;

     if(qp->head != NULL) {
          qp->head->prev = NULL;
     }
     else {
          qp->tail = NULL;
     }

     /* free memory to list of freeq available*/
     rm_node->next = qp->freeq;
     rm_node->data = NULL;
     qp->freeq = rm_node;

     return retval;
}

static inline unsigned int queue_size(nhqueue_t *qp)
{
     if(qp == NULL) {
          return 0;
     }

     return qp->size;
}

/***** 
 * moves node to head of queue
 * returns 0 on success or -1 on error
 *****/
static inline int queue_move2head(nhqueue_t *qp, q_node_t *node)
{
     if(qp == NULL || node == NULL) {
          return -1;
     }

     /* no need to do anything if node already at head */
     if(node == qp->head) {
          return 0;
     }

     /* extract node from queue */
     if(queue_extract(qp, node) < 0) {
          return -1;	
     }

     /* insert node at head */
     node->next = qp->head;
     node->prev = NULL;
     qp->head->prev = node;
     qp->head = node;

     qp->size++;

     return 0;
}

/***** 
 * moves node to tail of queue
 * returns 0 on success or -1 on error
 *****/
static inline int queue_move2tail(nhqueue_t *qp, q_node_t *node)
{
     if((qp == NULL) || (qp->tail == NULL) || (node == NULL)) {
          return -1;
     }

     /* no need to do anything if node already at tail */
     if(node == qp->tail) {
          return 0;
     }

     /* extract node from queue */
     if(queue_extract(qp, node) < 0) {
          return -1;	
     }

     /* insert node at tail */
     node->prev = qp->tail;
     node->next = NULL;
     qp->tail->next = node;
     qp->tail = node;

     qp->size++;

     return 0;
}


/***** 
 * removes node from queue
 * does free the memory allocated for this node
 * returns 0 on success or -1 on error
 *****/
static inline int queue_remove_midpoint(nhqueue_t *qp, q_node_t *node) {
     int rtn;
     rtn = queue_extract(qp, node);

     if (rtn) {
          return rtn;
     }

     /* free memory to list of freeq available*/
     node->next = qp->freeq;
     qp->freeq = node;

     return rtn;
}


/***** 
 * removes node from queue
 * does NOT free the memory allocated for this node
 * returns 0 on success or -1 on error
 *****/
static inline int queue_extract(nhqueue_t *qp, q_node_t *node)
{
     if((qp == NULL) || (qp->head == NULL) || (qp->tail == NULL) || 
        (node == NULL)) {
          return -1;
     }

     /* adjust head pointer */
     if(node == qp->head) {
          qp->head = qp->head->next;
     }

     /* adjust tail pointer */
     if(node == qp->tail) {
          qp->tail = qp->tail->prev;
     }

     /* adjust previous & next pointers in queue */
     if(node->prev != NULL) {
          node->prev->next = node->next;
     }

     if(node->next != NULL) {
          node->next->prev = node->prev;
     }

     qp->size--;

     return 0;
}

/***** 
 * returns pointer to data at specified location on success or NULL on error
 *****/
static inline void* queue_get_at(nhqueue_t *qp, unsigned int loc)
{
     if( (qp == NULL) || (qp->head == NULL) || (qp->tail == NULL) || 
         (loc >= qp->size) ) {
          return NULL;
     }

     /* adjust head pointer */
     if(loc == 0) {
          return qp->head->data;
     }

     /* adjust tail pointer */
     if(loc == qp->size-1) {
          return qp->tail->data;
     }

     uint32_t i;
     q_node_t * retnode = qp->head;
     for(i=0; i < loc; i++) {
          retnode = retnode->next;
     }

     return retnode->data;
}

static inline int queue_clear(nhqueue_t *qp)
{
     q_node_t *qnode, *this_node;

     if(qp == NULL) {
          return -1;
     }

     /* place all nodes in freeq */
     qnode = qp->head;
     while(qnode != NULL) {
          this_node = qnode;	
          qnode = qnode->next;
          this_node->next = qp->freeq;
          qp->freeq = this_node;
     }

     qp->size = 0;
     qp->head = NULL;
     qp->tail = NULL;

     return 0;
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WSQUEUE_H
