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

/*
   This is an approximate counting algorithm

   This work is based on the Space Saving Algorithm in
   "Efficient Computation of Frequent and Top-k Elements in Data Streams"
   by Ahmed Metwally, Divyakant Agrawal, and Amr El Abbadi

   and informed by analysis in:
   "Methods for Finding Frequent Items in Data Streams"
   by Graham Cormode, Marios Hadjieleftheriou
   */


#ifndef _HEAVYHITTERS_H
#define _HEAVYHITTERS_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "error_print.h"
#include "dprint.h"
#include "evahash64.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

/* main structures are:
   1. priority queue implemented as doubly-linked list stack
   2. hashtable-list that is accessed sorted

   probably better implemented as a min-heap for priority queue and sorting upon
   output
 */
typedef struct _hh_node_t
{
     uint64_t         key;
     uint64_t         value;
     uint64_t         starting_value;
     struct _hh_node_t * q_prev;
     struct _hh_node_t * q_next;
     struct _hh_node_t * ht_prev;
     struct _hh_node_t * ht_next;
     uint8_t           data[];
} hh_node_t;

typedef void (* heavyhitters_callback)(void * /*data*/);

typedef struct _heavyhitters_t
{
     uint64_t     max;
     uint64_t     count;
     size_t       data_size;
     size_t       node_data_size;
     void *       buffer;
     hh_node_t **  hashtable;
     hh_node_t *  q_head;
     hh_node_t *  q_tail;
     heavyhitters_callback release;
     uint32_t     seed;
} heavyhitters_t;

/* allocates & initializes queue memory */
static inline heavyhitters_t* heavyhitters_init(uint64_t max, size_t data_size,
                                                heavyhitters_callback release)
{
     heavyhitters_t * hh = (heavyhitters_t*)calloc(1, sizeof(heavyhitters_t));

     hh->max = max;
     hh->data_size = data_size;
     hh->node_data_size = sizeof(hh_node_t) + data_size;
     hh->buffer = (void *)calloc(max, hh->node_data_size);
     if (!hh->buffer) {
          free(hh);
          error_print("unable to allocate heavy hitters buffer");
          return NULL;
     }
     hh->hashtable = (hh_node_t**)calloc(max, sizeof(hh_node_t*));
     if (!hh->hashtable) {
          free(hh->buffer);
          free(hh);
          error_print("unable to allocate heavy hitters hashtable");
          return NULL;
     }

     hh->release = release;
     hh->seed = rand();

     return hh;
}

static inline int hh_release_all(heavyhitters_t * hh) {
     if (!hh) {
          return 0;
     }
     //do callback to release old data
     if (hh->count && hh->release) {
          uint64_t i;
          for (i = 0; i < hh->count; i++) {
               void * pos = hh->buffer + i * hh->node_data_size;
               void * data = pos + sizeof(hh_node_t);
               hh->release(data);
          }
     }
     return 1;

}

// reset data structures back to empty
static inline int heavyhitters_reset(heavyhitters_t * hh) {
     dprint("reset");
     if (!hh) {
          return 0;
     }
     hh_release_all(hh);
     if (hh->hashtable) {
          memset(hh->hashtable, 0, sizeof(hh_node_t*) * hh->max);
     }
     if (hh->buffer) {
          memset(hh->buffer, 0, hh->max * hh->node_data_size);
     }
     hh->count = 0;
     hh->q_head = NULL;
     hh->q_tail = NULL;

     return 1;
}

// destroy all data structures
static inline int heavyhitters_destroy(heavyhitters_t * hh) {
     dprint("destroy");
     if (!hh) {
          return 0;
     }
     hh_release_all(hh);
     if (hh->hashtable) {
          free(hh->hashtable);
     }
     if (hh->buffer) {
          free(hh->buffer);
     }

     free(hh);

     return 1;
}

//remove node from list to be placed elsewhere
static inline void hh_q_release(heavyhitters_t * hh, hh_node_t *node) {
     dprint("q_release");

     if (hh->q_head == node) {
          hh->q_head = node->q_next;
     }
     if (hh->q_tail == node) {
          hh->q_tail = node->q_prev;
     }
     if (node->q_prev) {
          node->q_prev->q_next = node->q_next;
     }
     if (node->q_next) {
          node->q_next->q_prev = node->q_prev;
     }

     node->q_prev = NULL;
     node->q_next = NULL;
}

//insert node after cursor
static inline void hh_q_insert_after(heavyhitters_t * hh,
                                     hh_node_t *cursor,
                                     hh_node_t * node) {
     dprint("q_insert_after");
     hh_node_t * next = cursor->q_next;
     cursor->q_next = node;
     node->q_prev = cursor;
     node->q_next = next;
     if (next) {
          next->q_prev = node;
     }
     else {
          hh->q_tail = node;
     }
}

//maintain priority queue
static inline void hh_queue_sort(heavyhitters_t * hh, hh_node_t * node) {
     dprint("q_sort");
     if (!node->q_prev || (node->q_prev->value >= node->value)) {
          //nothing to be done
          return;
     }
     //release node from list...
     hh_node_t * prev = node->q_prev;
     hh_q_release(hh, node); 

     //find place to insert
     hh_node_t * cursor;
     for (cursor = prev->q_prev; cursor; cursor = cursor->q_prev) {
          if (cursor->value >= node->value) {
               hh_q_insert_after(hh, cursor, node);
               return;
          }
     }
     //insert at head
     hh_node_t * next = hh->q_head;
     hh->q_head = node;
     node->q_next = next;
     if (next) {
          next->q_prev = node;
     }
     else {
          hh->q_tail = node;
     }
} 

static inline void hh_hashtable_detach(heavyhitters_t * hh, hh_node_t * node) {
     dprint("ht_detach");
     uint64_t hindex = node->key % hh->max;

     if (hh->hashtable[hindex] == node) {
          hh->hashtable[hindex] = node->ht_next;
     }
     if (node->ht_prev) {
          node->ht_prev->ht_next = node->ht_next;
     }
     if (node->ht_next) {
          node->ht_next->ht_prev = node->ht_prev;
     }

     node->ht_prev = NULL;
     node->ht_next = NULL;
}

static inline void hh_hashtable_attach(heavyhitters_t * hh, hh_node_t * node,
                                       uint64_t hindex) {
     dprint("ht_attach");
     if (!hh->hashtable[hindex]) {
          hh->hashtable[hindex] = node;
          dprint("node %"PRIx64" hashtable %"PRIx64, (uint64_t)node, (uint64_t)hh->hashtable[hindex]);
     }
     else {
          hh->hashtable[hindex]->ht_prev = node;
          node->ht_next = hh->hashtable[hindex];
          hh->hashtable[hindex] = node;
     }
}

//returns data at increment position
static inline hh_node_t * heavyhitters_increment(heavyhitters_t * hh,
                                                 const void * key_data,
                                                 size_t key_length,
                                                 uint64_t value) {
     dprint("hh_increment");
     //hash key data
     uint64_t key = evahash64((uint8_t *)key_data, key_length, hh->seed);

     //look up key in hashtable
     uint64_t hindex = key % hh->max;
     dprint("key %"PRIx64" hindex %"PRIu64, key, hindex);
     
     hh_node_t * node = NULL;
     hh_node_t * cursor = hh->hashtable[hindex];

     dprint("find key in hashtable");
     for (;cursor; cursor = cursor->ht_next) {
          dprint("table %"PRIx64", cursor %"PRIx64", next %"PRIx64", buffer %"PRIx64,
                 (uint64_t)hh->hashtable[hindex], (uint64_t)cursor,
                 (uint64_t)cursor->ht_next, (uint64_t)hh->buffer);
          if (cursor->key == key) {
               dprint("found key at cursor%"PRIx64, (uint64_t)cursor);
               //found key
               cursor->value += value;
               hh_queue_sort(hh, cursor);

               return cursor;
          }
     }

     //key not found, check if space left
     dprint("check space");
     if (hh->count < hh->max) {
          //add new data position
          node = hh->buffer + (hh->count * hh->node_data_size);
          hh->count++;

          node->key = key;
          node->value = value;

          //attach node to hashtable...
          hh_hashtable_attach(hh, node, hindex);
     
          //attach node to priority queue
          if (!hh->q_head) {
               hh->q_head = node;
               hh->q_tail = node;
          }
          else {
               //attach to tail..
               hh->q_tail->q_next = node;
               node->q_prev = hh->q_tail;
               hh->q_tail = node;
               hh_queue_sort(hh, node);
          }

     }
     else {
          dprint("recycle");
          //recycle smallest record in table position
          assert(hh->q_tail != NULL);
          node = hh->q_tail;

          //do callback to release old data
          if (hh->release) {
               hh->release((void *)node->data);
          }
          //reset data buffer
          memset(node->data, 0, sizeof(hh->data_size));

          hh_hashtable_detach(hh, node);
           
          node->key = key;
          hh_hashtable_attach(hh, node, hindex);
          
          //set old value
          node->starting_value = node->value;
          node->value += value;

          //sort queue
          hh_queue_sort(hh, node);
     }
     return node;
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _HEAVYHITTERS_H
