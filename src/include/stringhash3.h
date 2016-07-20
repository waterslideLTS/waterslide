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
/**
     @file stringhash3.h


     \date 2005

     \brief Fixed length string hash data structure

     String hash table library that can expire upon table filling.
 */

#ifndef _STRINGHASH3_H
#define _STRINGHASH3_H

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include "sht_registry.h"
#include "error_print.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus


typedef struct _stringhash3_t {
     uint8_t *  str;
     void *    data;
     struct _stringhash3_t * next;    //hash table link list
     struct _stringhash3_t * prev;
     struct _stringhash3_t * sf_next;  //sort table link list
     struct _stringhash3_t * sf_prev;
} stringhash3_t;

typedef int (*sh3_callback)(stringhash3_t *, void * /*extra data*/);

typedef struct _stringhashtable3_t {
     stringhash3_t **     table;
     stringhash3_t * sf_head;
     stringhash3_t * sf_tail;

     stringhash3_t * free_q; // queue of free nodes in table
     
     int               strlen;
     int               records;
     int               max_records;
     int               table_depth; //index size
     int               data_alloc;  //size of auxillary data for each flow rec
     sh3_callback      destroy_func;
     void *            callback_data;

     int               hash_bytes;          //number of bytes for index..
     uint64_t          drops;
} stringhashtable3_t;


static inline stringhashtable3_t * stringhash3_create_table(int /*stringlen*/,
					      int /*max_records*/,
					      int /*data_alloc*/,
					      sh3_callback /*destroy_func*/,
					      void * /*callback_data*/);

static inline stringhash3_t * stringhash3_find_attach(stringhashtable3_t *, uint8_t *);
static inline stringhash3_t * stringhash3_find(stringhashtable3_t *, uint8_t *);

/** perform function on every element of table */
static inline void stringhash3_scour(stringhashtable3_t *, sh3_callback, void *);

/** destroy all records of table */
static inline int stringhash3_flush(stringhashtable3_t * /*ftable*/);

/** prune one record from hashtable */
static inline void stringhash3_prune(stringhashtable3_t *, stringhash3_t *);

static inline void stringhash3_update_sort(stringhashtable3_t *, stringhash3_t *);

/** free all memory in the table */
static inline void stringhash3_free(stringhashtable3_t * /*ftable*/);

/** get the expiration count for this hash table */
static inline uint64_t stringhash3_drop_cnt(stringhashtable3_t *);


static inline uint32_t compute_hash3(stringhashtable3_t * sht, uint8_t * str) {
     uint32_t hash = 0;
     int i;
     int j;

     for (i = 3; i < sht->strlen; i+=4) {
	  hash ^= ((uint32_t)str[i]) | ((uint32_t)str[i-1] << 8) | 
	       ((uint32_t)str[i-2] << 16) | ((uint32_t) str[i-3] << 24) ;
     }
     if (i >= sht->strlen) {
	  i -= 3;
	  for (j = i; j < sht->strlen; j++) {
	       hash ^= (uint32_t) str[j] << (j & 0x03);
	  }
     }

     switch(sht->hash_bytes) {
     case 1:
	  hash = (hash >> 24) ^ (hash >> 16) ^ (hash >> 8) ^ hash;
     case 2:
	  hash = (hash >> 16) ^ hash;
     case 3:
	  hash = (hash >> 24) ^ hash;
     }

     return (hash % sht->table_depth);
}

/* create a new table of string records */
static inline stringhashtable3_t * stringhash3_create_table(int strlen,
					      int max_records,
					      int data_alloc,
					      sh3_callback destroy_func,
					      void * callback_data) {

     stringhashtable3_t * sht;

     sht = (stringhashtable3_t *) malloc(sizeof(stringhashtable3_t));
     if (!sht) {
          error_print("failed malloc of stringhash3 table");
          return NULL;
     }
     memset(sht,0,sizeof(stringhashtable3_t));
//fprintf(stderr, "stringhash3_create: CREATE sht = %x\n", sht);

     sht->strlen = strlen;
     sht->table_depth = max_records;
     sht->max_records = max_records;
     sht->data_alloc = data_alloc;
     sht->destroy_func = destroy_func;
     sht->callback_data = callback_data;

     if (sht->table_depth < 255) {
	  sht->hash_bytes = 1;
     }
     else if (sht->table_depth < 65535) {
	  sht->hash_bytes = 2;
     }
     else if (sht->table_depth < ((65536 * 256) - 1)) {
	  sht->hash_bytes = 3;
     }

     sht->table = (stringhash3_t**) malloc(sizeof(stringhash3_t *) * 
                                           sht->table_depth);
     if (!sht->table) {
          error_print("failed malloc of stringhash3 sht->table");
          return NULL;
     }

//fprintf(stderr, "stringhash3_create: CREATE sht->table = %x\n", sht->table);
     memset(sht->table,0,sizeof(stringhash3_t *) * sht->table_depth);

     if (!enroll_in_sht_registry(sht, "sh3", sizeof(stringhashtable3_t) +
                                 sizeof(stringhash3_t *)*sht->table_depth +
                                 (sizeof(stringhash3_t *)+sht->strlen+sht->data_alloc)*
                                  sht->max_records, 0)) {
          return NULL;
     }

     return sht;
}

static inline uint64_t stringhash3_drop_cnt(stringhashtable3_t * sht) {
     return sht->drops;
}


//move cursor to tail of sortflow list....
static inline void stringhash3_update_sort(stringhashtable3_t * sht, 
			     stringhash3_t * cursor) {
     if (cursor->sf_next == NULL) {
	  return; //we are already at tail of list..
     }

     //we are in middle of list..
     if (cursor->sf_prev) {
	  //detach from list..
	  cursor->sf_prev->sf_next = cursor->sf_next;
	  cursor->sf_next->sf_prev = cursor->sf_prev;
     }
     //we are at head of list
     else {
	  cursor->sf_next->sf_prev = NULL;
	  sht->sf_head = cursor->sf_next;
     }

     cursor->sf_next = NULL;
     //attach to tail of list.
     sht->sf_tail->sf_next = cursor;
     cursor->sf_prev = sht->sf_tail;
     sht->sf_tail = cursor;
}

/* find a particular string record based on a reference item */
static inline stringhash3_t * stringhash3_find(stringhashtable3_t * sht, uint8_t * pstring) {
     uint32_t hash;
     stringhash3_t * cursor;

     if (!sht) {
	  return NULL;
     }

     hash = compute_hash3(sht, pstring);

     for (cursor = sht->table[hash]; cursor; cursor = cursor->next) {
	   if (memcmp(cursor->str, pstring, sht->strlen) == 0) {
	       return cursor;
	  }
     }
     return NULL;

}

static inline void destroy_stringhash3_record(stringhashtable3_t *sht,
                                       stringhash3_t *cursor) {
     if (sht->destroy_func) {
	  sht->destroy_func(cursor, sht->callback_data);
     }

     //reclaim data
     cursor->sf_next = sht->free_q;
     sht->free_q = cursor;
     
     /*if (cursor->data) {
	  free(cursor->data);
     }

     free(cursor->str);
     free(cursor);*/
}

static inline void stringhash3_scour(stringhashtable3_t * sht,
		       sh3_callback cfunc, void * vdata) {
     stringhash3_t *  cursor;
     stringhash3_t *  ncursor;

     if (!cfunc) {
	  return;
     }

     for (cursor = sht->sf_head; cursor; cursor = ncursor) {
          ncursor = cursor->sf_next;
          //allow cursor to be destroyed/pruned...
          cfunc(cursor, vdata); 
     }
}

/* scour entire list of string records..
   if callback returns 1, delete that record
*/
static inline int stringhash3_flush(stringhashtable3_t * sht) {
     stringhash3_t *  cursor;
     stringhash3_t *  next;
     int pruned = 0;
     
     cursor = sht->sf_head;

     while(cursor) {
	  next = cursor->sf_next;
	  destroy_stringhash3_record(sht, cursor);
	  pruned++;
	  cursor = next;
     }

     sht->sf_head = NULL;
     sht->sf_tail = NULL;

     sht->records = 0;

     memset(sht->table,0,sizeof(stringhash3_t *) * sht->table_depth);

     return pruned;
}

//remove cursor from hashtable..
static inline void sht3_tableremove(stringhashtable3_t * sht, stringhash3_t * cursor) {
     uint32_t hash;
     if (cursor->prev == NULL) {
	  //lookup hash index
	  hash = compute_hash3(sht, cursor->str);

	  sht->table[hash] = cursor->next;

	  if (cursor->next) {
	       cursor->next->prev = NULL;
	  }
     }
     else {
	  cursor->prev->next = cursor->next;
	  if (cursor->next) {
	       cursor->next->prev = cursor->prev;
	  }
     }
}

static inline void stringhash3_prune(stringhashtable3_t * sht, stringhash3_t * cursor) {

     if (!cursor) {
	  return;
     }

     //remove flwo from hash table...
     sht3_tableremove(sht, cursor);

     if (sht->sf_head == cursor) {
	  sht->sf_head = cursor->sf_next;
     }

     if (sht->sf_tail == cursor) {
	  sht->sf_tail = cursor->sf_prev;
     }

     if (cursor->sf_next) {
	  cursor->sf_next->sf_prev = cursor->sf_prev;
     }

     if (cursor->sf_prev) {
	  cursor->sf_prev->sf_next = cursor->sf_next;
     }


     sht->records--;
     
     destroy_stringhash3_record(sht,cursor);
     
}

/* find string record based on reference item.. if string record does not
 * exist, create it
 */
static inline stringhash3_t * stringhash3_find_attach(stringhashtable3_t * sht,
					uint8_t * pstring) {
     uint32_t hash;
     stringhash3_t * cursor;
     uint8_t * str;
     void * data;
     int reinit_cursor = 0;
     
     if (!sht) {
	  return NULL;
     }

     cursor = stringhash3_find(sht, pstring);
     if (cursor != NULL) {
	  return cursor;
     }

     //attach - new record....

     //see if we have reached table limit prune off record
     if (sht->max_records && 
	 (sht->records >= sht->max_records) &&
	 sht->sf_head) {

	  cursor = sht->sf_head;
	  
	  //remove record from hash table...
	  sht3_tableremove(sht, cursor);
       if (sht->destroy_func) {
	    sht->destroy_func(cursor, sht->callback_data);
       }
	  
	  //remove from sort table:
	  sht->sf_head = cursor->sf_next;
	  cursor->sf_next->sf_prev = NULL;
	  
	  sht->records--;
          sht->drops++;

	  //remove and reuse oldest record
       reinit_cursor = 1;
	  
     }
     //reuse record in the free_q
     else if (sht->free_q) {
          cursor = sht->free_q;
          sht->free_q = cursor->sf_next;
          reinit_cursor = 1;
     }
     //create new record
     else {
	  cursor = (stringhash3_t *)malloc(sizeof(stringhash3_t));
          if (!cursor) {
               error_print("failed malloc of stringhash3 cursor");
               return NULL;
          }
	  memset(cursor,0,sizeof(stringhash3_t));
     
	  cursor->str = (uint8_t *)malloc(sht->strlen);
          if (!cursor->str) {
               error_print("failed malloc of stringhash3 cursor->str");
               return NULL;
          }
	  memcpy(cursor->str, pstring, sht->strlen);
				           
	  if (sht->data_alloc) {
	       cursor->data = (void *) malloc(sht->data_alloc);
               if (!cursor->data) {
                    error_print("failed malloc of stringhash3 cursor->data");
                    return NULL;
               }
	       memset(cursor->data, 0, sht->data_alloc);
	  }
     }

     if (reinit_cursor) {
	  str = cursor->str;
	  data = cursor->data;

	  memset(cursor, 0, sizeof(stringhash3_t));

	  cursor->str = str;
	  cursor->data = data;

	  memcpy(str, pstring, sht->strlen);

	  if (sht->data_alloc) {
	       memset(data, 0, sht->data_alloc);
	  }
     }

     //attach record to hash table and priority queue

     hash = compute_hash3(sht, pstring);

     //attach new record to head of table link list
     if (sht->table[hash]) {
	  cursor->next = sht->table[hash];
	  cursor->next->prev = cursor;
     }
     sht->table[hash] = cursor;

     //add cursor to list of sorted strings..
     if (sht->sf_tail) {
	  sht->sf_tail->sf_next = cursor;
	  cursor->sf_prev = sht->sf_tail;
	  sht->sf_tail = cursor;
     }
     else {
	  sht->sf_tail = cursor;
	  sht->sf_head = cursor;
     }

     sht->records++;

     return cursor;
}

static inline void stringhash3_free(stringhashtable3_t * sht) {
     stringhash3_t *  cursor;
     stringhash3_t *  next;
     int              i;

     if (sht && sht->table) {
     
        for (i = 0; i < sht->table_depth; i++) {

            cursor = sht->table[i];
            while (cursor) {
                next = cursor->next;
                if (sht->destroy_func) {
	                sht->destroy_func(cursor, sht->callback_data);
                }
                if (cursor->data) {
	                free(cursor->data);
                }
                if (cursor->str) {
                    free(cursor->str);
                }
                free(cursor);
                cursor = next;
            }
        }

        // free free list
        cursor = sht->free_q;
        while (cursor) {
          next = cursor->next;
          free(cursor);
          cursor = next;
        }

        free(sht->table);

        free(sht);
    }
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _STRINGHASH3_H
