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

//STRINGHASH5
// Created: July 2009
// Purpose: for storing data for as long as possible based on a key using
// a localized least recently used expiration mechanism
//
// What it is: expiring hash table library that is designed for fast lookup
// and fair expiration using a localized least-recently-used scheme.
// Every index is assigned to a bucket of records.  There are 16 items per
// bucket.  The items are sorted least recently used.
//
// This table stores static sized user data that is allocated at initialization.
// It uses a single hash function for indexing and a 28bit digest for matching
// in the table.  
//
//  LRU = least recently used. In this hashtable it means the oldest record
//        in a given index'd bucket.
//
//
// Key Interface Functions:
//
// stringhash5_t * stringhash5_create(uint32_t is_shared, uint64_t max_records, 
//                                    uint32_t data_alloc);
//   Creates a hashtable, allocates memory for records up to the closest
//   power of 2 of max_records. Allocates data_alloc memory for each record.
//   returns pointer to the table that is used in all other calls to the table
//
// void * stringhash5_find(stringhash5_t * sht, uint8_t * key, int keylen);
//   Locates a record at a given key.  Returns a pointer to the data alloc'd to
//   this record or NULL if not found.  The LRU for a given table index'd bucket
//   is adjusted/sorted to mark searched record as most recent.
//
// void * stringhash5_find_attach(stringhash5_t * sht, uint8_t * key, int keylen);
//   Locates a record at a given key.  Returns a pointer to the data alloc'd to
//   this record.  If record is not found, it will allocate a new record and
//   return the data_alloc structure zeroed out.  If the table is full, it will
//   find a least recently used record, zero it out and reuse it for the new
//   record.   The LRU for a given table index'd bucket is adjusted/sorted to
//   mark searched record as most recent.  If a callback function is assigned,
//   using stringhash5_set_callback, then prior to records being deleted/reused
//   the callback will be called.
//
// void * stringhash5_delete(stringhash5_t * sht, uint8_t * key, int keylen);
//   Deletes a record at a given key.  Marks this deleted record as least
//   recently used.
//
// void stringhash5_scour(stringhash5_t * sht, stringhash5_callback cb, void * vproc);
//   Walks through the table and calls the callback function for every record
//   found.
//
// void * stringhash5_flush(stringhash5_t * sht);
//   Zeros out entire table.
//
// void * stringhash5_destroy(stringhash5_t * sht);
//   Destroys and frees memory for entire table.
//
// void stringhash5_set_callback(stringhash5_t * sht, stringhash5_callback cb,
//                              void * vproc);
//   Sets a callback function for code
//
// Example code:
//
// typedef struct _my_data_t {
//    uint64_t value;
// } my_data_t;
//
// stringhash5_t * mytable;
// mytable = stringhash5_create(0, 10000, sizeof(my_data_t));
//
// my_data_t * some_data;
//
// some_data = stringhash5_find_attach(mytable, "a key", 5);
// some_data->value = 4; //store value at key 'a key'
//
// some_data = stringhash5_find_attach(mytable, "b key", 5);
// some_data->value = 3;
//
// some_data = stringhash5_find(mytable, "a key", 5);
// if (some_data) print("value at 'a key' is %lu", some_data->value);
//
// stringhash5_delete(mytable); 
// //end example code
//
//
//
// Implementation Details:
//   The structure has 2 tables, a bucket table for storing digests and a data
//   table. Each of the 16 items in a bucket is mapped to 16 items in the data table
//   An item in a bucket is 32 bits, 27 of which is used for digest, 4 bits
//   used to point to the corresponding data offset in the data table, and the
//   last bit is used to construct a 16bit epoch time clock.
//   Items in the bucket are sorted upon access(find or find attach) to create
//   an LRU sorting of items.
//
//  This implementation utilizes a multi-way hashtable with epoch based
//  multi-way LRU selection by stealing a bit from each potential digest
//
//
#ifndef _STRINGHASH5_H
#define _STRINGHASH5_H

//#define DEBUG 1
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include "evahash64.h"
#include "sysutil.h"
#include "sht_registry.h"
#include "tool_print.h"
#include "error_print.h"
#include "shared/getrank.h"
#include "shared/lock_init.h"
#include "shared/sht_lock_init.h"
#include "shared/kidshare.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

//macros
#ifndef SHT_ID_SIZE
#define SHT_ID_SIZE 13
#endif // SHT_ID_SIZE
#define SHT5_ID   "STRINGHASH5 "
#define SHT9A_ID  "STRINGHASH9A"
#define SH5_DEPTH 16 //should be matched with cache line size..
#define SH5_DEPTH_BITS 4 //log2 of depth

#define SH5_DIGEST_MASK      0x07FFFFFFU
#define SH5_ANTI_DIGEST_MASK 0xF8000000U
#define SH5_DIGEST_DEFAULT   0x00001000U
#define SH5_EPOCH_MASK       0x08000000U
#define SH5_ANTI_EPOCH_MASK  0xF7FFFFFFU
#define SH5_EPOCH_SHIFT      27
#define SH5_DIGEST_BITS      27

#define SH5_DATA_BIN         28
#define SH5_DATA_BIN_MASK    0xF0000000U

//macros internal to stringhash5
#define SET_NRANK const int nrank = GETRANK();
#define CURRENT_LOCK_VALUE(sht,h) (sht)->curr_lock[GETRANK()] = (h) >> (sht)->mutex_index_shift; 
#define SH5_SHIFT_KEY(h1,k1) uint32_t k1 = (h1) >> (sht)->mutex_index_shift;
#define SH5_LOCK(sht,h) SHT_LOCK(&((sht)->mutex[(h) >> (sht)->mutex_index_shift]))
#define SH5_UNLOCK(sht,h) SHT_UNLOCK(&((sht)->mutex[(h) >> (sht)->mutex_index_shift]))
#define SH5_J_LOCK(sht,j) SHT_LOCK(&((sht)->mutex[(j)]))
#define SH5_J_UNLOCK(sht,j) SHT_UNLOCK(&((sht)->mutex[(j)]))
#define SH5_LOCK_PAIR(sht,k1,k2,pairflag) \
     pairflag = 1; \
     if ((k1) < (k2)) { \
          SHT_LOCK(&((sht)->mutex[k1])) \
          SHT_LOCK(&((sht)->mutex[k2])) \
     } \
     else if ((k1) > (k2)) { \
          SHT_LOCK(&((sht)->mutex[k2])) \
          SHT_LOCK(&((sht)->mutex[k1])) \
     } \
     else { \
          SHT_LOCK(&((sht)->mutex[k1])) \
          pairflag = 0; \
     }
#define SH5_ALL_LOCK(sht) \
{ \
      uint32_t j; \
      for (j = 0; j < (sht)->max_mutex; j++) { \
           SH5_J_LOCK(sht,j) \
      } \
}
#define SH5_ALL_UNLOCK(sht) \
{ \
      uint32_t j; \
      for (j = 0; j < (sht)->max_mutex; j++) { \
           SH5_J_UNLOCK(sht,j) \
      } \
}

#ifdef WS_PTHREADS
#ifdef WS_LOCK_DBG
#define sh5_mutex_t pthread_mutex_t
#else
#define sh5_mutex_t pthread_spinlock_t
#endif // WS_LOCK_DBG

//dummy "mutex" type for serial
#else
#define sh5_mutex_t int
#endif // WS_PTHREADS

typedef uint32_t sh5_digest_t;

// typedefs for callback functions
typedef void (*sh_callback_t)(void *, void *);
typedef uint32_t (*sh5datadump_callback_t)(void *, uint32_t, uint32_t, FILE *);
typedef uint32_t (*sh5dataread_callback_t)(void *, uint32_t, FILE *);

//callback functions
typedef void (*stringhash5_callback)(void * /*data*/, void * /*calldata*/);
typedef uint32_t (*sh5_datadump_cb)(void * /*data*/, uint32_t /*max_records*/, 
                                    uint32_t /*data_alloc*/, FILE * /*fp*/);
typedef uint32_t (*sh5_dataread_cb)(void * /*data*/, uint32_t /*data_alloc*/, 
                                    FILE * /*fp*/);
typedef int (*stringhash5_visit)(void * /*data*/, void * /*calldata*/);

typedef struct _share5_t {
     int cnt;
     void * table;
} share5_t;

//28 bits of digest, 4 bits of pointer to data
//items in list sorted LRU
typedef struct _sh5_bucket_t {
     sh5_digest_t digest[SH5_DEPTH];
} sh5_bucket_t;

typedef struct _stringhash5_sh_opts_t {
     sh_callback_t sh_callback;
     void * proc; 
     int readonly;
     const char * open_table;
     int read_n_scour;
     sh_callback_t sh_scour; 
     sh5dataread_callback_t sh5_dataread_cb;
} stringhash5_sh_opts_t;

typedef struct _stringhash5_t {
     sh5_bucket_t * buckets;
     uint8_t * data;
     size_t data_alloc;
     size_t max_records;
     uint64_t mem_used;
     uint32_t index_size;
     uint32_t all_index_size;
     uint32_t hash_seed;
     stringhash5_callback callback;
     void ** cb_vproc;
     uint8_t epoch;
     uint32_t einserts;
     uint32_t einsert_max;
     uint64_t mask_index;
     uint64_t drops;
     uint32_t read_success;
     struct _stringhash5_walker_t ** walkers;
     uint32_t num_walkers;
     uint32_t * walker_row;
     uint64_t nextval;

     int is_shared;
     char * sharelabel;
     share5_t * sharedata;
     void * v_type_table;
     uint32_t max_mutex;
     uint32_t mutex_index_shift;
     sh5_mutex_t * mutex;
     uint32_t * curr_lock;
     sh5_mutex_t masterlock;
#ifdef WS_LOCK_DBG
     WS_MUTEX_DECL(walker_mutex);
#else
     WS_SPINLOCK_DECL(walker_mutex);
#endif // WS_LOCK_DBG
} stringhash5_t;

typedef struct _stringhash5_walker_t {
     struct _stringhash5_t * sht;
     uint64_t loop;
     stringhash5_visit callback;
     void * cb_vproc;
     uint32_t walker_id;
} stringhash5_walker_t;

// Globals
extern uint32_t work_size;

//prototypes
static inline void stringhash5_set_callback(stringhash5_t *, stringhash5_callback, void *);
//DEPRECATED
static inline int stringhash5_open_table(stringhash5_t **, void *, const char *, uint64_t *, 
                                            int, sh_callback_t);
//DEPRECATED
static inline int stringhash5_open_table_with_ptrs(stringhash5_t **, void *, const char *, 
                                            uint64_t *, int, sh_callback_t, 
                                            sh5dataread_callback_t);
//CURRENT
static inline int stringhash5_open_sht_table(stringhash5_t **, void *, uint64_t, uint32_t, 
                                            stringhash5_sh_opts_t *);
static inline stringhash5_t * stringhash5_create(uint32_t, uint64_t, uint32_t);
static inline void stringhash5_clean_data_field(stringhash5_t *);
static inline uint32_t check_sh5_max_records(uint64_t);
//DEPRECATED
static inline int stringhash5_create_shared(void *, void **, const char *, uint32_t, uint32_t, 
                                            int *, sh_callback_t, void *, int, void *, uint64_t *, 
                                            int, sh_callback_t);
//DEPRECATED
static inline int stringhash5_create_shared_with_ptrs(void *, void **, const char *, uint32_t, uint32_t, 
                                            int *, sh_callback_t, void *, int, void *, uint64_t *, 
                                            int, sh_callback_t, sh5dataread_callback_t);
//CURRENT
static inline int stringhash5_create_shared_sht(void *, void **, const char *, uint32_t, uint32_t, 
                                            int *, stringhash5_sh_opts_t *);
static inline void stringhash5_unlock(stringhash5_t *);
static inline void stringhash5_masterlock_lock(stringhash5_t *);
static inline void stringhash5_masterlock_unlock(stringhash5_t *);
static inline void * stringhash5_find(stringhash5_t *, void *, int);
static inline void * stringhash5_jump_to_entry(stringhash5_t *, uint32_t, uint32_t);
static inline void stringhash5_mark_as_used(stringhash5_t *, uint32_t, uint32_t);
static inline uint64_t stringhash5_drop_cnt(stringhash5_t *);
static inline void * stringhash5_find_attach(stringhash5_t *, void *, int);
static inline int stringhash5_delete(stringhash5_t *, void *, int);
static inline void stringhash5_flush(stringhash5_t *);
static inline void stringhash5_destroy(stringhash5_t *);
static inline void stringhash5_scour(stringhash5_t *, stringhash5_callback, void *);
static inline void stringhash5_scour_and_destroy(stringhash5_t *, stringhash5_callback, void *);
static inline stringhash5_t * stringhash5_read(FILE *);
static inline stringhash5_t * stringhash5_read_with_ptrs(FILE *, sh5dataread_callback_t);
static inline int stringhash5_dump(stringhash5_t *, FILE *);
static inline int stringhash5_dump_with_ptrs(stringhash5_t *, FILE *, sh5datadump_callback_t);
static inline void * stringhash5_find_loc(stringhash5_t *, ws_hashloc_t *);
static inline void * stringhash5_find_wsdata(stringhash5_t *, wsdata_t *);
static inline void * stringhash5_find_attach_loc(stringhash5_t *, ws_hashloc_t *);
static inline void * stringhash5_find_attach_wsdata(stringhash5_t *, wsdata_t *);
static inline int stringhash5_delete_loc(stringhash5_t *, ws_hashloc_t *);
static inline int stringhash5_delete_wsdata(stringhash5_t *, wsdata_t *);
static inline stringhash5_walker_t * stringhash5_walker_init(stringhash5_t *, stringhash5_visit, 
                                                             void *);
static inline int stringhash5_walker_next(stringhash5_walker_t *);
static inline int stringhash5_walker_destroy(stringhash5_walker_t *);


static inline void stringhash5_set_callback(stringhash5_t * sht,
                                            stringhash5_callback cb,
                                            void * vproc) {
     SET_NRANK

     // first arrival needs to set the (shared) callback function
     if (!sht->callback) {
          sht->callback = cb;
     }

     // all threads need to store their (local) data pointer
     if (sht->is_shared) {
          sht->cb_vproc[nrank] = vproc;
     }
     else {
          sht->cb_vproc[0] = vproc;
     }
}

//compute log2 of an unsigned int
// by Eric Cole - http://graphics.stanford.edu/~seander/bithacks.htm
static inline uint32_t sh5_uint32_log2(uint32_t v) {
     static const int MultiplyDeBruijnBitPosition[32] = 
     {
          0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8, 
          31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9
     };

     v |= v >> 1; // first round down to power of 2 
     v |= v >> 2;
     v |= v >> 4;
     v |= v >> 8;
     v |= v >> 16;
     v = (v >> 1) + 1;

     return MultiplyDeBruijnBitPosition[(uint32_t)(v * 0x077CB531U) >> 27];
}

static inline void sh5_init_buckets(stringhash5_t * sht) {
     uint32_t i,j;
     for (i = 0; i < sht->all_index_size; i++) {
          for (j = 0; j < SH5_DEPTH; j++) {
               sht->buckets[i].digest[j] = j << SH5_DATA_BIN;
          }
     }
}

static inline int stringhash5_create_mutex(stringhash5_t * sht) {

     //every 32 indexes we have a mutex
     sht->mutex_index_shift = 5;
     sht->max_mutex = ((sht->index_size * 2) >> sht->mutex_index_shift);
     if (!sht->max_mutex) {
          sht->max_mutex = 1;
     }
     sht->mutex = (sh5_mutex_t *)calloc(sht->max_mutex, sizeof(sh5_mutex_t));
     if (!sht->mutex) {
          free(sht);
          error_print("failed calloc of stringhash5_mway mutex");
          return 0;
     }
     uint32_t i;
     for (i = 0; i < sht->max_mutex; i++) {
          SHT_LOCK_INIT(&sht->mutex[i],mutex_attr);
     }
     sht->curr_lock = (uint32_t *)calloc(work_size, sizeof(uint32_t));
     if (!sht->curr_lock) {
          free(sht);
          error_print("failed calloc of stringhash5_mway curr_lock");
          return 0;
     }

     //also create a master lock for a few kids that need it (i.e. those
     //that would have two or more of the above mutexes active 
     //simultaneously
     SHT_LOCK_INIT(&sht->masterlock,mutex_attr);

     return 1;
}

//pad data allocation to 8 byte boundary if appropriate
static inline uint32_t stringhash5_pad_alloc(uint32_t alloc) {
     if (alloc < 8) {
          switch (alloc) {
          case 0:
          case 1:
          case 2:
          case 4:
               return alloc;
          case 3:
               return 4;
          default:
               return 8;
          }
     }
     else {
          int remainder = alloc % 8;
          if (remainder) {
               return alloc - remainder + 8;
          }
          else {
               return alloc;
          }
     }
}

static inline uint32_t check_sh5_max_records(uint64_t max_records) {

     //enforce a minimum table size for the sake of robustness
     if(max_records < 4*SH5_DEPTH) {
          tool_print("WARNING: minimum size for sh5 max_records must be at least %d...resizing",
                     4*SH5_DEPTH);
          max_records = 4*SH5_DEPTH;
     }

     // using max_records estimate and taking the floor of log2.. 
     uint32_t bits = sh5_uint32_log2((uint32_t)max_records);
     if(max_records & (max_records - 1)) {
          // we need to add 1 to our log2 value; max_records is not a power of 2
          bits++;
     }
     max_records = 1<<bits;

     return max_records;
}

static inline void stringhash5_sh_opts_alloc(stringhash5_sh_opts_t ** sh5_sh_opts) {
     *sh5_sh_opts = (stringhash5_sh_opts_t *)calloc(1,sizeof(stringhash5_sh_opts_t));
}

static inline void stringhash5_sh_opts_free(stringhash5_sh_opts_t * sh5_sh_opts) {
     free(sh5_sh_opts);
}

//DEPRECATED
static inline int stringhash5_open_table(stringhash5_t ** table, void * proc, 
                                         const char * open_table, uint64_t * nextval, 
                                         int read_n_scour, sh_callback_t sh_scour) { 

     stringhash5_sh_opts_t * sh5_sh_opts;
     int ret;
     tool_print("WARNING: 'stringhash5_open_table' HAS BEEN DEPRECATED. PLEASE USE 'stringhash5_open_sht_table' INSTEAD.");

     //checks against max_records and data_alloc won't be done for the deprecated functions
     uint64_t max_records = 0;
     uint32_t data_alloc = 0; 

     //calloc shared sh5 option struct
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     //set shared sh5 option fields
     sh5_sh_opts->open_table = open_table;
     sh5_sh_opts->read_n_scour = read_n_scour;
     sh5_sh_opts->sh_scour = sh_scour; 

     ret = stringhash5_open_sht_table(table, proc, max_records, data_alloc, sh5_sh_opts);

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //the following patches up another discrepancy between the old and new implementations
     if (nextval) {
          *nextval = ((stringhash5_t *)(*table))->nextval;
     }

     return ret;
}

//DEPRECATED
static inline int stringhash5_open_table_with_ptrs(stringhash5_t ** table, void * proc, 
                                                   const char * open_table, uint64_t * nextval,
                                                   int read_n_scour, sh_callback_t sh_scour, 
                                                   sh5dataread_callback_t sh5_dataread_cb) {

     stringhash5_sh_opts_t * sh5_sh_opts;
     int ret;
     tool_print("WARNING: 'stringhash5_open_table_with_ptrs' HAS BEEN DEPRECATED. PLEASE USE 'stringhash5_open_sht_table' INSTEAD.");

     //checks against max_records and data_alloc won't be done for the deprecated functions
     uint64_t max_records = 0;
     uint32_t data_alloc = 0; 

     //calloc shared sh5 option struct
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     //set shared sh5 option fields
     sh5_sh_opts->open_table = open_table;
     sh5_sh_opts->read_n_scour = read_n_scour;
     sh5_sh_opts->sh_scour = sh_scour; 
     sh5_sh_opts->sh5_dataread_cb = sh5_dataread_cb;

     ret = stringhash5_open_sht_table(table, proc, max_records, data_alloc, sh5_sh_opts);

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //the following patches up another discrepancy between the old and new implementations
     if (nextval) {
          *nextval = ((stringhash5_t *)(*table))->nextval;
     }

     return ret;
}

//CURRENT
static inline int stringhash5_open_sht_table(stringhash5_t ** table, void * proc, 
                                             uint64_t max_records, uint32_t data_alloc, 
                                             stringhash5_sh_opts_t * sh5_sh_opts) {
     if (sh5_sh_opts->open_table) {
          FILE * fp;
          fp = sysutil_config_fopen(sh5_sh_opts->open_table, "r");
          if (!fp) {
               tool_print("stringhash5_open_table unable to open table %s", (char *)sh5_sh_opts->open_table);
               tool_print("stringhash5_open_table ignoring load failure -- starting empty table");
          }
          else {
               //read the stringhash5 table
               if (sh5_sh_opts->sh5_dataread_cb) {
                    *table = stringhash5_read_with_ptrs(fp, sh5_sh_opts->sh5_dataread_cb);
               }
               else {
                    *table = stringhash5_read(fp);
               }
               sysutil_config_fclose(fp);
               if (!*table) {
                    error_print("unable to read stringhash5 table");
                    return 0;
               }

               //give an error if max_records does not match the input value for this thread
               if (max_records && ((stringhash5_t *)(*table))->max_records != 
                                   check_sh5_max_records(max_records)) {
                    error_print("stringhash5 table max_records %" PRIu64 " not equal to converted input value %d",
                               (uint64_t)((stringhash5_t *)(*table))->max_records, 
                               check_sh5_max_records(max_records));
                    return 0;
               }

               //give an error if data_alloc does not match the input value for this thread
               if (data_alloc && ((stringhash5_t *)(*table))->data_alloc != 
                                  stringhash5_pad_alloc(data_alloc)) {
                    error_print("stringhash5 table data_alloc %" PRIu64 " not equal to local stringhash5_pad_alloc(data_alloc) %d",
                                (uint64_t)((stringhash5_t *)(*table))->data_alloc, 
                                stringhash5_pad_alloc(data_alloc));
                    return 0;
               }

               tool_print("finished loading table %s", (char *)sh5_sh_opts->open_table);

               //if requested, immediately scour the table
               if (sh5_sh_opts->read_n_scour) {
                    stringhash5_scour((stringhash5_t *)(*table), sh5_sh_opts->sh_scour, proc);
               }

               if (!enroll_in_sht_registry(*table, "sh5", ((stringhash5_t *)(*table))->mem_used,
                                           ((stringhash5_t *)(*table))->hash_seed)) {
                    return 0;
               }

               return 1;
          }
     }

     return 0;
}

// At long last - the first parameter has a use!  We use the first argument to signal that
// stringhash5_create is being called to create a shared table (i.e. the call is made
// from stringhash5_create_shared)
static inline stringhash5_t * stringhash5_create(uint32_t is_shared, uint64_t max_records,
                                                 uint32_t data_alloc) {
     stringhash5_t * sht;

     if (!data_alloc) {
          error_print("must allocate data for each record");
          return NULL;
     }

     //enforce a minimum table size for the sake of robustness
     if(max_records < 4*SH5_DEPTH) {
          tool_print("WARNING: minimum size for sh5 max_records must be at least %d...resizing",
                     4*SH5_DEPTH);
          max_records = 4*SH5_DEPTH;
     }

     // using max_records estimate and taking the floor of log2.. 
     uint32_t bits = sh5_uint32_log2((uint32_t)max_records);
     if(max_records & (max_records - 1)) {
          // we need to add 1 to our log2 value; max_records is not a power of 2
          bits++;
     }

     sht = (stringhash5_t *)calloc(1, sizeof(stringhash5_t));
     if (!sht) {
          error_print("failed calloc of stringhash5_mway hash table");
          return NULL;
     }

     sht->cb_vproc = (void **)calloc(work_size, sizeof(void *));
     if (!sht->cb_vproc) {
          error_print("failed calloc of stringhash5_mway cb_vproc");
          free(sht);
          return NULL;
     }

     sht->max_records = 1<<bits;
     uint32_t ibits = bits - SH5_DEPTH_BITS - 1;
     sht->index_size = 1<<(ibits);
     sht->einsert_max = sht->index_size >> 4;
     sht->all_index_size = sht->index_size * 2;
     sht->mask_index = ((uint64_t)~0)>>(64-(ibits));
     sht->nextval = 1;

     sht->data_alloc = stringhash5_pad_alloc(data_alloc);

     sht->hash_seed = (uint32_t)rand();
     sht->epoch = 1;

     // now to allocate memory...
     sht->buckets = (sh5_bucket_t *)calloc(sht->all_index_size,
                                           sizeof(sh5_bucket_t));

     if (!sht->buckets) {
          free(sht);
          error_print("failed calloc of stringhash5_mway buckets");
          return NULL;
     }

     sht->data = (uint8_t*)calloc(sht->max_records, sht->data_alloc);
     if (!sht->data) {
          free(sht->buckets);
          free(sht);
          error_print("failed calloc of stringhash5_mway data");
          return NULL;
     }

     sh5_init_buckets(sht);

     // tally up the hash table memory use
     sht->mem_used = sht->all_index_size * sizeof(sh5_bucket_t) + 
                     sht->data_alloc * sht->max_records + sizeof(stringhash5_t);
     if (!is_shared) {
          if (!enroll_in_sht_registry(sht, "sh5", sht->mem_used, sht->hash_seed)) {
               return NULL;
          }
     }

     return sht;
}

static inline void stringhash5_clean_data_field(stringhash5_t * sht) {
     memset(sht->data, 0, sht->max_records * sht->data_alloc);
}

//DEPRECATED
// wrapper function for the deprecated stringhash5_create_shared
static inline int stringhash5_create_shared(void * v_type_table, void ** table, 
                     const char * sharelabel, uint32_t max_records, uint32_t data_alloc, 
                     int * sharer_id, sh_callback_t sh_callback, void * proc, 
                     int readonly, void * open_table, uint64_t * nextval, 
                     int read_n_scour, sh_callback_t sh_scour) {

     stringhash5_sh_opts_t * sh5_sh_opts;
     int ret;
     tool_print("WARNING: 'stringhash5_create_shared' HAS BEEN DEPRECATED. PLEASE USE 'stringhash5_create_shared_sht' INSTEAD.");

     //calloc shared sh5 option struct
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     //set shared sh5 option fields
     sh5_sh_opts->sh_callback = sh_callback;
     sh5_sh_opts->proc = proc; 
     sh5_sh_opts->readonly = readonly;
     sh5_sh_opts->open_table = (char *)open_table;
     sh5_sh_opts->read_n_scour = read_n_scour;
     sh5_sh_opts->sh_scour = sh_scour; 

     ret = stringhash5_create_shared_sht(v_type_table, table, 
                     sharelabel, max_records, data_alloc, 
                     sharer_id, sh5_sh_opts);

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //the following patches up another discrepancy between the old and new implementations
     if (nextval) {
          *nextval = ((stringhash5_t *)(*table))->nextval;
     }

     return ret;
}

//DEPRECATED
// wrapper function for the deprecated stringhash5_create_shared_with_ptrs
static inline int stringhash5_create_shared_with_ptrs(void * v_type_table, void ** table, 
                     const char * sharelabel, uint32_t max_records, uint32_t data_alloc, 
                     int * sharer_id, sh_callback_t sh_callback, void * proc, 
                     int readonly, void * open_table, uint64_t * nextval, 
                     int read_n_scour, sh_callback_t sh_scour, 
                     sh5dataread_callback_t sh5_dataread_cb) {

     stringhash5_sh_opts_t * sh5_sh_opts;
     int ret;
     tool_print("WARNING: 'stringhash5_create_shared_with_ptrs' HAS BEEN DEPRECATED. PLEASE USE 'stringhash5_create_shared_sht' INSTEAD.");

     //calloc shared sh5 option struct
     stringhash5_sh_opts_alloc(&sh5_sh_opts);

     //set shared sh5 option fields
     sh5_sh_opts->sh_callback = sh_callback;
     sh5_sh_opts->proc = proc; 
     sh5_sh_opts->readonly = readonly;
     sh5_sh_opts->open_table = (char *)open_table;
     sh5_sh_opts->read_n_scour = read_n_scour;
     sh5_sh_opts->sh_scour = sh_scour; 
     sh5_sh_opts->sh5_dataread_cb = sh5_dataread_cb;

     ret = stringhash5_create_shared_sht(v_type_table, table, 
                     sharelabel, max_records, data_alloc, 
                     sharer_id, sh5_sh_opts);

     //free shared sh5 option struct
     stringhash5_sh_opts_free(sh5_sh_opts);

     //the following patches up another discrepancy between the old and new implementations
     if (nextval) {
          *nextval = ((stringhash5_t *)(*table))->nextval;
     }

     return ret;
}

static inline int stringhash5_check_params(void ** table, uint32_t max_records, uint32_t data_alloc) {

     //give an error if max_records does not match the input value for this thread
     if (((stringhash5_t *)(*table))->max_records != check_sh5_max_records(max_records)) {
          error_print("stringhash5 table max_records %" PRIu64 " not equal to converted input value %d",
                     (uint64_t)((stringhash5_t *)(*table))->max_records, 
                     check_sh5_max_records(max_records));
          return 0;
     }

     //give an error if data_alloc does not match the input value for this thread
     if (((stringhash5_t *)(*table))->data_alloc != stringhash5_pad_alloc(data_alloc)) {
          error_print("stringhash5 table data_alloc %" PRIu64 " not equal to local stringhash5_pad_alloc(data_alloc) %d",
                      (uint64_t)((stringhash5_t *)(*table))->data_alloc, 
                      stringhash5_pad_alloc(data_alloc));
          return 0;
     }

     return 1;
}

//CURRENT
static inline int stringhash5_create_shared_sht(void * v_type_table, void ** table, 
                     const char * sharelabel, uint32_t max_records, uint32_t data_alloc, 
                     int * sharer_id, stringhash5_sh_opts_t * sh5_sh_opts) {
     int shid;
     SET_NRANK

     //sharelabel error checks:  null sharelabel
     if (!sharelabel) {
          error_print("this kid is not sharing stringhash5, please specify share with -J option");
          error_print("also, this kid should not have entered here without -J set");
          return 0;
     }

     //see if structure is already available at label
     share5_t * sharedata = (share5_t *)ws_kidshare_get(v_type_table, sharelabel);

     if (sharedata) {
          *table = sharedata->table;
          if (!*table){
               error_print("unable to share a proper stringhash5 table");
               return 0;
          }
          sharedata->cnt++;
          tool_print("this is kid #%d to share stringhash5 table at label %s",
                     sharedata->cnt, sharelabel);
          shid = sharedata->cnt;

          //check table size against input parameters
          if (!sh5_sh_opts->readonly) {
               if (!stringhash5_check_params(table, max_records, data_alloc)) {
                    return 0;
               }
          }
     }

     //no sharing at label yet..
     else {
          tool_print("this is the first kid to share stringhash5 table at label %s", sharelabel);

          //read the stringhash5 table from the open_table file
          if (sh5_sh_opts->open_table) {
               FILE * fp;
               fp = sysutil_config_fopen((const char *)sh5_sh_opts->open_table, "r");
               if (!fp) {
                    if (sh5_sh_opts->readonly) {
                         error_print("stringhash5_create_shared unable to load table %s", (char *)sh5_sh_opts->open_table);
                         return 0;
                    }
                    tool_print("stringhash5_create_shared unable to load table %s", (char *)sh5_sh_opts->open_table);
                    tool_print("stringhash5_create_shared ignoring load failure -- starting empty table");
                    *table = stringhash5_create(1, max_records, data_alloc);
                    if (!*table) {
                         error_print("stringhash5_create_shared unable to initialize stringhash5 table");
                         return 0;
                    }
               }
               else {
                    //read the stringhash5 table
                    tool_print("loading table %s", (char *)sh5_sh_opts->open_table);
                    if (sh5_sh_opts->sh5_dataread_cb) {
                         *table = stringhash5_read_with_ptrs(fp, sh5_sh_opts->sh5_dataread_cb);
                    }
                    else {
                         *table = stringhash5_read(fp);
                    }
                    sysutil_config_fclose(fp);

                    if (!*table) {
                         if (sh5_sh_opts->readonly) {
                              error_print("unable to read stringhash5 table");
                              return 0;
                         }
                         else {
                              tool_print("stringhash5_create_shared unable to read stringhash5 table");
                              tool_print("stringhash5_create_shared ignoring load failure -- starting empty table");
                              *table = stringhash5_create(1, max_records, data_alloc);
                              if (!*table) {
                                   error_print("stringhash5_create_shared unable to initialize stringhash5 table");
                                   return 0;
                              }
                         }
                    }

                    //check table size against input parameters
                    if (!sh5_sh_opts->readonly) {
                         if (!stringhash5_check_params(table, max_records, data_alloc)) {
                              return 0;
                         }
                    }
                    tool_print("finished loading table %s", (char *)sh5_sh_opts->open_table);

                    //if requested, immediately scour the table
                    if (sh5_sh_opts->read_n_scour) {
                         stringhash5_scour((stringhash5_t *)*table, sh5_sh_opts->sh_scour, sh5_sh_opts->proc);
                    }
               }
          }
          else {
               //create the stringhash5 table from scratch
               *table = stringhash5_create(1, max_records, data_alloc);
               if (!*table) {
                    error_print("unable to initialize stringhash5 table");
                    return 0;
               }
          }

          // set up mutex locks for a shared table
          if (!stringhash5_create_mutex((stringhash5_t *)*table)) {
               return 0;
          }
          ((stringhash5_t *)(*table))->mem_used += sizeof(sh5_mutex_t *) * 
                                                   (uint64_t)((stringhash5_t *)(*table))->max_mutex;

          //enroll the table
          if (!enroll_shared_in_sht_registry(*table, "sh5 shared", sharelabel, 
                                             ((stringhash5_t *)(*table))->mem_used, 
                                             ((stringhash5_t *)(*table))->hash_seed)) {
               error_print("unable to enroll stringhash5 table");
               return 0;
          }
          tool_print("sh5 memory used = %" PRIu64, ((stringhash5_t *)(*table))->mem_used);

          //create the sharedata
          sharedata = (share5_t *)calloc(1,sizeof(share5_t));
          if (!sharedata) {
               error_print("failed stringhash5_create_shared calloc of sharedata");
               return 0;
          }

          //load up the sharedata
          sharedata->table = *table;
          sharedata->cnt++;
          ((stringhash5_t *)(*table))->sharelabel = strdup(sharelabel);
          ((stringhash5_t *)(*table))->sharedata = sharedata;
          ((stringhash5_t *)(*table))->v_type_table = v_type_table;
          ((stringhash5_t *)(*table))->is_shared = 1;
          shid = 0;

          // If callbacks are active, use the proc structure of this thread as the 
          // default for flushing by thread 0 (if thread 0 also executes this kid,
          // it will come along later and overwrite its proc value here)
          if (nrank && sh5_sh_opts->sh_callback) {
               ((stringhash5_t *)(*table))->cb_vproc[0] = sh5_sh_opts->proc;
          }

          //actually share structure
          ws_kidshare_put(v_type_table, sharelabel, sharedata);
     }

     //set up the sh5 callback function
     if (sh5_sh_opts->sh_callback) {
          stringhash5_set_callback((stringhash5_t *)*table, sh5_sh_opts->sh_callback, sh5_sh_opts->proc);
     }

     // if a memory location is supplied, pass the sharer_id back to the kid
     if (sharer_id) {
          *sharer_id = shid;
     }

     return 1; 
}

static inline void stringhash5_unlock(stringhash5_t * sht) {
     if (sht->is_shared) {
          SH5_J_UNLOCK(sht,sht->curr_lock[GETRANK()])
     }
}

static inline void stringhash5_masterlock_lock(stringhash5_t * sht) {
     if (sht->is_shared) {
          SHT_LOCK(&sht->masterlock)
     }
}

static inline void stringhash5_masterlock_unlock(stringhash5_t * sht) {
     if (sht->is_shared) {
          SHT_UNLOCK(&sht->masterlock)
     }
}

//given an index and bucket.. find state data...
static inline int sh5_lookup_bucket(sh5_bucket_t * bucket,
                                    uint32_t digest,
                                    uint32_t *databin,
                                    uint32_t *digestbin) {
     int i;

     sh5_digest_t * dp = bucket->digest;
     for (i = 0 ; i < SH5_DEPTH; i++) {
          if (digest == (dp[i] & SH5_DIGEST_MASK)) {
               *databin = (dp[i]>>SH5_DATA_BIN);
               *digestbin = i;
               return 1;
          }
     }
     return 0;
}

#define SH5_PERMUTE1 (0xed31952d18a569ddULL)
#define SH5_PERMUTE2 (0x94e36ad1c8d2654bULL)

static inline void sh5_gethash(stringhash5_t * sht,
                               uint8_t * key, uint32_t keylen,
                               uint32_t *h1, uint32_t *h2,
                               uint32_t *pd1, uint32_t *pd2) {

     uint64_t m = evahash64((uint8_t*)key, keylen, sht->hash_seed);
     uint64_t p1 = m * SH5_PERMUTE1;
     uint64_t p2 = m * SH5_PERMUTE2;
     uint64_t lh1, lh2;
     lh1 = (p1 >> SH5_DIGEST_BITS) & sht->mask_index;
     lh2 = ((p2 >> SH5_DIGEST_BITS) & sht->mask_index) | sht->index_size;
     *h1 = (uint32_t)lh1;
     *h2 = (uint32_t)lh2;

     uint32_t d1, d2;
     d1 = (uint32_t)(p1 & SH5_DIGEST_MASK);
     d2 = (uint32_t)(p2 & SH5_DIGEST_MASK);

     //make sure digest not zero - if so, set to default
     *pd1 = d1 ? d1 : SH5_DIGEST_DEFAULT;
     *pd2 = d2 ? d2 : SH5_DIGEST_DEFAULT;
}

//move mru item to front..  ridiculous code bloat - but it's supposed
// to loop unravel
static inline void sh5_sort_lru(sh5_digest_t * d, uint8_t mru) {
     uint32_t a = d[mru];
     switch(mru) {
     case 15:
          d[15] = (d[15] & SH5_EPOCH_MASK) | (SH5_ANTI_EPOCH_MASK & d[14]);
     case 14:
          d[14] = (d[14] & SH5_EPOCH_MASK) | (SH5_ANTI_EPOCH_MASK & d[13]);
     case 13:
          d[13] = (d[13] & SH5_EPOCH_MASK) | (SH5_ANTI_EPOCH_MASK & d[12]);
     case 12:
          d[12] = (d[12] & SH5_EPOCH_MASK) | (SH5_ANTI_EPOCH_MASK & d[11]);
     case 11:
          d[11] = (d[11] & SH5_EPOCH_MASK) | (SH5_ANTI_EPOCH_MASK & d[10]);
     case 10:
          d[10] = (d[10] & SH5_EPOCH_MASK) | (SH5_ANTI_EPOCH_MASK & d[9]);
     case 9:
          d[9] = (d[9] & SH5_EPOCH_MASK) | (SH5_ANTI_EPOCH_MASK & d[8]);
     case 8:
          d[8] = (d[8] & SH5_EPOCH_MASK) | (SH5_ANTI_EPOCH_MASK & d[7]);
     case 7:
          d[7] = d[6];
     case 6:
          d[6] = d[5];
     case 5:
          d[5] = d[4];
     case 4:
          d[4] = d[3];
     case 3:
          d[3] = d[2];
     case 2:
          d[2] = d[1];
     case 1:
          d[1] = d[0];
     }
     d[0] = a; 
}

//called when epoch is going to be set anyways
static inline void sh5_sort_lru_noepoch(sh5_digest_t * d, uint8_t mru) {
     uint32_t a = d[mru];
     switch(mru) {
     case 15:
          d[15] = d[14];
     case 14:
          d[14] = d[13];
     case 13:
          d[13] = d[12];
     case 12:
          d[12] = d[11];
     case 11:
          d[11] = d[10];
     case 10:
          d[10] = d[9];
     case 9:
          d[9] = d[8];
     case 8:
          d[8] = d[7];
     case 7:
          d[7] = d[6];
     case 6:
          d[6] = d[5];
     case 5:
          d[5] = d[4];
     case 4:
          d[4] = d[3];
     case 3:
          d[3] = d[2];
     case 2:
          d[2] = d[1];
     case 1:
          d[1] = d[0];
     }
     d[0] = a;
}

//find records using hashkeys.. return 1 if found
static inline void * stringhash5_find_shared(stringhash5_t * sht,
                                      void * key, int keylen) {

     uint32_t h1, h2, d1, d2;
     uint32_t databin, digestbin;

     sh5_gethash(sht, (uint8_t*)key, keylen, &h1, &h2, &d1, &d2);

     SH5_LOCK(sht,h1)
     sh5_bucket_t * bucket1 = &sht->buckets[h1];

     if (sh5_lookup_bucket(bucket1, d1, &databin, &digestbin)) {
          sh5_sort_lru(bucket1->digest, digestbin);
          CURRENT_LOCK_VALUE(sht,h1)
          return sht->data + (sht->data_alloc *
                              ((size_t)databin +
                               ((size_t)h1 << SH5_DEPTH_BITS)));
     }
     SH5_UNLOCK(sht,h1)
     SH5_LOCK(sht,h2)
     sh5_bucket_t * bucket2 = &sht->buckets[h2];
     if (sh5_lookup_bucket(bucket2, d2, &databin, &digestbin)) {
          sh5_sort_lru(bucket2->digest, digestbin);
          CURRENT_LOCK_VALUE(sht,h2)
          return sht->data + (sht->data_alloc *
                              ((size_t)databin +
                               ((size_t)h2 << SH5_DEPTH_BITS)));
     }
     SH5_UNLOCK(sht,h2)

     return NULL;
}

static inline void * stringhash5_find_serial(stringhash5_t * sht,
                                      void * key, int keylen) {

     uint32_t h1, h2, d1, d2;
     uint32_t databin, digestbin;

     sh5_gethash(sht, (uint8_t*)key, keylen, &h1, &h2, &d1, &d2);

     sh5_bucket_t * bucket1 = &sht->buckets[h1];

     if (sh5_lookup_bucket(bucket1, d1, &databin, &digestbin)) {
          sh5_sort_lru(bucket1->digest, digestbin);
          return sht->data + (sht->data_alloc *
                              ((size_t)databin +
                               ((size_t)h1 << SH5_DEPTH_BITS)));
     }
     sh5_bucket_t * bucket2 = &sht->buckets[h2];
     if (sh5_lookup_bucket(bucket2, d2, &databin, &digestbin)) {
          sh5_sort_lru(bucket2->digest, digestbin);
          return sht->data + (sht->data_alloc *
                              ((size_t)databin +
                               ((size_t)h2 << SH5_DEPTH_BITS)));
     }

     return NULL;
}

static inline void * stringhash5_find(stringhash5_t * sht,
                                      void * key, int keylen) {
     if (sht->is_shared) {
          return stringhash5_find_shared(sht, key, keylen);
     }
     else {
          return stringhash5_find_serial(sht, key, keylen);
     }
}

// the next four functions are added to better support graphs (of fixed out degree).
// Each node's data also contains it's tag, and pointers to other nodes are their tag, bucket, and databin.
// To lookup a next node, call stringhash5_jump_to_entry(sht, bucket, databin) and check if this entry's tag matches parent_tag.
// If so, assume it's the same node. If not, assume it's different and remove the querying node no longer has a parent.
//
// jump straight to a record if the bucket and databin are known 
static inline void * stringhash5_jump_to_entry_shared(stringhash5_t *sht, uint32_t bucket, uint32_t databin) {
     SH5_LOCK(sht,bucket)
     return sht->data + (sht->data_alloc * ((size_t)databin + ((size_t)bucket << SH5_DEPTH_BITS)));
}

static inline void * stringhash5_jump_to_entry_serial(stringhash5_t *sht, uint32_t bucket, uint32_t databin) {
     return sht->data + (sht->data_alloc * ((size_t)databin + ((size_t)bucket << SH5_DEPTH_BITS)));
}

static inline void * stringhash5_jump_to_entry(stringhash5_t * sht, uint32_t bucket, uint32_t databin) {
     if (sht->is_shared) {
          return stringhash5_jump_to_entry_shared(sht, bucket, databin);
     }
     else {
          return stringhash5_jump_to_entry_serial(sht, bucket, databin);
     }
}

// given a bucket and databin, find the digestbin
static inline int sh5_lookup_digestbin(sh5_bucket_t * bucket,
                                       uint32_t databin) {
     int i;
     for (i = 0; i < SH5_DEPTH; i++) {
          if (databin == (bucket->digest[i]>>SH5_DATA_BIN)) {
               return i;
          }
     }
     return -1;
}

static inline void stringhash5_mark_as_used_shared(stringhash5_t * sht, uint32_t bucket, uint32_t databin) {
     SH5_LOCK(sht,bucket)
     sh5_bucket_t *bucket1=&sht->buckets[bucket];
     int32_t digestbin = sh5_lookup_digestbin(bucket1, databin);
     if (digestbin != -1) {
          sh5_sort_lru(bucket1->digest, digestbin);
     }
     SH5_UNLOCK(sht,bucket)
}

static inline void stringhash5_mark_as_used_serial(stringhash5_t * sht, uint32_t bucket, uint32_t databin) {
     sh5_bucket_t *bucket1=&sht->buckets[bucket];
     int32_t digestbin = sh5_lookup_digestbin(bucket1, databin);
     if (digestbin != -1) {
          sh5_sort_lru(bucket1->digest, digestbin);
     }
}

static inline void stringhash5_mark_as_used(stringhash5_t * sht, uint32_t bucket, uint32_t databin) {
     if (sht->is_shared) {
          stringhash5_mark_as_used_shared(sht, bucket, databin);
     }
     else {
          stringhash5_mark_as_used_serial(sht, bucket, databin);
     }
}

static inline void stringhash5_get_bucket_databin(stringhash5_t *sht, void *data,
                                                  uint32_t *bucket, uint32_t *databin) {
     uint64_t position = (uint64_t)((uint8_t*)data - sht->data)/((uint64_t)sht->data_alloc);
     *bucket= position >> SH5_DEPTH_BITS;
     *databin= position - (*bucket << SH5_DEPTH_BITS);
}

//steal bits for epoch counter
static inline uint8_t sh5_get_epoch(stringhash5_t * sht, sh5_digest_t * d) {
     uint32_t epoch =
          ((d[15] & SH5_EPOCH_MASK) >> (SH5_EPOCH_SHIFT - 7)) +
          ((d[14] & SH5_EPOCH_MASK) >> (SH5_EPOCH_SHIFT - 6)) +
          ((d[13] & SH5_EPOCH_MASK) >> (SH5_EPOCH_SHIFT - 5)) +
          ((d[12] & SH5_EPOCH_MASK) >> (SH5_EPOCH_SHIFT - 4)) +
          ((d[11] & SH5_EPOCH_MASK) >> (SH5_EPOCH_SHIFT - 3)) +
          ((d[10] & SH5_EPOCH_MASK) >> (SH5_EPOCH_SHIFT - 2)) +
          ((d[9] & SH5_EPOCH_MASK) >> (SH5_EPOCH_SHIFT - 1)) +
          ((d[8] & SH5_EPOCH_MASK) >> (SH5_EPOCH_SHIFT));
     return sht->epoch - (uint8_t)epoch;
}

static inline void sh5_set_epoch(stringhash5_t * sht, sh5_digest_t * d) {
     sht->einserts++;
     if (sht->einserts > sht->einsert_max) {
          sht->epoch++;
          sht->einserts = 0;
     }
     uint8_t epoch = sht->epoch;
     int i;
     for (i = 0; i < 8; i++) {
          d[8 + i] = (d[8 + i] & SH5_ANTI_EPOCH_MASK) |
               (((epoch >> i) & 0x1) << SH5_EPOCH_SHIFT);
     }
}

static inline sh5_bucket_t * sh5_find_oldest_bucket(stringhash5_t * sht,
                                                  sh5_bucket_t * b1,
                                                  sh5_bucket_t * b2) {
     uint8_t e1 = sh5_get_epoch(sht, b1->digest);
     uint8_t e2 = sh5_get_epoch(sht, b2->digest);

     if (e2 > e1) {
          return b2;
     }
     return b1;
}

//find lowest bucket depth or oldest bucket
static inline sh5_bucket_t * sh5_find_best_bucket(stringhash5_t * sht,
                                                  sh5_bucket_t * b1,
                                                  sh5_bucket_t * b2) {
     int d1 = 0;
     int d2 = 0;

     int i;
     for (i = SH5_DEPTH - 1; i >= 0; i--) {
          if (b1->digest[i] & SH5_DIGEST_MASK) {
               d1 = i + 1;
               break;
          }
     }
     for (i = SH5_DEPTH - 1; i >= 0; i--) {
          if (b2->digest[i] & SH5_DIGEST_MASK) {
               d2 = i + 1;
               break;
          }
     }
     if (d1 == d2) {
          if (d1 == SH5_DEPTH) {
               sht->drops++;
               return sh5_find_oldest_bucket(sht, b1, b2);
          }
          else {
               return b1;
          }
     }
     else if (d2 < d1) {
          return b2;
     }
     return b1;
}

static inline uint64_t stringhash5_drop_cnt(stringhash5_t * sht) {
     return sht->drops;
}

//find records using hashkeys.. return 1 if found
static inline void * stringhash5_find_attach_shared(stringhash5_t * sht,
                                             void * key, int keylen) {

     uint32_t h1, h2, d1, d2, pairflag;
     uint32_t databin, digestbin;
     SET_NRANK

     sh5_gethash(sht, (uint8_t*)key, keylen, &h1, &h2, &d1, &d2);

     SH5_SHIFT_KEY(h1, k1)
     SH5_SHIFT_KEY(h2, k2)
     SH5_LOCK_PAIR(sht,k1,k2,pairflag)
     sh5_bucket_t * bucket1 = &sht->buckets[h1];

     if (sh5_lookup_bucket(bucket1, d1, &databin, &digestbin)) {
          if (pairflag) {
               SH5_UNLOCK(sht,h2)
          }
          sh5_sort_lru(bucket1->digest, digestbin);
          CURRENT_LOCK_VALUE(sht,h1)
          return sht->data + (sht->data_alloc *
                              ((size_t)databin +
                               ((size_t)h1 << SH5_DEPTH_BITS)));
     }
     sh5_bucket_t * bucket2 = &sht->buckets[h2];
     if (sh5_lookup_bucket(bucket2, d2, &databin, &digestbin)) {
          if (pairflag) {
               SH5_UNLOCK(sht,h1)
          }
          sh5_sort_lru(bucket2->digest, digestbin);
          CURRENT_LOCK_VALUE(sht,h2)
          return sht->data + (sht->data_alloc *
                              ((size_t)databin +
                               ((size_t)h2 << SH5_DEPTH_BITS)));
     }

     //ok nothing found.. measure depth of each bucket
     sh5_bucket_t * ibucket = sh5_find_best_bucket(sht, bucket1, bucket2);
     uint32_t idigest = d1;
     uint32_t ih = h1;
     if (ibucket == bucket2) {
          if (pairflag) {
               SH5_UNLOCK(sht,h1)
          }
          idigest = d2;
          ih = h2;
     } 
     else {
          if (pairflag) {
               SH5_UNLOCK(sht,h2)
          }
     }
     
     databin = (ibucket->digest[SH5_DEPTH -1])>>SH5_DATA_BIN;

     uint8_t * data = sht->data + (sht->data_alloc * 
                                   ((size_t)databin +
                                    ((size_t)ih << SH5_DEPTH_BITS)));

     if (sht->callback) {
          sh5_digest_t od = ibucket->digest[SH5_DEPTH -1] & SH5_DIGEST_MASK;
          if (od) {
               sht->callback(data, sht->cb_vproc[nrank]);
          }
     }     

     //reset data.. 
     memset(data, 0, sht->data_alloc);

     //set new digest
     ibucket->digest[SH5_DEPTH -1] = 
          (ibucket->digest[SH5_DEPTH -1] & SH5_ANTI_DIGEST_MASK) | idigest;

     sh5_sort_lru_noepoch(ibucket->digest, SH5_DEPTH-1);
     sh5_set_epoch(sht, ibucket->digest);

     CURRENT_LOCK_VALUE(sht,ih)

     return data;
}

static inline void * stringhash5_find_attach_serial(stringhash5_t * sht,
                                                    void * key, int keylen) {

     uint32_t h1, h2, d1, d2;
     uint32_t databin, digestbin;

     sh5_gethash(sht, (uint8_t*)key, keylen, &h1, &h2, &d1, &d2);

     sh5_bucket_t * bucket1 = &sht->buckets[h1];

     if (sh5_lookup_bucket(bucket1, d1, &databin, &digestbin)) {
          sh5_sort_lru(bucket1->digest, digestbin);
          return sht->data + (sht->data_alloc *
                              ((size_t)databin +
                               ((size_t)h1 << SH5_DEPTH_BITS)));
     }
     sh5_bucket_t * bucket2 = &sht->buckets[h2];
     if (sh5_lookup_bucket(bucket2, d2, &databin, &digestbin)) {
          sh5_sort_lru(bucket2->digest, digestbin);
          return sht->data + (sht->data_alloc *
                              ((size_t)databin +
                               ((size_t)h2 << SH5_DEPTH_BITS)));
     }

     //ok nothing found.. measure depth of each bucket
     sh5_bucket_t * ibucket = sh5_find_best_bucket(sht, bucket1, bucket2);
     uint32_t idigest = d1;
     uint32_t ih = h1;
     if (ibucket == bucket2) {
          idigest = d2;
          ih = h2;
     } 
     
     databin = (ibucket->digest[SH5_DEPTH -1])>>SH5_DATA_BIN;

     uint8_t * data = sht->data + (sht->data_alloc * 
                                   ((size_t)databin +
                                    ((size_t)ih << SH5_DEPTH_BITS)));

     if (sht->callback) {
          sh5_digest_t od = ibucket->digest[SH5_DEPTH -1] & SH5_DIGEST_MASK;
          if (od) {
               sht->callback(data, sht->cb_vproc[0]);
          }
     }     

     //reset data.. 
     memset(data, 0, sht->data_alloc);

     //set new digest
     ibucket->digest[SH5_DEPTH -1] = 
          (ibucket->digest[SH5_DEPTH -1] & SH5_ANTI_DIGEST_MASK) | idigest;

     sh5_sort_lru_noepoch(ibucket->digest, SH5_DEPTH-1);
     sh5_set_epoch(sht, ibucket->digest);

     return data;
}

static inline void * stringhash5_find_attach(stringhash5_t * sht,
                                      void * key, int keylen) {
     if (sht->is_shared) {
          return stringhash5_find_attach_shared(sht, key, keylen);
     }
     else {
          return stringhash5_find_attach_serial(sht, key, keylen);
     }
}

static inline void sh5_delete_lru(sh5_digest_t * d, uint8_t item) {
     uint32_t data = d[item] & SH5_ANTI_DIGEST_MASK;
     uint32_t i;
     for (i = item; i < 15; i++) {
          d[i] = (d[i] & SH5_EPOCH_MASK) | (SH5_ANTI_EPOCH_MASK & d[i+1]);
     }
     d[15] = (d[15] & SH5_EPOCH_MASK) | (SH5_ANTI_EPOCH_MASK & data);
}

//given an index and digest.. find state data...
static inline int sh5_delete_bucket(sh5_bucket_t * bucket,
                                    uint32_t digest) {
     uint32_t i;

     sh5_digest_t * dp = bucket->digest;
     for (i = 0 ; i < SH5_DEPTH; i++) {
          if (digest == (dp[i] & SH5_DIGEST_MASK)) {
               sh5_delete_lru(bucket->digest, i);
               return 1;
          }
     }
     return 0;
}

//delete record at hashkey
static inline int stringhash5_delete(stringhash5_t * sht, void * key, int keylen) {
     uint32_t h1, h2, d1, d2;

     sh5_gethash(sht, (uint8_t*)key, keylen, &h1, &h2, &d1, &d2);

     if (sh5_delete_bucket(&sht->buckets[h1], d1)) {
          return 1;
     }

     if (sh5_delete_bucket(&sht->buckets[h2], d2)) {
          return 1;
     }
     
     return 0;
}

// NOTE: flush, scour and destroy are serialized by the kids
//       except when flush is used as an inline flush, the table must be
//       protected from access by other kids
static inline void stringhash5_flush(stringhash5_t * sht) {

     SH5_ALL_LOCK(sht)
     sh5_init_buckets(sht);
     SH5_ALL_UNLOCK(sht)
}

static inline void stringhash5_destroy(stringhash5_t * sht) {
     int ret = 0;

     // The table is destroyed by the last thread to arrive
     if (sht->sharelabel) {
          ret = ws_kidshare_unshare(sht->v_type_table, sht->sharelabel);
     }
     if (!sht->sharelabel || !ret) {
          uint64_t expire_cnt = stringhash5_drop_cnt(sht);
          if (expire_cnt) {
               tool_print("sh5 table expire cnt %" PRIu64, expire_cnt);
          }
          if (sht->sharelabel) {
               free(sht->sharedata);
               free((void *)sht->mutex);
               free(sht->curr_lock);
               free(sht->sharelabel);
               sht->sharelabel = NULL;
          }
          if (sht->num_walkers) {
               if (sht->is_shared) {
                    SHT_LOCK_DESTROY(&(sht->walker_mutex))
               }
               free(sht->walker_row);
               uint32_t i;
               for (i = 0; i < sht->num_walkers; i++) {
                    free(sht->walkers[i]);
               }
               free(sht->walkers);
          }
          free(sht->cb_vproc);
          free(sht->buckets);
          free(sht->data);
          free(sht);
     }
     //something BAD happened with shared table accounting, so report this!
     else if (ret < 0) {
          error_print("failed stringhash5_destroy due to invalid ws_kidshare_unshare return");
     }
}

static inline void stringhash5_scour(stringhash5_t * sht,
                                     stringhash5_callback cb, void * vproc) {
     if (!cb) {
          return;
     }

     uint32_t i, j;
     uint8_t * data;
     sh5_bucket_t * bucket;

     SH5_ALL_LOCK(sht)
     for (i = 0 ; i < sht->all_index_size; i++) {
          bucket = &sht->buckets[i];
          data = sht->data +
               ((size_t)sht->data_alloc * ((size_t)i<<SH5_DEPTH_BITS));
          for (j = 0; j < SH5_DEPTH; j++) {
               sh5_digest_t d = bucket->digest[j];
               if (d & SH5_DIGEST_MASK) {
                    sh5_digest_t item = d >> SH5_DATA_BIN;
                    cb(data + item * sht->data_alloc, vproc); 
               } 
          }
     }
     SH5_ALL_UNLOCK(sht)
}

static inline void stringhash5_scour_and_flush(stringhash5_t * sht,
                                               stringhash5_callback cb, void * vproc) {
     if (!cb) {
          return;
     }

     uint32_t i, j;
     uint8_t * data;
     sh5_bucket_t * bucket;

     SH5_ALL_LOCK(sht)
     for (i = 0 ; i < sht->all_index_size; i++) {
          bucket = &sht->buckets[i];
          data = sht->data +
               ((size_t)sht->data_alloc * ((size_t)i<<SH5_DEPTH_BITS));
          for (j = 0; j < SH5_DEPTH; j++) {
               sh5_digest_t d = bucket->digest[j];
               if (d & SH5_DIGEST_MASK) {
                    sh5_digest_t item = d >> SH5_DATA_BIN;
                    cb(data + item * sht->data_alloc, vproc); 
               } 
               //this is the stringhash5_flush (sh5_init_buckets) part
               sht->buckets[i].digest[j] = j << SH5_DATA_BIN;
          }
     }
     SH5_ALL_UNLOCK(sht)
}

static inline int stringhash5_clean_sharing (void * sht_generic, int * index) {
     stringhash5_t * sht = (stringhash5_t *)sht_generic;

     if (work_size == 1 || sht->sharedata->cnt == 1) {

          // free items associated with sharing
          free((void *)sht->mutex);
          sht->mutex = NULL;
          free(sht->sharedata);
          sht->sharedata = NULL;
          free(sht->curr_lock);
          sht->curr_lock = NULL;
          free(sht->sharelabel);
          sht->sharelabel = NULL;

          // reset mutex counter and sharing flag
          sht->max_mutex = 0;
          sht->is_shared = 0;

          // TODO: it would be nice if we had gathered and saved each kid's 
          //       proc->sharelabel address, so that we could free 
          //       proc->sharelabel here and reset it to NULL

          // move the sht from the shared to the local registry
          if (!move_sht_to_local_registry(sht, index)) {
               return 0;
          }
     }
     else {
          (*index)++;
     }

     return 1;
}

// Use this function to serialize both scour and destroy in kid proc_destroy calls
static inline void stringhash5_scour_and_destroy(stringhash5_t * sht,
                                                 stringhash5_callback cb, void * vproc) {
     int ret = 0;

     // The table is destroyed by the last thread to arrive
     if (sht->sharelabel) {
          ret = ws_kidshare_unshare(sht->v_type_table, sht->sharelabel);
     }
     if (!sht->sharelabel || !ret) {
          stringhash5_scour(sht, cb, vproc);
          uint64_t expire_cnt = stringhash5_drop_cnt(sht);
          if (expire_cnt) {
               tool_print("sh5 table expire cnt %" PRIu64, expire_cnt);
          }
          if (sht->sharelabel) {
               free(sht->sharedata);
               free((void *)sht->mutex);
               free(sht->curr_lock);
               free(sht->sharelabel);
               sht->sharelabel = NULL;
          }
          if (sht->num_walkers) {
               if (sht->is_shared) {
                    SHT_LOCK_DESTROY(&(sht->walker_mutex))
               }
               free(sht->walker_row);
               uint32_t i;
               for (i = 0; i < sht->num_walkers; i++) {
                    free(sht->walkers[i]);
               }
               free(sht->walkers);
          }
          free(sht->cb_vproc);
          free(sht->buckets);
          free(sht->data);
          free(sht);
     }
     //something BAD happened with shared table accounting, so report this!
     else if (ret < 0) {
          error_print("failed stringhash5_scour_and_destroy due to invalid ws_kidshare_unshare return");
     }
}

#ifdef DEBUG
static inline void stringhash5_print_table(stringhash5_t * sht) {
     uint32_t i, j;
     sh5_bucket_t * bucket;

     SH5_ALL_LOCK(sht)
     for (i = 0 ; i < sht->all_index_size; i++) {
          bucket = &sht->buckets[i];
          printf("%02u:", i);
          for (j = 0; j < SH5_DEPTH; j++) {
               sh5_digest_t d = bucket->digest[j] & SH5_DIGEST_MASK;
               uint32_t ptr = bucket->digest[j] >> SH5_DATA_BIN;
               printf(" %07x,%02u", d, ptr);
          }
          printf(" ::%02x, %02x\n", sh5_get_epoch(sht, bucket->digest), sht->epoch);
     }
     SH5_ALL_UNLOCK(sht)
}
#endif

//function for reading in a table from a file
static inline stringhash5_t * stringhash5_read(FILE * fp) {
     char sht_id[SHT_ID_SIZE];

     //save initial fp in case we need to back up for an unlabled table
     FILE * fp0 = fp;

     stringhash5_t * sht = (stringhash5_t *)calloc(1, sizeof(stringhash5_t));
     if (!sht) {
          error_print("failed calloc of stringhash5_mway hash table");
          return NULL;
     }

     // check for correct stringhash table type
     if (!fread(&sht_id, SHT_ID_SIZE, 1, fp)) {
          return NULL;
     }
     if (strncmp(sht_id, SHT5_ID, SHT_ID_SIZE) != 0) {

          //failure - this is a stringhash9a table
          if (strncmp(sht_id, SHT9A_ID, SHT_ID_SIZE) == 0) {
               error_print("attempting to read hash table type %s instead of type %s",
                           sht_id, SHT5_ID);
               return NULL;
          }
          //this is an unlabeled table - try reading it
          else {
               status_print("attempting to read unlabeled hash table");

               //reset file ptr to read from the beginning
               fp = fp0;
          }
     }

     if (!fread(&sht->nextval, sizeof(uint64_t), 1, fp)) {
          return NULL;
     }
     if (!fread(&sht->data_alloc, sizeof(size_t), 1, fp)) {
          return NULL;
     }
     if (!fread(&sht->max_records, sizeof(size_t), 1, fp)) {
          return NULL;
     }
     if (!fread(&sht->index_size, sizeof(uint32_t), 1, fp)) {
          return NULL;
     }

     sht->all_index_size = sht->index_size * 2;
     sht->einsert_max = sht->index_size >> 4;

     if (!fread(&sht->hash_seed, sizeof(uint32_t), 1, fp)) {
          return NULL;
     }
     if (!fread(&sht->mask_index, sizeof(uint64_t), 1, fp)) {
          return NULL;
     }
     if (!fread(&sht->epoch, sizeof(uint8_t), 1, fp)) {
          return NULL;
     }

     sht->buckets = (sh5_bucket_t *)calloc(sht->all_index_size,
                                           sizeof(sh5_bucket_t));
     if (!sht->buckets) {
          error_print("failed calloc of stringhash5_mway buckets");
          return NULL;
     }

     sht->data = (uint8_t*)calloc(sht->max_records, sht->data_alloc);
     if (!sht->data) {
          error_print("failed calloc of stringhash5_mway data");
          return NULL;
     }

     uint32_t i = 0;
     while(i < sht->all_index_size) {
          if (feof(fp) || ferror(fp)) {
               stringhash5_destroy(sht);
               return NULL;
          }
          i += fread(sht->buckets + i, sizeof(sh5_bucket_t),
                     sht->all_index_size - i, fp);
     }
     i = 0;
     while(i < sht->max_records) {
          if (feof(fp) || ferror(fp)) {
               stringhash5_destroy(sht);
               return NULL;
          }
          i += fread(sht->data + (size_t)i * sht->data_alloc,
                     sht->data_alloc,
                     sht->max_records - i, fp);
     }

     //allocate the callback data
     sht->cb_vproc = (void **)calloc(work_size, sizeof(void *));
     if (!sht->cb_vproc) {
          error_print("failed calloc of stringhash5_read cb_vproc");
          free(sht);
          return 0;
     }

     // tally up the hash table memory use
     sht->mem_used = sht->all_index_size * sizeof(sh5_bucket_t) + 
                     sht->data_alloc * sht->max_records + sizeof(stringhash5_t);

     return sht;
}

//function for reading in a table from a file
static inline stringhash5_t * stringhash5_read_with_ptrs(FILE * fp, 
                                             sh5dataread_callback_t sh5_dataread_cb) {
     char sht_id[SHT_ID_SIZE];

     //save initial fp in case we need to back up for an unlabled table
     FILE * fp0 = fp;

     stringhash5_t * sht = (stringhash5_t *)calloc(1, sizeof(stringhash5_t));
     if (!sht) {
          error_print("failed calloc of stringhash5_mway hash table");
          return NULL;
     }

     // check for correct stringhash table type
     if (!fread(&sht_id, SHT_ID_SIZE, 1, fp)) {
          return NULL;
     }
     if (strncmp(sht_id, SHT5_ID, SHT_ID_SIZE) != 0) {

          //failure - this is a stringhash9a table
          if (strncmp(sht_id, SHT9A_ID, SHT_ID_SIZE) == 0) {
               error_print("attempting to read hash table type %s instead of type %s",
                           sht_id, SHT5_ID);
               return NULL;
          }
          //this is an unlabeled table - try reading it
          else {
               status_print("attempting to read unlabeled hash table");

               //reset file ptr to read from the beginning
               fp = fp0;
          }
     }

     if (!fread(&sht->nextval, sizeof(uint64_t), 1, fp)) {
          return NULL;
     }
     if (!fread(&sht->data_alloc, sizeof(size_t), 1, fp)) {
          return NULL;
     }
     if (!fread(&sht->max_records, sizeof(size_t), 1, fp)) {
          return NULL;
     }
     if (!fread(&sht->index_size, sizeof(uint32_t), 1, fp)) {
          return NULL;
     }

     sht->all_index_size = sht->index_size * 2;
     sht->einsert_max = sht->index_size >> 4;

     if (!fread(&sht->hash_seed, sizeof(uint32_t), 1, fp)) {
          return NULL;
     }
     if (!fread(&sht->mask_index, sizeof(uint64_t), 1, fp)) {
          return NULL;
     }
     if (!fread(&sht->epoch, sizeof(uint8_t), 1, fp)) {
          return NULL;
     }

     sht->buckets = (sh5_bucket_t *)calloc(sht->all_index_size,
                                           sizeof(sh5_bucket_t));
     if (!sht->buckets) {
          error_print("failed calloc of stringhash5_mway buckets");
          return NULL;
     }

     sht->data = (uint8_t*)calloc(sht->max_records, sht->data_alloc);
     if (!sht->data) {
          error_print("failed calloc of stringhash5_mway data");
          return NULL;
     }

     uint32_t i = 0;
     uint32_t bytes = 0;
     while(i < sht->all_index_size) {
          if (feof(fp) || ferror(fp)) {
               stringhash5_destroy(sht);
               return NULL;
          }
          i += fread(sht->buckets + i, sizeof(sh5_bucket_t),
                     sht->all_index_size - i, fp);
     }
     i = 0;
     while(i < sht->max_records) {
          if (feof(fp) || ferror(fp)) {
               stringhash5_destroy(sht);
               return NULL;
          }
          i += fread(sht->data + (size_t)i * sht->data_alloc,
                     sht->data_alloc,
                     sht->max_records - i, fp);
     }

     //read and store data items associated with pointers in the table
     if (sh5_dataread_cb) {
          bytes = sh5_dataread_cb(sht->data, sht->data_alloc, fp);
          tool_print("sh5_read_w_ptrs data bytes = %u",bytes);
     }

     //allocate the callback data
     sht->cb_vproc = (void **)calloc(work_size, sizeof(void *));
     if (!sht->cb_vproc) {
          error_print("failed calloc of stringhash5_read_with_ptrs cb_vproc");
          free(sht);
          return 0;
     }
     sht->read_success = 1;

     // tally up the hash table memory use
     sht->mem_used = sht->all_index_size * sizeof(sh5_bucket_t) + 
                     sht->data_alloc * sht->max_records + sizeof(stringhash5_t);

     return sht;
}

//function for writing a table to a file
//generally this is serialized, but if it is being done in response
//to an inline flush, then the table must be protected during writing
//against access by other threads
static inline int stringhash5_dump(stringhash5_t * sht, FILE * fp) {
     char sht_id[SHT_ID_SIZE] = SHT5_ID;
     int rtn;

     SH5_ALL_LOCK(sht)
     rtn = fwrite(&sht_id, SHT_ID_SIZE, 1, fp);
     rtn += fwrite(&sht->nextval, sizeof(uint64_t), 1, fp);
     rtn += fwrite(&sht->data_alloc, sizeof(size_t), 1, fp);
     rtn += fwrite(&sht->max_records, sizeof(size_t), 1, fp);
     rtn += fwrite(&sht->index_size, sizeof(uint32_t), 1, fp);
     rtn += fwrite(&sht->hash_seed, sizeof(uint32_t), 1, fp);
     rtn += fwrite(&sht->mask_index, sizeof(uint64_t), 1, fp);
     rtn += fwrite(&sht->epoch, sizeof(uint8_t), 1, fp);

     uint32_t i = 0;
     while(i < sht->all_index_size) {
          i += fwrite(sht->buckets + i, sizeof(sh5_bucket_t),
                        sht->all_index_size - i, fp);
     }
     i = 0;
     while(i < sht->max_records) {
          i += fwrite(sht->data + (size_t)i * sht->data_alloc,
                      sht->data_alloc,
                      sht->max_records - i, fp);
     }
     tool_print("sh5_dump table bytes = %" PRIu64,sht->mem_used);
     SH5_ALL_UNLOCK(sht)

     return 1;
}

static inline int stringhash5_dump_with_ptrs(stringhash5_t * sht, FILE * fp,
                                             sh5datadump_callback_t sh5_datadump_cb) {
     char sht_id[SHT_ID_SIZE] = SHT5_ID;
     int rtn;

     SH5_ALL_LOCK(sht)
     rtn = fwrite(&sht_id, SHT_ID_SIZE, 1, fp);
     rtn += fwrite(&sht->nextval, sizeof(uint64_t), 1, fp);
     rtn += fwrite(&sht->data_alloc, sizeof(size_t), 1, fp);
     rtn += fwrite(&sht->max_records, sizeof(size_t), 1, fp);
     rtn += fwrite(&sht->index_size, sizeof(uint32_t), 1, fp);
     rtn += fwrite(&sht->hash_seed, sizeof(uint32_t), 1, fp);
     rtn += fwrite(&sht->mask_index, sizeof(uint64_t), 1, fp);
     rtn += fwrite(&sht->epoch, sizeof(uint8_t), 1, fp);

     uint32_t i = 0;
     uint32_t bytes = 0;
     while(i < sht->all_index_size) {
          i += fwrite(sht->buckets + i, sizeof(sh5_bucket_t),
                        sht->all_index_size - i, fp);
     }
     i = 0;
     while(i < sht->max_records) {
          i += fwrite(sht->data + (size_t)i * sht->data_alloc,
                      sht->data_alloc,
                      sht->max_records - i, fp);
     }
     tool_print("sh5_dump_w_ptrs table bytes = %" PRIu64,sht->mem_used);

     //write data items associated with pointers in the table
     if (sh5_datadump_cb) {
          bytes = sh5_datadump_cb(sht->data, sht->max_records, sht->data_alloc, fp);
          tool_print("sh5_dump_w_ptrs data bytes = %u",bytes);
     }

     SH5_ALL_UNLOCK(sht)

     return 1;
}

static inline void * stringhash5_find_loc(stringhash5_t * sht, ws_hashloc_t * loc) {
     if (loc && loc->len) {
          return stringhash5_find(sht, (void *)loc->offset, loc->len);
     }
     return NULL;
}

static inline void * stringhash5_find_wsdata(stringhash5_t * sht,
                                             wsdata_t * wsd) {
     ws_hashloc_t * loc = wsd->dtype->hash_func(wsd);
     if (loc) {
          return stringhash5_find(sht, (void *)loc->offset, loc->len);
     }
     return NULL;
}


static inline void * stringhash5_find_attach_loc(stringhash5_t * sht, ws_hashloc_t * loc) {
     if (loc && loc->len) {
          return stringhash5_find_attach(sht, (void *)loc->offset, loc->len);
     }
     return NULL;
}

static inline void * stringhash5_find_attach_wsdata(stringhash5_t * sht,
                                                    wsdata_t * wsd) {
     ws_hashloc_t * loc = wsd->dtype->hash_func(wsd);
     if (loc) {
          return stringhash5_find_attach(sht, (void *)loc->offset, loc->len);
     }
     return NULL;
}


static inline int stringhash5_delete_loc(stringhash5_t * sht, ws_hashloc_t * loc) {
     if (loc && loc->len) {
          return stringhash5_delete(sht, (void *)loc->offset, loc->len);
     }
     return 0;
}

static inline int stringhash5_delete_wsdata(stringhash5_t * sht,
                                               wsdata_t * wsd) {
     ws_hashloc_t * loc = wsd->dtype->hash_func(wsd);
     if (loc) {
          return stringhash5_delete(sht, (void *)loc->offset, loc->len);
     }
     return 0;
}

//define prototype for walking records in a table a bit at a time..
// a return 0 deletes record.. a return 1 leaves data
//look up to 16 records at a time
static inline stringhash5_walker_t * stringhash5_walker_init(stringhash5_t * sht,
                                                             stringhash5_visit cb,
                                                             void * vproc) {

     stringhash5_walker_t * w =
          (stringhash5_walker_t *)calloc(1, sizeof(stringhash5_walker_t));
     if (!w) {
          error_print("failed calloc of stringhash5_mway walker");
          return NULL;
     }

     w->sht = sht;
     w->callback = cb;
     w->cb_vproc = vproc;
     w->walker_id = sht->num_walkers;

     // track the number of walkers associated with this hash table
     sht->num_walkers++;

     // grow the array of those walkers, which is used to clean up in stringhash5_destroy
     sht->walkers =
          (stringhash5_walker_t **)realloc(sht->walkers, sizeof(stringhash5_walker_t *)*sht->num_walkers);
     if (!sht->walkers) {
          error_print("failed calloc of stringhash5_mway walkers");
          return NULL;
     }

     // store the walker_row for each walker for use in stringhash5_walker_next
     sht->walker_row =
          (uint32_t *)realloc(sht->walker_row, sizeof(uint32_t)*sht->num_walkers);
     if (!sht->walker_row) {
          error_print("failed calloc of stringhash5_mway walker");
          return NULL;
     }

     // init the new entries
     sht->walkers[sht->num_walkers-1] = w; // used in stringhash5_destroy to clean up
     sht->walker_row[sht->num_walkers-1] = 0;

     // set up a mutex for the sh5 walker(s)
     if (sht->is_shared) {
          SHT_LOCK_INIT(&(w->sht->walker_mutex),mutex_attr);
     }

     return w;
}

static inline int stringhash5_walker_next(stringhash5_walker_t * w) {

     if (!w->callback || !w->sht) {
          return 0;
     }

     // NOTE:  we need to lock the walker AND the affected hashtable row for inline flushers 
     //        Otherwise, the use of stringhash5_walker_next is serialized 
     //        by the serialization of kid proc_flush calls.
     if (w->sht->is_shared) {
          SHT_LOCK(&(w->sht->walker_mutex))
     }

     uint32_t j = w->walker_id;

     if (w->sht->is_shared) {
          SH5_LOCK(w->sht,w->sht->walker_row[j])
     }

     sh5_digest_t * dp = w->sht->buckets[w->sht->walker_row[j]].digest;

     int i;
     int calls = 0;
     for (i = 0; i < SH5_DEPTH; i++) {
          //check if digest > 0
          if (dp[i] & SH5_DIGEST_MASK) {
               uint32_t databin = dp[i]>>SH5_DATA_BIN;

               void * data = w->sht->data + 
                    (w->sht->data_alloc * ((size_t)databin
                                           + ((size_t)w->sht->walker_row[j] << SH5_DEPTH_BITS)));
               if (!w->callback(data, w->cb_vproc)) {
                    //delete data here by setting digest =0
                    dp[i] &= SH5_ANTI_DIGEST_MASK;
               }
               calls++;
          }
     }

     //set up structure for next read
     w->sht->walker_row[j]++;

     if (w->sht->walker_row[j] >= w->sht->all_index_size) {
          w->sht->walker_row[j] = 0;
          w->loop++;
     } 

     if (w->sht->is_shared) {
          SH5_UNLOCK(w->sht,w->sht->walker_row[j]-1)
          SHT_UNLOCK(&(w->sht->walker_mutex))
     }

     return calls;
}

static inline int stringhash5_walker_destroy(stringhash5_walker_t * w) {
     fprintf(stderr, "WARNING: function stringhash5_walker_destroy is unnecessary.\n");
     fprintf(stderr, "WARNING: walker memory is now freed by stringhash5_destroy.\n");
     fprintf(stderr, "WARNING: please remove invocations of stringhash5_walker_destroy from your source.\n");
     return 0;
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _STRINGHASH5_H

