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

//STRINGHASH9A - a existance check table.. like an expiring bloom filter.. only not.
// uses buckets each with 21 items in it.. it expires 
#ifndef _STRINGHASH9A_H
#define _STRINGHASH9A_H

//#define DEBUG 1
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include "evahash64.h"
#include "sysutil.h"
#include "sht_registry.h"
#include "tool_print.h"
#include "error_print.h"
#include "shared/kidshare.h"
#include "shared/sht_lock_init.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

//macros
#ifndef SHT_ID_SIZE
#define SHT_ID_SIZE 13
#endif // SHT_ID_SIZE
#define SHT9A_ID  "STRINGHASH9A"
#define SHT5_ID   "STRINGHASH5 "
#define SH9A_DEPTH 16 //should be matched with cache line size..

#define SH9A_DIGEST_MASK 0xFFFFFF00U
#define SH9A_DIGEST_MASK2 0x00FFFFFFU
#define SH9A_DIGEST_BITS 24
#define SH9A_DIGEST_SHIFT 8
#define SH9A_DIGEST_DEFAULT 0x00000100U
#define SH9A_LEFTOVER_MASK 0x000000FFU

//macro for specifying uint64_t in a print statement.. architecture dependant..
#ifndef PRIu64
#if __WORDSIZE == 64
#define PRIu64 "lu"
#else
#define PRIu64 "llu"
#endif
#endif

//macros internal to stringhash9a
#if defined(WS_PTHREADS) && !defined(OWMR_TABLES)
#define MUTEX_INDEX_SHIFT 5
#define SH9A_SHIFT_KEY(h1,k1) uint32_t k1 = (h1) >> MUTEX_INDEX_SHIFT;
#define SH9A_LOCK(sht,x) do { \
    SHT_LOCK(&((sht)->mutex[(x)])) \
} while (0);
#define SH9A_UNLOCK(sht,x) SHT_UNLOCK(&((sht)->mutex[(x)]))
#define SH9A_LOCK_PAIR(sht,k1,k2) \
     if ((k1) < (k2)) { \
          SH9A_LOCK((sht), k1) \
          SH9A_LOCK((sht), k2) \
     } \
     else if ((k1) > (k2)) { \
          SH9A_LOCK((sht), k2) \
          SH9A_LOCK((sht), k1) \
     } \
     else { \
          SH9A_LOCK((sht), k1) \
     }
#define SH9A_UNLOCK_PAIR(sht,k1,k2) \
     if ((k1) < (k2)) { \
          SH9A_UNLOCK((sht), k2) \
          SH9A_UNLOCK((sht), k1) \
     } \
     else if ((k1) > (k2)) { \
          SH9A_UNLOCK((sht), k1) \
          SH9A_UNLOCK((sht), k2) \
     } \
     else { \
          SH9A_UNLOCK((sht), k1) \
     }
#define SH9A_LOCK_IDX(sht,x) do { \
    SHT_LOCK(&((sht)->mutex[(x)])) \
} while (0);
#define SH9A_UNLOCK_IDX(sht,x) SHT_UNLOCK(&((sht)->mutex[(x)]))
#define SH9A_LOCK_ALL(sht) \
{ \
      uint32_t j; \
      for (j = 0; j < (sht)->max_mutex; j++) { \
           SH9A_LOCK_IDX((sht), j) \
      } \
}
#define SH9A_UNLOCK_ALL(sht) \
{ \
      uint32_t j; \
      for (j = 0; j < (sht)->max_mutex; j++) { \
           SH9A_UNLOCK_IDX((sht), j) \
      } \
}
#define SH9A_LOCK_DESTROY(sht,x) SHT_LOCK_DESTROY(&((sht)->mutex[x]))

#else
#define SH9A_SHIFT_KEY(h1,k1)
#define SH9A_LOCK(sht,x)
#define SH9A_UNLOCK(sht,x)
#define SH9A_LOCK_PAIR(sht,k1,k2)
#define SH9A_UNLOCK_PAIR(sht,k1,k2)
#define SH9A_LOCK_IDX(sht,x)
#define SH9A_UNLOCK_IDX(sht,x)
#define SH9A_LOCK_ALL(sht)
#define SH9A_UNLOCK_ALL(sht)
#define SH9A_LOCK_DESTROY(sht,x)
#endif // OWMR_TABLES

#ifdef WS_PTHREADS
#ifdef WS_LOCK_DBG
#define sh9a_mutex_t pthread_mutex_t
#else
#define sh9a_mutex_t pthread_spinlock_t
#endif // WS_LOCK_DBG

//dummy "mutex" type for serial
#else
#define sh9a_mutex_t int
#endif // WS_PTHREADS

typedef struct _share9a_t {
     int cnt;
     void * table; //same as what is in proc_instance
} share9a_t;

//28 bits of digest, 4 bits of pointer to data
//items in list sorted LRU
typedef struct _sh9a_bucket_t {
     uint32_t digest[SH9A_DEPTH];
} sh9a_bucket_t;

typedef struct _stringhash9a_sh_opts_t {
     int readonly;
     const char * open_table;
} stringhash9a_sh_opts_t;

typedef struct _stringhash9a_t {
     sh9a_bucket_t * buckets;
     uint32_t max_records;
     uint64_t mem_used;
     uint32_t ibits;
     uint32_t index_size;
     uint32_t hash_seed;
     uint64_t drops;
     uint8_t epoch;
     uint32_t insert_cnt;
     uint32_t max_insert_cnt;
     uint64_t mask_index;
     uint32_t table_bit;

     int is_shared;
     char * sharelabel;
     share9a_t * sharedata;
     void * v_type_table;
     uint32_t max_mutex;
#ifndef OWMR_TABLES
     sh9a_mutex_t * mutex;
#endif // OWMR_TABLES
} stringhash9a_t;

//prototypes
//DEPRECATED
static inline int stringhash9a_open_table(stringhash9a_t **, const char *);
//CURRENT
static inline int stringhash9a_open_sht_table(stringhash9a_t **, uint32_t, 
                                              stringhash9a_sh_opts_t *);
static inline stringhash9a_t * stringhash9a_create(uint32_t, uint32_t);
//DEPRECATED
static inline int stringhash9a_create_shared(void *, void **, const char *, 
                                             uint32_t, int *, int, void *);
//CURRENT
static inline int stringhash9a_create_shared_sht(void *, void **, const char *, 
                                                 uint32_t, int *, 
                                                 stringhash9a_sh_opts_t *);
static inline int stringhash9a_check(stringhash9a_t *, void *, int);
static inline uint64_t stringhash9a_drop_cnt(stringhash9a_t *);
static inline int stringhash9a_set(stringhash9a_t *, void *, int);
static inline int stringhash9a_delete(stringhash9a_t *, void *, int);
static inline void stringhash9a_flush(stringhash9a_t *);
static inline void stringhash9a_destroy(stringhash9a_t *);
static inline stringhash9a_t * stringhash9a_read(FILE *);
static inline int stringhash9a_dump(stringhash9a_t *, FILE *);
static inline int stringhash9a_check_loc(stringhash9a_t *, ws_hashloc_t *);
static inline int stringhash9a_check_wsdata(stringhash9a_t *, wsdata_t *);
static inline int stringhash9a_set_loc(stringhash9a_t * sht, ws_hashloc_t *);
static inline int stringhash9a_set_wsdata(stringhash9a_t *, wsdata_t *);
static inline int stringhash9a_delete_loc(stringhash9a_t *, ws_hashloc_t *);
static inline int stringhash9a_delete_wsdata(stringhash9a_t *, wsdata_t *);


//compute log2 of an unsigned int
// by Eric Cole - http://graphics.stanford.edu/~seander/bithacks.htm
static inline uint32_t sh9a_uint32_log2(uint32_t v) {
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

#if defined(WS_PTHREADS) && !defined(OWMR_TABLES)
static inline int sh9a_create_mutex(stringhash9a_t * sht) {

     //every 16 indexes we have a mutex
     sht->max_mutex = ((sht->index_size * 2) >> MUTEX_INDEX_SHIFT);
     if (!sht->max_mutex) {
          sht->max_mutex = 1;
     }
     sht->mutex = (sh9a_mutex_t *)calloc(sht->max_mutex, sizeof(sh9a_mutex_t));
     if (!sht->mutex) {
          free(sht);
          error_print("failed calloc of stringhash9a mutex set");
          return 0;
     }
     uint32_t i;
     for (i = 0; i < sht->max_mutex; i++) {
          SHT_LOCK_INIT(&sht->mutex[i],mutex_attr);
     }

     return 1;
}
#endif // WS_PTHREADS && !OWMR_TABLES

static inline int check_sh9a_max_records(uint32_t max_records) {

     //note the minimum table size for sh9a
     if(max_records < 84) {
          tool_print("Caution: minimum size for sh9a max_records is 84...resizing\n");
          // the minimum size of 84 will be enforced by the sh9a_create_ibits() below
     }

     // 42 == 21 items per bucket, 2 tables 
     uint32_t ibits = sh9a_uint32_log2((uint32_t)(max_records/42)) + 1;
     int mxr = (1<<(ibits)) * 21 * 2;

     return mxr;
}

static inline void stringhash9a_sh_opts_alloc(stringhash9a_sh_opts_t ** sh9a_sh_opts) {
     *sh9a_sh_opts = (stringhash9a_sh_opts_t *)calloc(1,sizeof(stringhash9a_sh_opts_t));
}

static inline void stringhash9a_sh_opts_free(stringhash9a_sh_opts_t * sh9a_sh_opts) {
     free(sh9a_sh_opts);
}

//DEPRECATED
static inline int stringhash9a_open_table(stringhash9a_t ** table, const char * open_table) {

     stringhash9a_sh_opts_t * sh9a_sh_opts;
     int ret;
     tool_print("WARNING: 'stringhash9a_open_table' HAS BEEN DEPRECATED. PLEASE USE 'stringhash9a_open_sht_table' INSTEAD.");

     //checks against max_records won't be done for the deprecated functions
     uint32_t max_records = 0;

     //calloc shared sh9a option struct
     stringhash9a_sh_opts_alloc(&sh9a_sh_opts);

     //set shared sh9a option fields
     sh9a_sh_opts->open_table = open_table;

     ret = stringhash9a_open_sht_table(table, max_records, sh9a_sh_opts);

     //free shared sh9a option struct
     stringhash9a_sh_opts_free(sh9a_sh_opts);

     return ret;
}

//CURRENT
static inline int stringhash9a_open_sht_table(stringhash9a_t ** table, uint32_t max_records, 
                                              stringhash9a_sh_opts_t * sh9a_sh_opts) {
     if (sh9a_sh_opts->open_table) {
          FILE * fp;
          fp = sysutil_config_fopen(sh9a_sh_opts->open_table, "r");
          if (!fp) {
               tool_print("unable to open table %s", sh9a_sh_opts->open_table);
               tool_print("ignoring load failure -- starting empty table");
          }
          else {
               *table = stringhash9a_read(fp);
               sysutil_config_fclose(fp);
               if (!*table) {
                    error_print("unable to read stringhash9a table");
                    return 0;
               }
               tool_print("finished loading table %s", sh9a_sh_opts->open_table);

               if (!enroll_in_sht_registry(*table, "sh9a", ((stringhash9a_t *)(*table))->mem_used, 
                                           ((stringhash9a_t *)(*table))->hash_seed)) {
                    return 0;
               }

               //give an error if max_records does not match the input value for this thread
               if (max_records && ((stringhash9a_t *)(*table))->max_records != 
                                   check_sh9a_max_records(max_records)) {
                    error_print("stringhash9a table max_records %d not equal to converted input value %d",
                               ((stringhash9a_t *)(*table))->max_records, 
                               check_sh9a_max_records(max_records));
                    return 0;
               }

               return 1;
          }
     }

     return 0;
}

static inline stringhash9a_t * sh9a_create_ibits(uint32_t ibits) {
     stringhash9a_t * sht;
     sht = (stringhash9a_t *)calloc(1, sizeof(stringhash9a_t));
     if (!sht) {
          error_print("failed calloc of stringhash9a hash table");
          return NULL;
     }

     sht->ibits = ibits;
     sht->index_size = 1<<(ibits);

     //do some bounds checking on table size
     if (sht->index_size > 0x7FFFFFFF) {
          return NULL;
     }
     sht->max_insert_cnt = sht->index_size >> 4;
     sht->table_bit = 1<<(ibits);
     sht->mask_index = ((uint64_t)~0)>>(64-(ibits));
     sht->max_records = sht->index_size * 21 * 2;

     sht->hash_seed = (uint32_t)rand();
     sht->epoch = 1;

     // now to allocate memory...
     sht->buckets = (sh9a_bucket_t *)calloc(sht->index_size * 2,
                                            sizeof(sh9a_bucket_t));

     if (!sht->buckets) {
          free(sht);
          error_print("failed calloc of stringhash9a buckets");
          return NULL;
     }

     // tally up the hash table memory use
     sht->mem_used = sizeof(stringhash9a_t) + 2 * (uint64_t)sht->index_size * sizeof(sh9a_bucket_t);

     return sht;
}

// At long last - the first parameter has a use!  We use the first argument to signal that
// stringhash9a_create is being called to create a shared table (i.e. the call is made
// from stringhash9a_create_shared)
static inline stringhash9a_t * stringhash9a_create(uint32_t is_shared, uint32_t max_records) {
     stringhash9a_t * sht;

     //note the minimum table size for sh9a
     if(max_records < 84) {
          tool_print("Caution: minimum size for sh9a max_records is 84...resizing\n");
          // the minimum size of 84 will be enforced by the sh9a_create_ibits() below
     }

     //create the stringhash9a table from scratch
     // 42 == 21 items per bucket, 2 tables 
     uint32_t ibits = sh9a_uint32_log2((uint32_t)(max_records/42)) + 1;

     sht = sh9a_create_ibits(ibits);
     if (!(sht)) {
          error_print("stringhash9a_create failed");
          return NULL;
     }

#ifndef TOOL_NAME
     if (!is_shared) {
           if (!enroll_in_sht_registry(sht, "sh9a", sht->mem_used, sht->hash_seed)) {
                return NULL;
           }
     }
#endif // TOOL_NAME

     return sht;
}

static inline int stringhash9a_check_params(void ** table, uint32_t max_records) {

     //give an error if max_records does not match the input value for this thread
     if (((stringhash9a_t *)(*table))->max_records != check_sh9a_max_records(max_records)) {
          error_print("stringhash9a table max_records %d not equal to converted input value %d",
                     ((stringhash9a_t *)(*table))->max_records, 
                     check_sh9a_max_records(max_records));
          return 0;
     }

     return 1;
}

//DEPRECATED
static inline int stringhash9a_create_shared(void * v_type_table, void ** table, 
                                             const char * sharelabel, uint32_t max_records, 
                                             int * sharer_id, int readonly, 
                                             void * open_table) {

     stringhash9a_sh_opts_t * sh9a_sh_opts;
     int ret;
     tool_print("WARNING: 'stringhash9a_create_shared' HAS BEEN DEPRECATED. PLEASE USE 'stringhash9a_create_shared_sht' INSTEAD.");

     //calloc shared sh9a option struct
     stringhash9a_sh_opts_alloc(&sh9a_sh_opts);

     //set shared sh9a option fields
     sh9a_sh_opts->readonly = readonly;
     sh9a_sh_opts->open_table = (char *)open_table;

     ret = stringhash9a_create_shared_sht(v_type_table, table, sharelabel, max_records, 
                                          sharer_id, sh9a_sh_opts);

     //free shared sh9a option struct
     stringhash9a_sh_opts_free(sh9a_sh_opts);

     return ret;
}

//CURRENT
static inline int stringhash9a_create_shared_sht(void * v_type_table, void ** table, 
                                                 const char * sharelabel, 
                                                 uint32_t max_records, int * sharer_id, 
                                                 stringhash9a_sh_opts_t * sh9a_sh_opts) {
     int shid;
     
     //sharelabel error checks:  null sharelabel
     if (!sharelabel) {
          error_print("this kid is not sharing stringhash9a, please specify share with -J option");
          error_print("also, this kid should not have entered here without -J set");
          return 0;
     } 

     //see if structure is already available at label
     share9a_t * sharedata = ws_kidshare_get(v_type_table, sharelabel);

     if (sharedata) {
          *table = sharedata->table;
          if (!*table) {
               error_print("unable to share a proper stringhash9a table");
               return 0;
          }
          sharedata->cnt++;
          tool_print("this is kid #%d to share stringhash9a table at label %s",
                     sharedata->cnt, sharelabel);
          shid = sharedata->cnt;

          //give an error if max_records does not match the input value for this thread
          if (!sh9a_sh_opts->readonly) {
               if (!stringhash9a_check_params(table, max_records)) {
                    return 0;
               }
          }
     }
     //no sharing at label yet..
     else {
          tool_print("this is the first kid to share stringhash9a table at label %s", sharelabel);

          //read the stringhash9a table from the open_table file
          if (sh9a_sh_opts->open_table) {
               FILE * fp;
               fp = sysutil_config_fopen((const char *)sh9a_sh_opts->open_table, "r");
               if (!fp) {
                    if (sh9a_sh_opts->readonly) {
                         error_print("stringhash9a_create_shared unable to load table %s", 
                                     (char *)sh9a_sh_opts->open_table);
                         return 0;
                    }
                    tool_print("stringhash9a_create_shared unable to open table %s", 
                               (char *)sh9a_sh_opts->open_table);
                    tool_print("stringhash9a_create_shared ignoring load failure -- starting empty table");
                    *table = stringhash9a_create(1, max_records);
                    if (!*table) {
                         error_print("stringhash9a_create_shared unable to initialize stringhash9a table");
                         return 0;
                    }
               }
               else {
                    //read the stringhash9a table
                    *table = stringhash9a_read(fp);
                    sysutil_config_fclose(fp);
                    if (!*table) {
                         if (sh9a_sh_opts->readonly) {
                              error_print("stringhash9a_create_shared unable to read stringhash9a table");
                              return 0;
                         }
                         tool_print("stringhash9a_create_shared unable to read stringhash9a table");
                         tool_print("stringhash9a_create_shared ignoring load failure -- starting empty table");
                         *table = stringhash9a_create(1, max_records);
                         if (!*table) {
                              error_print("stringhash9a_create_shared unable to initialize stringhash9a table");
                              return 0;
                         }
                    }
                    else {
                         //check table size against input parameters
                         if (!sh9a_sh_opts->readonly) {
                              if (!stringhash9a_check_params(table, max_records)) {
                                   return 0;
                              }
                         }
                         tool_print("finished loading table %s", (char *)sh9a_sh_opts->open_table);
                    }
               }
          }
          else {
               //create the stringhash9a table from scratch
               *table = stringhash9a_create(1, max_records);
               if (!*table) {
                    error_print("unable to initialize stringhash9a shared table");
                    return 0;
               }
          }

          // set up mutex locks for the table
#if defined(WS_PTHREADS) && !defined(OWMR_TABLES)
          if (!sh9a_create_mutex(*table)) {
               return 0;
          }
          ((stringhash9a_t *)(*table))->mem_used += sizeof(sh9a_mutex_t *) * 
                                                    (uint64_t)((stringhash9a_t *)(*table))->max_mutex;
#endif // WS_PTHREADS && !OWMR_TABLES

          if (!enroll_shared_in_sht_registry(*table, "sh9a shared", sharelabel, 
                                             ((stringhash9a_t *)(*table))->mem_used,
                                             ((stringhash9a_t *)(*table))->hash_seed)) {
               error_print("unable to enroll stringhash9a shared table");
               return 0;
          }
          tool_print("sh9a memory used = %"PRIu64, ((stringhash9a_t *)(*table))->mem_used);

          //create the sharedata
          sharedata = (void *)calloc(1,sizeof(share9a_t));
          if (!sharedata) {
               error_print("failed stringhash9a_create_shared calloc of sharedata");
               return 0;
          }

          //load up the sharedata
          sharedata->table = *table;
          sharedata->cnt++;
          ((stringhash9a_t *)(*table))->sharelabel = strdup(sharelabel);
          ((stringhash9a_t *)(*table))->sharedata = sharedata;
          ((stringhash9a_t *)(*table))->v_type_table = v_type_table;
          ((stringhash9a_t *)(*table))->is_shared = 1;
          shid = 0;

          //actually share structure
          ws_kidshare_put(v_type_table, sharelabel, sharedata);
     }

     // if a memory location is supplied, pass the sharer_id back to the kid
     if (sharer_id) {
          *sharer_id = shid;
     }

     return 1; 
}

//steal bytes from digests.. populate
#define sh9a_build_leftover(i, lookup, digest) do { \
    if (i <= 14) { \
	const uint32_t partial = (digest) & SH9A_LEFTOVER_MASK; \
	const unsigned int shift = 8 * (((i) % 3) + 1); \
	const unsigned int idx = (i)/3; \
	(lookup)[idx] |= partial << shift; \
    } \
} while (0)

//could be optimized a bit more for loop unrolling
static inline void sh9a_sort_lru_lower(uint32_t * d, uint8_t mru) {
     uint32_t a = d[mru] & SH9A_DIGEST_MASK;

#define SH9A_SET_DIGEST(X,Y) X = ((X & SH9A_LEFTOVER_MASK) | (Y & SH9A_DIGEST_MASK))
     switch(mru) {
     case 15:
          SH9A_SET_DIGEST(d[15],d[14]);
     case 14:
          SH9A_SET_DIGEST(d[14],d[13]);
     case 13:
          SH9A_SET_DIGEST(d[13],d[12]);
     case 12:
          SH9A_SET_DIGEST(d[12],d[11]);
     case 11:
          SH9A_SET_DIGEST(d[11],d[10]);
     case 10:
          SH9A_SET_DIGEST(d[10],d[9]);
     case 9:
          SH9A_SET_DIGEST(d[9],d[8]);
     case 8:
          SH9A_SET_DIGEST(d[8],d[7]);
     case 7:
          SH9A_SET_DIGEST(d[7],d[6]);
     case 6:
          SH9A_SET_DIGEST(d[6],d[5]);
     case 5:
          SH9A_SET_DIGEST(d[5],d[4]);
     case 4:
          SH9A_SET_DIGEST(d[4],d[3]);
     case 3:
          SH9A_SET_DIGEST(d[3],d[2]);
     case 2:
          SH9A_SET_DIGEST(d[2],d[1]);
     case 1:
          SH9A_SET_DIGEST(d[1],d[0]);
     }

     d[0] &= SH9A_LEFTOVER_MASK;
     d[0] |= a;
}

static inline void print_lru(uint32_t * d, uint32_t m) {
     fprintf(stdout, "%u", m);
     uint32_t i;
     for (i = 0; i < 15; i++) {
          fprintf(stdout, " %x", (d[i] & SH9A_DIGEST_MASK)>>8);
     }
     for (i = 0; i < 5; i++) {
          uint32_t u = (d[i*3] & SH9A_LEFTOVER_MASK);
          u |= (d[(i*3)+1] & SH9A_LEFTOVER_MASK) << 8;
          u |= (d[(i*3)+2] & SH9A_LEFTOVER_MASK) << 16;
          fprintf(stdout, " %x", u);
     }
     fprintf(stdout, "\n");
}

//move mru item to front..  ridiculous code bloat - but it's supposed
// to loop unravel
static inline void sh9a_sort_lru_upper(uint32_t * d, uint8_t mru) {
     uint32_t a = 0;
     uint32_t i;

     switch (mru) {
     case 16:
          a = ((d[0] & SH9A_LEFTOVER_MASK)<<8) + 
               ((d[1] & SH9A_LEFTOVER_MASK)<<16) +
               ((d[2] & SH9A_LEFTOVER_MASK)<<24);
          break;
     case 17:
          a = ((d[3] & SH9A_LEFTOVER_MASK)<<8) + 
               ((d[4] & SH9A_LEFTOVER_MASK)<<16) +
               ((d[5] & SH9A_LEFTOVER_MASK)<<24);
          break;
     case 18:
          a = ((d[6] & SH9A_LEFTOVER_MASK)<<8) + 
               ((d[7] & SH9A_LEFTOVER_MASK)<<16) +
               ((d[8] & SH9A_LEFTOVER_MASK)<<24);
          break;
     case 19:
          a = ((d[9] & SH9A_LEFTOVER_MASK)<<8) + 
               ((d[10] & SH9A_LEFTOVER_MASK)<<16) +
               ((d[11] & SH9A_LEFTOVER_MASK)<<24);
          break;
     case 20:
          a = ((d[12] & SH9A_LEFTOVER_MASK)<<8) + 
               ((d[13] & SH9A_LEFTOVER_MASK)<<16) +
               ((d[14] & SH9A_LEFTOVER_MASK)<<24);
          break;
     }

     switch (mru) {
     case 20:
          d[12] &= SH9A_DIGEST_MASK;
          d[12] |= d[9] & SH9A_LEFTOVER_MASK;
          d[13] &= SH9A_DIGEST_MASK;
          d[13] |= d[10] & SH9A_LEFTOVER_MASK;
          d[14] &= SH9A_DIGEST_MASK;
          d[14] |= d[11] & SH9A_LEFTOVER_MASK;
          //nobreak;
     case 19:
          d[9] &= SH9A_DIGEST_MASK;
          d[9] |= d[6] & SH9A_LEFTOVER_MASK;
          d[10] &= SH9A_DIGEST_MASK;
          d[10] |= d[7] & SH9A_LEFTOVER_MASK;
          d[11] &= SH9A_DIGEST_MASK;
          d[11] |= d[8] & SH9A_LEFTOVER_MASK;
          //nobreak;
     case 18:
          d[6] &= SH9A_DIGEST_MASK;
          d[6] |= d[3] & SH9A_LEFTOVER_MASK;
          d[7] &= SH9A_DIGEST_MASK;
          d[7] |= d[4] & SH9A_LEFTOVER_MASK;
          d[8] &= SH9A_DIGEST_MASK;
          d[8] |= d[5] & SH9A_LEFTOVER_MASK;
          //nobreak;
     case 17:
          d[3] &= SH9A_DIGEST_MASK;
          d[3] |= d[0] & SH9A_LEFTOVER_MASK;
          d[4] &= SH9A_DIGEST_MASK;
          d[4] |= d[1] & SH9A_LEFTOVER_MASK;
          d[5] &= SH9A_DIGEST_MASK;
          d[5] |= d[2] & SH9A_LEFTOVER_MASK;
          //no break;
     }

     uint32_t b = d[15];
     for (i = 15; i > 0; i--) {
          d[i] &= SH9A_LEFTOVER_MASK;
          d[i] |= d[i-1] & SH9A_DIGEST_MASK;
     }
     d[0] &= SH9A_DIGEST_MASK;
     d[0] |= (b & 0xFF00)>>8;
     d[1] &= SH9A_DIGEST_MASK;
     d[1] |= (b & 0xFF0000)>>16;
     d[2] &= SH9A_DIGEST_MASK;
     d[2] |= (b & 0xFF000000)>>24;

     d[0] &= SH9A_LEFTOVER_MASK;
     d[0] |= a; 
}

// GCC pragma diagnostics only work for gcc 4.2 and later
#define GCC_VERSION (  __GNUC__       * 10000  \
                                              + __GNUC_MINOR__ *   100   \
                                              + __GNUC_PATCHLEVEL__)
#if GCC_VERSION >= 40200
#pragma GCC diagnostic ignored "-Wuninitialized"
#endif
#undef GCC_VERSION

//given an index and bucket.. find state data...
static inline int sh9a_lookup_bucket(sh9a_bucket_t * bucket,
                                     uint32_t digest) {
     int i;

     uint32_t * dp = bucket->digest;
     uint32_t leftover[5] = {0};
     for (i = 0 ; i < SH9A_DEPTH; i++) {
          if (digest == (dp[i] & SH9A_DIGEST_MASK)) {
               //print_lru(dp, i);
#ifndef OWMR_TABLES
               sh9a_sort_lru_lower(dp, i);
#endif // WS_PTHREADS && !OWMR_TABLES
               //print_lru(dp, i);
               return 1;
          }
          sh9a_build_leftover(i, leftover, dp[i]);
     }
     for (i = 0; i < 5; i++) {
          if (digest == leftover[i]) {
               sh9a_sort_lru_upper(dp, i+16);
               return 1;
          }
     }
     return 0;
}

// lookup bucket.. count zeros in bucket
static inline int sh9a_lookup_bucket2(sh9a_bucket_t * bucket,
                                      uint32_t digest, uint32_t * zeros) {
     int i;

     *zeros = 0;
     uint32_t * dp = bucket->digest;
     uint32_t leftover[5] = {0};
     uint32_t dcmp;
     for (i = 0 ; i < SH9A_DEPTH; i++) {
          dcmp = dp[i] & SH9A_DIGEST_MASK;
          if (digest == dcmp) {
               //print_lru(dp, i);
               sh9a_sort_lru_lower(dp, i);
               //print_lru(dp, i);
               return 1;
          }
          *zeros += dcmp ? 0 : 1;
          sh9a_build_leftover(i, leftover, dp[i]);
     }
     for (i = 0; i < 5; i++) {
          if (digest == leftover[i]) {
               sh9a_sort_lru_upper(dp, i+16);
               return 1;
          }
          *zeros += leftover[i] ? 0 : 1;
     }
     return 0;
}

#define SH9A_PERMUTE1 0xed31952d18a569ddULL
#define SH9A_PERMUTE2 0x94e36ad1c8d2654bULL
static inline void sh9a_gethash(stringhash9a_t * sht,
                                uint8_t * key, uint32_t keylen,
                                uint32_t *h1, uint32_t *h2,
                                uint32_t *pd1, uint32_t *pd2) {

     uint64_t m = evahash64(key, keylen, sht->hash_seed);
     uint64_t p1 = m * SH9A_PERMUTE1;
     uint64_t p2 = m * SH9A_PERMUTE2;
     uint64_t lh1, lh2;
     lh1 = (p1 >> SH9A_DIGEST_BITS) & sht->mask_index;
     lh2 = ((p2 >> SH9A_DIGEST_BITS) & sht->mask_index) | sht->table_bit;
     *h1 = (uint32_t)lh1;
     *h2 = (uint32_t)lh2;

     uint32_t d1, d2;
     d1 = (uint32_t)(p1 & SH9A_DIGEST_MASK2) << SH9A_DIGEST_SHIFT;
     d2 = (uint32_t)(p2 & SH9A_DIGEST_MASK2) << SH9A_DIGEST_SHIFT;

     //make sure digest not zero - if so, set to default
     *pd1 = d1 ? d1 : SH9A_DIGEST_DEFAULT;
     *pd2 = d2 ? d2 : SH9A_DIGEST_DEFAULT;
}

static inline void sh9a_gethash2(stringhash9a_t * sht,
                                 uint8_t * key, uint32_t keylen,
                                 uint32_t *h1, uint32_t *h2,
                                 uint32_t *pd1, uint32_t *pd2,
                                 uint64_t *hash) {

     uint64_t m = evahash64(key, keylen, sht->hash_seed);
     *hash = m;
     uint64_t p1 = m * SH9A_PERMUTE1;
     uint64_t p2 = m * SH9A_PERMUTE2;
     uint64_t lh1, lh2;
     lh1 = (p1 >> SH9A_DIGEST_BITS) & sht->mask_index;
     lh2 = ((p2 >> SH9A_DIGEST_BITS) & sht->mask_index) | sht->table_bit;
     *h1 = (uint32_t)lh1;
     *h2 = (uint32_t)lh2;

     uint32_t d1, d2;
     d1 = (uint32_t)(p1 & SH9A_DIGEST_MASK2) << SH9A_DIGEST_SHIFT;
     d2 = (uint32_t)(p2 & SH9A_DIGEST_MASK2) << SH9A_DIGEST_SHIFT;

     //make sure digest not zero - if so, set to default
     *pd1 = d1 ? d1 : SH9A_DIGEST_DEFAULT;
     *pd2 = d2 ? d2 : SH9A_DIGEST_DEFAULT;
}

static inline void sh9a_gethash3(stringhash9a_t * sht,
                                 uint64_t hash,
                                 uint32_t *h1, uint32_t *h2,
                                 uint32_t *pd1, uint32_t *pd2) {

     uint64_t m = hash;
     uint64_t p1 = m * SH9A_PERMUTE1;
     uint64_t p2 = m * SH9A_PERMUTE2;
     uint64_t lh1, lh2;
     lh1 = (p1 >> SH9A_DIGEST_BITS) & sht->mask_index;
     lh2 = ((p2 >> SH9A_DIGEST_BITS) & sht->mask_index) | sht->table_bit;
     *h1 = (uint32_t)lh1;
     *h2 = (uint32_t)lh2;

     uint32_t d1, d2;
     d1 = (uint32_t)(p1 & SH9A_DIGEST_MASK2) << SH9A_DIGEST_SHIFT;
     d2 = (uint32_t)(p2 & SH9A_DIGEST_MASK2) << SH9A_DIGEST_SHIFT;

     //make sure digest not zero - if so, set to default
     *pd1 = d1 ? d1 : SH9A_DIGEST_DEFAULT;
     *pd2 = d2 ? d2 : SH9A_DIGEST_DEFAULT;
}

static inline void sh9a_shift_new(uint32_t * d, uint32_t a) {

     d[12] &= SH9A_DIGEST_MASK;
     d[12] |= d[9] & SH9A_LEFTOVER_MASK;
     d[13] &= SH9A_DIGEST_MASK;
     d[13] |= d[10] & SH9A_LEFTOVER_MASK;
     d[14] &= SH9A_DIGEST_MASK;
     d[14] |= d[11] & SH9A_LEFTOVER_MASK;

     d[9] &= SH9A_DIGEST_MASK;
     d[9] |= d[6] & SH9A_LEFTOVER_MASK;
     d[10] &= SH9A_DIGEST_MASK;
     d[10] |= d[7] & SH9A_LEFTOVER_MASK;
     d[11] &= SH9A_DIGEST_MASK;
     d[11] |= d[8] & SH9A_LEFTOVER_MASK;

     d[6] &= SH9A_DIGEST_MASK;
     d[6] |= d[3] & SH9A_LEFTOVER_MASK;
     d[7] &= SH9A_DIGEST_MASK;
     d[7] |= d[4] & SH9A_LEFTOVER_MASK;
     d[8] &= SH9A_DIGEST_MASK;
     d[8] |= d[5] & SH9A_LEFTOVER_MASK;

     d[3] &= SH9A_DIGEST_MASK;
     d[3] |= d[0] & SH9A_LEFTOVER_MASK;
     d[4] &= SH9A_DIGEST_MASK;
     d[4] |= d[1] & SH9A_LEFTOVER_MASK;
     d[5] &= SH9A_DIGEST_MASK;
     d[5] |= d[2] & SH9A_LEFTOVER_MASK;

     uint32_t b = d[15];
     uint32_t i;
     for (i = 15; i > 0; i--) {
          d[i] &= SH9A_LEFTOVER_MASK;
          d[i] |= d[i-1] & SH9A_DIGEST_MASK;
     }
     d[0] &= SH9A_DIGEST_MASK;
     d[0] |= (b & 0xFF00)>>8;
     d[1] &= SH9A_DIGEST_MASK;
     d[1] |= (b & 0xFF0000)>>16;
     d[2] &= SH9A_DIGEST_MASK;
     d[2] |= (b & 0xFF000000)>>24;

     d[0] &= SH9A_LEFTOVER_MASK;
     d[0] |= a; 
}

//find records using hashkeys.. return 1 if found
static inline int stringhash9a_check_posthash_shared(stringhash9a_t * sht,
                                              uint32_t h1, uint32_t h2,
                                              uint32_t d1, uint32_t d2) {
     //get lookup hashes
     SH9A_SHIFT_KEY(h1, k1)
     SH9A_LOCK(sht, k1)
     if (sh9a_lookup_bucket(&sht->buckets[h1], d1)) {
          SH9A_UNLOCK(sht, k1)
          return 1;
     }
     SH9A_UNLOCK(sht, k1)
     SH9A_SHIFT_KEY(h2, k2)
     SH9A_LOCK(sht, k2)
     if (sh9a_lookup_bucket(&sht->buckets[h2], d2)) {
          SH9A_UNLOCK(sht, k2)
          return 1;
     }
     SH9A_UNLOCK(sht, k2)

     return 0;
}

static inline int stringhash9a_check_posthash_serial(stringhash9a_t * sht,
                                              uint32_t h1, uint32_t h2,
                                              uint32_t d1, uint32_t d2) {
     //get lookup hashes
     if (sh9a_lookup_bucket(&sht->buckets[h1], d1)) {
          return 1;
     }
     if (sh9a_lookup_bucket(&sht->buckets[h2], d2)) {
          return 1;
     }

     return 0;
}

static inline int stringhash9a_check_posthash(stringhash9a_t * sht,
                                              uint32_t h1, uint32_t h2,
                                              uint32_t d1, uint32_t d2) {
     if (sht->is_shared) {
          return stringhash9a_check_posthash_shared(sht, h1, h2, d1, d2);
     }
     else {
          return stringhash9a_check_posthash_serial(sht, h1, h2, d1, d2);
     }
}

//find records using hashkeys.. return 1 if found
static inline int stringhash9a_check(stringhash9a_t * sht,
                                     void * key, int keylen) {

     uint32_t h1, h2;
     uint32_t d1, d2;

     sh9a_gethash(sht, (uint8_t*)key, keylen, &h1, &h2, &d1, &d2);

     return stringhash9a_check_posthash(sht, h1, h2, d1, d2);
}

//find records using hashkeys.. return 1 if found
static inline int stringhash9a_check_gethash(stringhash9a_t * sht,
                                             void * key, int keylen,
                                             uint64_t * phash) {

     uint32_t h1, h2;
     uint32_t d1, d2;

     sh9a_gethash2(sht, (uint8_t*)key, keylen, &h1, &h2, &d1, &d2, phash);

     return stringhash9a_check_posthash(sht, h1, h2, d1, d2);
}

//find records using hashkeys.. return 1 if found
static inline int stringhash9a_check_hash(stringhash9a_t * sht,
                                          uint64_t hash) {

     uint32_t h1, h2;
     uint32_t d1, d2;

     sh9a_gethash3(sht, hash, &h1, &h2, &d1, &d2);

     return stringhash9a_check_posthash(sht, h1, h2, d1, d2);
}

static inline int sh9a_cmp_epoch(stringhash9a_t * sht, uint32_t h1, uint32_t h2,
                                 uint32_t d1) {
     uint8_t e1, e2;
     uint8_t diff1, diff2;

     e1 = sht->buckets[h1].digest[15] & SH9A_LEFTOVER_MASK;
     diff1 = sht->epoch - e1;
     e2 = sht->buckets[h2].digest[15] & SH9A_LEFTOVER_MASK;
     diff2 = sht->epoch - e2; 
    
     //find oldest table 
     if (diff1 > diff2) {
          return 1;
     }
     else if (diff2 < diff1) {
          return 0;
     }
     //in the case of a tie, try to choose randomly
     else {
          if (d1 & 0x1) {
               return 1;
          }
          else {
               return 0;
          }
     }
     //return (diff1 >= diff2) ? 1 : 0;
}

static inline void sh9a_update_bucket_epoch(stringhash9a_t * sht, sh9a_bucket_t * bucket) {
     sht->insert_cnt++;
     if (sht->insert_cnt > sht->max_insert_cnt) {
          sht->insert_cnt = 0;
          sht->epoch++;
     }
     bucket->digest[15] &= SH9A_DIGEST_MASK;
     bucket->digest[15] |= (uint32_t)sht->epoch;
}

//find records using hashkeys.. return 1 if found
static inline int stringhash9a_set_posthash_shared(stringhash9a_t * sht,
                                            uint32_t h1, uint32_t h2,
                                            uint32_t d1, uint32_t d2) {
     uint32_t zeros1, zeros2;

     SH9A_SHIFT_KEY(h1, k1)
     SH9A_SHIFT_KEY(h2, k2)
     SH9A_LOCK_PAIR(sht, k1, k2)
     if (sh9a_lookup_bucket2(&sht->buckets[h1], d1, &zeros1) ||
         sh9a_lookup_bucket2(&sht->buckets[h2], d2, &zeros2)) {
          SH9A_UNLOCK_PAIR(sht, k1, k2)
          return 1;
     }

     sh9a_bucket_t * bucket;

     //if zeros.. do normal d-left balance
     if (zeros1 > zeros2) {
          bucket = &sht->buckets[h1];
          sh9a_shift_new(bucket->digest, d1);
     }
     else if (zeros1 < zeros2) {
          bucket = &sht->buckets[h2];
          sh9a_shift_new(bucket->digest, d2);
     }
     else if (zeros1) { /// its a tie
          bucket = &sht->buckets[h1];
          sh9a_shift_new(bucket->digest, d1);
     }
     else {
          //ok we have to drop an item
          sht->drops++;

          if (sh9a_cmp_epoch(sht, h1, h2, d1)) {
               bucket = &sht->buckets[h1];
               sh9a_shift_new(bucket->digest, d1);
          }
          else {
               bucket = &sht->buckets[h2];
               sh9a_shift_new(bucket->digest, d2);
          }
     }

     sh9a_update_bucket_epoch(sht, bucket); 
     SH9A_UNLOCK_PAIR(sht, k1, k2)

     return 0;
}

static inline int stringhash9a_set_posthash_serial(stringhash9a_t * sht,
                                            uint32_t h1, uint32_t h2,
                                            uint32_t d1, uint32_t d2) {
     uint32_t zeros1, zeros2;

     if (sh9a_lookup_bucket2(&sht->buckets[h1], d1, &zeros1) ||
         sh9a_lookup_bucket2(&sht->buckets[h2], d2, &zeros2)) {
          return 1;
     }

     sh9a_bucket_t * bucket;

     //if zeros.. do normal d-left balance
     if (zeros1 > zeros2) {
          bucket = &sht->buckets[h1];
          sh9a_shift_new(bucket->digest, d1);
     }
     else if (zeros1 < zeros2) {
          bucket = &sht->buckets[h2];
          sh9a_shift_new(bucket->digest, d2);
     }
     else if (zeros1) { /// its a tie
          bucket = &sht->buckets[h1];
          sh9a_shift_new(bucket->digest, d1);
     }
     else {
          //ok we have to drop an item
          sht->drops++;

          if (sh9a_cmp_epoch(sht, h1, h2, d1)) {
               bucket = &sht->buckets[h1];
               sh9a_shift_new(bucket->digest, d1);
          }
          else {
               bucket = &sht->buckets[h2];
               sh9a_shift_new(bucket->digest, d2);
          }
     }

     sh9a_update_bucket_epoch(sht, bucket); 

     return 0;
}

static inline int stringhash9a_set_posthash(stringhash9a_t * sht,
                                              uint32_t h1, uint32_t h2,
                                              uint32_t d1, uint32_t d2) {
     if (sht->is_shared) {
          return stringhash9a_set_posthash_shared(sht, h1, h2, d1, d2);
     }
     else {
          return stringhash9a_set_posthash_serial(sht, h1, h2, d1, d2);
     }
}

static inline uint64_t stringhash9a_drop_cnt(stringhash9a_t * sht) {
     return sht->drops;
}

//find records using hashkeys.. return 1 if found
static inline int stringhash9a_set(stringhash9a_t * sht,
                                   void * key, int keylen) {

     uint32_t h1, h2;
     uint32_t d1, d2;

     sh9a_gethash(sht, (uint8_t*)key, keylen, &h1, &h2, &d1, &d2);

     return stringhash9a_set_posthash(sht, h1, h2, d1, d2);

}

//find records using hashkeys.. return 1 if found
static inline int stringhash9a_set_gethash(stringhash9a_t * sht,
                                           void * key, int keylen,
                                           uint64_t * phash) {

     uint32_t h1, h2;
     uint32_t d1, d2;

     sh9a_gethash2(sht, (uint8_t*)key, keylen, &h1, &h2, &d1, &d2, phash);

     return stringhash9a_set_posthash(sht, h1, h2, d1, d2);

}

//find records using hashkeys.. return 1 if found
static inline int stringhash9a_set_hash(stringhash9a_t * sht,
                                        uint64_t hash) {

     uint32_t h1, h2;
     uint32_t d1, d2;

     sh9a_gethash3(sht, hash, &h1, &h2, &d1, &d2);

     return stringhash9a_set_posthash(sht, h1, h2, d1, d2);
}



//move mru item to front.. for lower 16 items in a bucket
static inline void sh9a_sort_lru_lower_half(uint32_t * d, uint8_t mru) {
     uint32_t a;
     uint32_t i;
     uint32_t x = (mru > 10) ? (mru - 10) : 0;
     a = d[mru] & SH9A_DIGEST_MASK;
     for (i = mru; i > x; i--) {
          d[i] &= SH9A_LEFTOVER_MASK;
          d[i] |= d[i-1] & SH9A_DIGEST_MASK;
     }
     d[x] &= SH9A_LEFTOVER_MASK;
     d[x] |= a;
}


//move mru item to front..  ridiculous code bloat - but it's supposed
// to perform better than straightforward looping..
static inline void sh9a_sort_lru_upper_half(uint32_t * d, uint8_t mru) {
     uint32_t a;
     uint32_t i;

     switch (mru) {
     case 16:
          a = ((d[0] & SH9A_LEFTOVER_MASK)<<8) + 
               ((d[1] & SH9A_LEFTOVER_MASK)<<16) +
               ((d[2] & SH9A_LEFTOVER_MASK)<<24);
          break;
     case 17:
          a = ((d[3] & SH9A_LEFTOVER_MASK)<<8) + 
               ((d[4] & SH9A_LEFTOVER_MASK)<<16) +
               ((d[5] & SH9A_LEFTOVER_MASK)<<24);
          break;
     case 18:
          a = ((d[6] & SH9A_LEFTOVER_MASK)<<8) + 
               ((d[7] & SH9A_LEFTOVER_MASK)<<16) +
               ((d[8] & SH9A_LEFTOVER_MASK)<<24);
          break;
     case 19:
          a = ((d[9] & SH9A_LEFTOVER_MASK)<<8) + 
               ((d[10] & SH9A_LEFTOVER_MASK)<<16) +
               ((d[11] & SH9A_LEFTOVER_MASK)<<24);
          break;
     case 20:
          a = ((d[12] & SH9A_LEFTOVER_MASK)<<8) + 
               ((d[13] & SH9A_LEFTOVER_MASK)<<16) +
               ((d[14] & SH9A_LEFTOVER_MASK)<<24);
          break;
     }

     switch (mru) {
     case 20:
          d[12] &= SH9A_DIGEST_MASK;
          d[12] |= d[9] & SH9A_LEFTOVER_MASK;
          d[13] &= SH9A_DIGEST_MASK;
          d[13] |= d[10] & SH9A_LEFTOVER_MASK;
          d[14] &= SH9A_DIGEST_MASK;
          d[14] |= d[11] & SH9A_LEFTOVER_MASK;
          //nobreak;
     case 19:
          d[9] &= SH9A_DIGEST_MASK;
          d[9] |= d[6] & SH9A_LEFTOVER_MASK;
          d[10] &= SH9A_DIGEST_MASK;
          d[10] |= d[7] & SH9A_LEFTOVER_MASK;
          d[11] &= SH9A_DIGEST_MASK;
          d[11] |= d[8] & SH9A_LEFTOVER_MASK;
          //nobreak;
     case 18:
          d[6] &= SH9A_DIGEST_MASK;
          d[6] |= d[3] & SH9A_LEFTOVER_MASK;
          d[7] &= SH9A_DIGEST_MASK;
          d[7] |= d[4] & SH9A_LEFTOVER_MASK;
          d[8] &= SH9A_DIGEST_MASK;
          d[8] |= d[5] & SH9A_LEFTOVER_MASK;
          //nobreak;
     case 17:
          d[3] &= SH9A_DIGEST_MASK;
          d[3] |= d[0] & SH9A_LEFTOVER_MASK;
          d[4] &= SH9A_DIGEST_MASK;
          d[4] |= d[1] & SH9A_LEFTOVER_MASK;
          d[5] &= SH9A_DIGEST_MASK;
          d[5] |= d[2] & SH9A_LEFTOVER_MASK;
          //no break;
     }

     uint32_t x = mru - 10;
     uint32_t b = d[15];
     for (i = 15; i > x; i--) {
          d[i] &= SH9A_LEFTOVER_MASK;
          d[i] |= d[i-1] & SH9A_DIGEST_MASK;
     }
     d[0] &= SH9A_DIGEST_MASK;
     d[0] |= (b & 0xFF00)>>8;
     d[1] &= SH9A_DIGEST_MASK;
     d[1] |= (b & 0xFF0000)>>16;
     d[2] &= SH9A_DIGEST_MASK;
     d[2] |= (b & 0xFF000000)>>24;

     d[x] &= SH9A_LEFTOVER_MASK;
     d[x] |= a; 
}

//move mru item to front..  ridiculous code bloat - but it's supposed
// to loop unravel
static inline void sh9a_delete_lru(uint32_t * d, uint8_t item) {
     uint32_t i;
     if (item < 16) {
          for (i = item; i < 15; i++) {
               d[i] &= SH9A_LEFTOVER_MASK;
               d[i] = d[i+1] & SH9A_DIGEST_MASK;
          }
          d[15] = ((d[0] & SH9A_LEFTOVER_MASK)<<8) + 
               ((d[1] & SH9A_LEFTOVER_MASK)<<16) +
               ((d[2] & SH9A_LEFTOVER_MASK)<<24);

          item = 16;
     }
     switch (item) {
     case 16:
          d[0] &= SH9A_DIGEST_MASK;
          d[0] |= d[3] & SH9A_LEFTOVER_MASK;
          d[1] &= SH9A_DIGEST_MASK;
          d[1] |= d[4] & SH9A_LEFTOVER_MASK;
          d[2] &= SH9A_DIGEST_MASK;
          d[2] |= d[5] & SH9A_LEFTOVER_MASK;
          //no break;
     case 17:
          d[3] &= SH9A_DIGEST_MASK;
          d[3] |= d[6] & SH9A_LEFTOVER_MASK;
          d[4] &= SH9A_DIGEST_MASK;
          d[4] |= d[7] & SH9A_LEFTOVER_MASK;
          d[5] &= SH9A_DIGEST_MASK;
          d[5] |= d[8] & SH9A_LEFTOVER_MASK;
          //no break;
     case 18:
          d[6] &= SH9A_DIGEST_MASK;
          d[6] |= d[9] & SH9A_LEFTOVER_MASK;
          d[7] &= SH9A_DIGEST_MASK;
          d[7] |= d[10] & SH9A_LEFTOVER_MASK;
          d[8] &= SH9A_DIGEST_MASK;
          d[8] |= d[11] & SH9A_LEFTOVER_MASK;
          //no break;
     case 19:
          d[9] &= SH9A_DIGEST_MASK;
          d[9] |= d[12] & SH9A_LEFTOVER_MASK;
          d[10] &= SH9A_DIGEST_MASK;
          d[10] |= d[13] & SH9A_LEFTOVER_MASK;
          d[11] &= SH9A_DIGEST_MASK;
          d[11] |= d[14] & SH9A_LEFTOVER_MASK;
          //no break;
     }

     //delete last element..
     d[12] &= SH9A_DIGEST_MASK;
     d[13] &= SH9A_DIGEST_MASK;
     d[14] &= SH9A_DIGEST_MASK;
}

//given an index and digest.. find state data...
static inline int sh9a_delete_bucket(sh9a_bucket_t * bucket,
                                    uint32_t digest) {
     uint32_t i;
     
     uint32_t * dp = bucket->digest;
     uint32_t leftover[6] = {0};
     for (i = 0 ; i < SH9A_DEPTH; i++) {
          if (digest == (dp[i] & SH9A_DIGEST_MASK)) {
               sh9a_delete_lru(bucket->digest, i);
               return 1;
          }
          sh9a_build_leftover(i, leftover, digest);
     }
     for (i = 0; i < 5; i++) {
          if (digest == leftover[i]) {
               sh9a_delete_lru(bucket->digest, i+16);
               return 1;
          }
     }
     return 0;

}

//delete record at hashkey
static inline int stringhash9a_delete_shared(stringhash9a_t * sht,
                                      void * key, int keylen) {

     uint32_t h1, h2;
     uint32_t d1, d2;

     sh9a_gethash(sht, (uint8_t*)key, keylen, &h1, &h2, &d1, &d2);

     //lookup in digest.. location1
     SH9A_SHIFT_KEY(h1, k1)
     SH9A_LOCK(sht, k1)
     if (sh9a_delete_bucket(&sht->buckets[h1], d1)) {
          SH9A_UNLOCK(sht, k1)
          return 1;
     }
     SH9A_UNLOCK(sht, k1)
     SH9A_SHIFT_KEY(h2, k2)
     SH9A_LOCK(sht, k2)

     if (sh9a_delete_bucket(&sht->buckets[h2], d2)) {
          SH9A_UNLOCK(sht, k2)
          return 1;
     }
     SH9A_UNLOCK(sht, k2)

     return 0;
}

static inline int stringhash9a_delete_serial(stringhash9a_t * sht,
                                      void * key, int keylen) {

     uint32_t h1, h2;
     uint32_t d1, d2;

     sh9a_gethash(sht, (uint8_t*)key, keylen, &h1, &h2, &d1, &d2);

     //lookup in digest.. location1
     if (sh9a_delete_bucket(&sht->buckets[h1], d1)) {
          return 1;
     }

     if (sh9a_delete_bucket(&sht->buckets[h2], d2)) {
          return 1;
     }

     return 0;
}

static inline int stringhash9a_delete(stringhash9a_t * sht,
                                      void * key, int keylen) {
     if (sht->is_shared) {
          return stringhash9a_delete_shared(sht, key, keylen);
     }
     else {
          return stringhash9a_delete_serial(sht, key, keylen);
     }
}

static inline void stringhash9a_flush(stringhash9a_t * sht) {
     SH9A_LOCK_ALL(sht)
     memset(sht->buckets, 0, sizeof(sh9a_bucket_t) * (uint64_t)sht->index_size * 2);
     SH9A_UNLOCK_ALL(sht)
     sht->epoch = 1;
}

static inline int stringhash9a_clean_sharing (void * sht_generic, int * index) {
     stringhash9a_t * sht = (stringhash9a_t *)sht_generic;

     if (work_size == 1 || sht->sharedata->cnt == 1) {

          // free items associated with sharing
#if defined(WS_PTHREADS) && !defined(OWMR_TABLES)
          if (sht->sharelabel) {
               uint32_t i;
               for (i = 0; i < sht->max_mutex; i++) {
                    SH9A_LOCK_DESTROY(sht,i)
               }

               free((void *)sht->mutex);
               sht->mutex = NULL;
          }
#endif // WS_PTHREADS && !OWMR_TABLES
          free(sht->sharedata);
          sht->sharedata = NULL;
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
               return 0;;
          }
     }
     else {
          (*index)++;
     }

     return 1;
}

static inline void stringhash9a_destroy(stringhash9a_t * sht) {
     int ret = 0;

#ifndef TOOL_NAME
     // The table is destroyed by the last thread to arrive
     if (sht->sharelabel) {
          ret = ws_kidshare_unshare(sht->v_type_table, sht->sharelabel);
     }
#endif // !TOOL_NAME
     if (!sht->sharelabel || !ret) {
          uint64_t expire_cnt = stringhash9a_drop_cnt(sht);
          if (expire_cnt) {
               tool_print("sh9a table expire cnt %"PRIu64, expire_cnt);
          }
          free(sht->sharedata);
#if defined(WS_PTHREADS) && !defined(OWMR_TABLES)
          if (sht->sharelabel) {
               uint32_t i;
               for (i = 0; i < sht->max_mutex; i++) {
                    SH9A_LOCK_DESTROY(sht,i)
               }

               free((void *)sht->mutex);
          }
#endif // WS_PTHREADS && !OWMR_TABLES
          free(sht->sharelabel);
          free(sht->buckets);
          free(sht);
     }
     //something BAD happened with shared table accounting, so report this!
     else if (ret < 0) {
          error_print("failed stringhash9a_destroy due to invalid ws_kidshare_unshare return");
     }
}

//read sh9 table from file..
static inline stringhash9a_t * stringhash9a_read(FILE * fp) {
     uint32_t ibits, hash_seed;
     char sht_id[SHT_ID_SIZE];

     //save initial fp in case we need to back up for an unlabled table
     FILE * fp0 = fp;

     // check for correct stringhash table type
     if (!fread(&sht_id, SHT_ID_SIZE, 1, fp)) {
          return NULL;
     }
     if (strncmp(sht_id, SHT9A_ID, SHT_ID_SIZE) != 0) {

          //failure - this is a stringhash9a table
          if (strncmp(sht_id, SHT5_ID, SHT_ID_SIZE) == 0) {
               error_print("attempting to read hash table type %s instead of type %s",
                           sht_id, SHT9A_ID);
               return NULL;
          }
          //this is an unlabeled table - try reading it
          else {
               status_print("attempting to read unlabeled hash table");

               //reset file ptr to read from the beginning
               fp = fp0;
          }
     }

     //get the table ibits and hash_seed
     if (!fread(&ibits, sizeof(uint32_t), 1, fp)) {
          return NULL;
     }
     if (!fread(&hash_seed, sizeof(uint32_t), 1, fp)) {
          return NULL;
     }

     //create table..
     stringhash9a_t * sht = sh9a_create_ibits(ibits);

     sht->hash_seed = hash_seed;
     uint32_t total_buckets = sht->index_size * 2;
     uint32_t i = 0;
     while (i < total_buckets) {
          if (feof(fp) || ferror(fp)) {
               stringhash9a_destroy(sht);
               return NULL;
          }
          i += fread(sht->buckets + i, sizeof(sh9a_bucket_t),
                     total_buckets - i, fp);
     }

     sht->mem_used = 2 * (uint64_t)sht->index_size * sizeof(sh9a_bucket_t);

     return sht;
}

//dump sh9 table to file
static inline int stringhash9a_dump(stringhash9a_t * sht, FILE * fp) {
     int rtn;
     char sht_id[SHT_ID_SIZE] = SHT9A_ID;

     SH9A_LOCK_ALL(sht)
     rtn = fwrite(&sht_id, SHT_ID_SIZE, 1, fp);
     rtn = fwrite(&sht->ibits, sizeof(uint32_t), 1, fp);
     rtn += fwrite(&sht->hash_seed, sizeof(uint32_t), 1, fp);
     uint32_t i = 0;
     uint32_t total_buckets = sht->index_size * 2;
     while (i < total_buckets) {
          i += fwrite(sht->buckets + i, sizeof(sh9a_bucket_t),
                      total_buckets - i, fp);
     }
     SH9A_UNLOCK_ALL(sht)

     return 1;
} 

#ifdef _WATERSLIDE_H
static inline int stringhash9a_check_loc(stringhash9a_t * sht, ws_hashloc_t * loc) {
     if (loc && loc->len) {
          return stringhash9a_check(sht, loc->offset, loc->len);
     }
     return 0;
}

static inline int stringhash9a_check_wsdata(stringhash9a_t * sht,
                                          wsdata_t * wsd) {
     ws_hashloc_t * loc = wsd->dtype->hash_func(wsd);
     if (loc) {
          return stringhash9a_check(sht, loc->offset, loc->len);
     }
     return 0;
}


static inline int stringhash9a_set_loc(stringhash9a_t * sht, ws_hashloc_t * loc) {
     if (loc && loc->len) {
          return stringhash9a_set(sht, loc->offset, loc->len);
     }
     return 0;
}

static inline int stringhash9a_set_wsdata(stringhash9a_t * sht,
                                          wsdata_t * wsd) {
     ws_hashloc_t * loc = wsd->dtype->hash_func(wsd);
     if (loc) {
          return stringhash9a_set(sht, loc->offset, loc->len);
     }
     return 0;
}


static inline int stringhash9a_delete_loc(stringhash9a_t * sht, ws_hashloc_t * loc) {
     if (loc && loc->len) {
          return stringhash9a_delete(sht, loc->offset, loc->len);
     }
     return 0;
}

static inline int stringhash9a_delete_wsdata(stringhash9a_t * sht,
                                             wsdata_t * wsd) {
     ws_hashloc_t * loc = wsd->dtype->hash_func(wsd);
     if (loc) {
          return stringhash9a_delete(sht, loc->offset, loc->len);
     }
     return 0;
}
#endif // _WATERSLIDE_H

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _STRINGHASH9A_H

