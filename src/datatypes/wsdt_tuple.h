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
// infinite tuple containers -- grows tuples as needed for data
// allows for indexing into tuple
// in the case of threading, growth of tuple should only occur when reference count on data is 1

// tuple growth -- old tuples sizes are kept around in a linked list for better
// consistency and easier recovery

#ifndef _WSDT_TUPLE_H
#define _WSDT_TUPLE_H

//#define DEBUG 1
#include <stdlib.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_binary.h"
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_mediumstring.h"
#include "datatypes/wsdt_bigstring.h"
#include "wstypes.h"
#include "wsqueue.h"
#include "assert.h"
#include "evahash64.h"

#define WSDT_TUPLE_STR "TUPLE_TYPE"

/*#ifdef HUGETUPLE
#define WSDT_TUPLE_MAX 1024
#else
#define WSDT_TUPLE_MAX 96
#endif*/

#define WSDT_TUPLE_SMALL_LEN (32)
#define WSDT_TUPLE_MEDIUM_LEN (128)
#define WSDT_TUPLE_LARGE_LEN (1024)
#define WSDT_TUPLE_MAX_LEN (0x3FFFFFFF)

#define WSDT_TUPLE_MAX WSDT_TUPLE_LARGE_LEN

//define initial length of huge - but allow it to grow
#define WSDT_TUPLE_HUGE_LEN (65536)
#define WSDT_TUPLE_HUGE_INCREMENT (65536)

//huge are automatically grown and reclaimed 
#define WSDT_TUPLE_MIN WSDT_TUPLE_SMALL_LEN

#define TUPLE_DEFAULT_HASHKEY (0x123FF13)

//the primary tuple datastructure (please access via helper functions)
typedef struct _wsdt_tuple_t {
     wsfree_list_node_t fl_node;
     uint32_t len; // length of current membership
#ifdef USE_ATOMICS
     uint32_t add_len; // index for next available write
#else
     WS_SPINLOCK_DECL(lock);
#endif
     uint32_t max; //actual max size of member structure
     uint32_t index_len;
     struct _wsdt_tuple_t * prev;  //point to previously size tuple
     wsfree_list_t * freeq;  //point to freeq from which tuple originated
     int * icnt;  //label index cnt
     wsdata_t ** index;        //points to local index
     //the following is a struct hack and is actually size max
     wsdata_t * member[WSDT_TUPLE_MIN];
} wsdt_tuple_t;

#define TUPLE_ALLOC_SIZE(max_members, index_len)  \
     (sizeof(wsdt_tuple_t) + \
      sizeof(wsdata_t *) * ((size_t)max_members - WSDT_TUPLE_MIN) + \
      (size_t)index_len * sizeof(int) + \
      (size_t)index_len * (size_t)max_members * sizeof(wsdata_t *)) 

#define TUPLE_MEMBER_CNT_OFFSET(max_members, index_len)  \
     (sizeof(wsdt_tuple_t) + \
      sizeof(wsdata_t *) * (max_members - WSDT_TUPLE_MIN))

#define TUPLE_INDEX_OFFSET(max_members, index_len)  \
     (sizeof(wsdt_tuple_t) + \
      sizeof(wsdata_t *) * (max_members - WSDT_TUPLE_MIN) + \
      index_len * sizeof(int))

#define TUPLE_INDEX_ARRAY(tuple, index_pos)  \
     (tuple->index + (index_pos * tuple->max))

typedef struct _wsdt_tuple_freeq {
     int allocd;
     int freed;

     uint32_t index_len;
     uint32_t *p_ilen;
#ifndef USE_ATOMICS
     WS_SPINLOCK_DECL(lock);
#endif

     //for tuple space
     wsfree_list_t * freeq_small;
     wsfree_list_t * freeq_medium;
     wsfree_list_t * freeq_large;
     //wsstack_t * freeq_huge;  -- huge  are automatically freed
} wsdt_tuple_freeq_t;


//defined below..
static inline int tuple_attach_member_labels(wsdata_t * wsd_tuple,
                                             wsdata_t * member);
static inline int tuple_add_member_label(wsdata_t * wsd_tuple,
                                         wsdata_t * member, wslabel_t * label);

// internal tuple allocation / initialization routine
static inline wsdt_tuple_t * wsdt_tuple_internal_alloc(wsdt_tuple_freeq_t *tfq,
                                                       int newlen) {
     wsdt_tuple_t *newtup;
     size_t tuplesize = TUPLE_ALLOC_SIZE(newlen, tfq->index_len);
     uint8_t *vtup = (uint8_t*)calloc(1, tuplesize);
     if (!vtup) {
          return NULL;
     }
     newtup = (wsdt_tuple_t * )vtup;

     newtup->max = newlen;

     //allocate indexes
     newtup->index_len = tfq->index_len;
     newtup->icnt = 
          (int*)
          (vtup + TUPLE_MEMBER_CNT_OFFSET(newlen, tfq->index_len));
     newtup->index = 
          (wsdata_t **)(vtup + TUPLE_INDEX_OFFSET(newlen, tfq->index_len));

#ifndef USE_ATOMICS
     WS_SPINLOCK_INIT(&newtup->lock);
#endif
     newtup->freeq = NULL;
     return newtup;
}

static inline wsdt_tuple_t * wsdt_tuple_alloc(wsdt_tuple_freeq_t * tfq,
                                              wsfree_list_t * freeq, int newlen) {
     wsdt_tuple_t * newtup = NULL;
     if (freeq) {
again:
          newtup = (wsdt_tuple_t*) wsfree_list_alloc(freeq);
          //  check if index is correctly sized -- otherwise discard..
          if (newtup) {
               if (newtup->index_len != tfq->index_len) {
                    dprint("index size has been modified");
                    // don't use this tuple, index size has been modified
                    free(newtup);
                    goto again;
               }
               else {
                    newtup->prev = NULL;
                    newtup->len = 0;
#ifdef USE_ATOMICS
                    newtup->add_len = 0;
#endif
                    memset(newtup->icnt, 0,
                           sizeof(int) * tfq->index_len);
               }
          }
     }
     //otherwise alloc new data where size = newlen - WSDT_TUPLE_MIN
     if (!newtup) {
          newtup = wsdt_tuple_internal_alloc(tfq, newlen);
#ifdef USE_ATOMICS
          (void) __sync_fetch_and_add(&tfq->allocd, 1);
#else
          WS_SPINLOCK_LOCK(&tfq->lock);
          tfq->allocd++;
          WS_SPINLOCK_UNLOCK(&tfq->lock);
#endif
     }
#ifdef USE_ATOMICS
     __sync_synchronize();
#endif
     return newtup;
}

// the following function should be done prior to destroying a tuple
// move tuple data to its freeq.
static inline int wsdt_tuple_recover(wsdata_t * tdata) {
     wsdt_tuple_t * tuple = (wsdt_tuple_t * )tdata->data;
     wsdt_tuple_t * prev;

     while (tuple) {
          prev = tuple->prev;

          //should be the last tuple in the list
          //a recoverable tuple size
          if (tuple->freeq) {
               wsfree_list_free(tuple->freeq, tuple);
          }
          //a for huge tuple
          else {
               free(tuple);
               wsdt_tuple_freeq_t *tfq = (wsdt_tuple_freeq_t*)tdata->dtype->instance;
#ifdef USE_ATOMICS
               (void) __sync_fetch_and_add(&tfq->freed, 1);
#else
               WS_SPINLOCK_LOCK(&tfq->lock);
               tfq->freed++;
               WS_SPINLOCK_UNLOCK(&tfq->lock);
#endif
          }
          tuple = prev;
     }
     tdata->data = NULL;
     return 0;
}

static inline int tuple_grow_membership(wsdata_t * tdata) {
     int newlen = 0;
     wsfree_list_t * new_freeq = NULL;

     wsdt_tuple_freeq_t * tfq = (wsdt_tuple_freeq_t *)tdata->dtype->instance;
     wsdt_tuple_t * tuple = (wsdt_tuple_t *)tdata->data;

     switch(tuple->max) {
     case WSDT_TUPLE_SMALL_LEN:
          newlen = WSDT_TUPLE_MEDIUM_LEN;
          new_freeq = tfq->freeq_medium;
          break;
     case WSDT_TUPLE_MEDIUM_LEN:
          newlen = WSDT_TUPLE_LARGE_LEN;
          new_freeq = tfq->freeq_large;
          break;
     case WSDT_TUPLE_LARGE_LEN:
          newlen = WSDT_TUPLE_HUGE_LEN;
          break;
     default:
          //assume this is huge
          if (tuple->max >= WSDT_TUPLE_MAX_LEN) {
               return 0;
          }
          newlen = tuple->max << 1;  //increment by power of 2
     }
     dprint ("growing tuple from %u to %u", tuple->max, newlen);

     //allocate new tuple
     wsdt_tuple_t * newtup = wsdt_tuple_alloc(tfq, new_freeq, newlen);

     if (!newtup) {
          return 0;
     }

     //move data from old tuple to new tuple.  Since tuples can only grow and no
     //other thread has our tuple yet, this is thread safe(ish)
     newtup->len = tuple->len;
#ifdef USE_ATOMICS
     newtup->add_len = newtup->len;
#endif
     uint32_t i;
     for (i = 0; i < newtup->len; i++) {
         newtup->member[i] = tuple->member[i]; 
     }
    
     //move indexes from old tuple to new tuple
     assert(tuple->index_len <= newtup->index_len);
     for (i = 0; i < tuple->index_len; i++) {
          if (tuple->icnt[i]) {
               newtup->icnt[i] = tuple->icnt[i];
               wsdata_t ** orig_index_array = TUPLE_INDEX_ARRAY(tuple, i);
               wsdata_t ** new_index_array = TUPLE_INDEX_ARRAY(newtup, i); 
               memcpy(new_index_array, orig_index_array,
                      sizeof(wsdata_t *) * tuple->icnt[i]);
          }
     }
    
     //keep old tuple around in linked list 
     newtup->prev = tuple;

     //assign new tuple to data
     tdata->data = (void *)newtup;

     return 1;
}

// call this function when processing data to add a member to the tuple
static inline int add_tuple_member(wsdata_t * tdata, wsdata_t *member) {
     uint32_t tmp;

     dprint("add_tuple_member"); 
     if (!tdata || !member) {
          return 0;
     }
     wsdt_tuple_t * tuple = (wsdt_tuple_t *)tdata->data;

#ifdef USE_ATOMICS
     tmp = __sync_fetch_and_add(&tuple->add_len, 1);
#else
     WS_SPINLOCK_LOCK(&tuple->lock);
     tmp = tuple->len++;
#endif
     // XXX: There are two points to note when crossing boundary points on the 
     //      value of tuple->max:
     //      1. There is a chance of inserting members in an out of order fashion
     //         in the tuple->member[] array, e.g., 33rd member may be added before
     //         the 32nd member. This really doesn't matter as we don't extract
     //         members by index, but rather by name (as in subtuple)
     while (tmp >= tuple->max) {
#ifndef USE_ATOMICS
          WS_SPINLOCK_UNLOCK(&tuple->lock);
#endif
          // We appear to be out of tuple space.  Acquire the lock on the
          // datatype (not on the tuple) to make sure another thread hasn't also
          // discovered this problem.  The one that acquires the lock resets the
          // length of the old tuple to be rational!
          if (tdata->isptr) {
               dprint("Attempt to grow tuple from pointer");
               return 0;
          }
#ifdef USE_ATOMICS
          // Wait for all previous to have gone, or indication
          // that somebody else has already grown the tuple.
          while ((tuple->len != tmp) && (tuple == tdata->data))
               asm("":::"memory"); // compiler fence
#endif

          WS_SPINLOCK_LOCK(&tdata->lock);
          if (tuple == (wsdt_tuple_t*)tdata->data) {
               tuple->len = tuple->max;
#ifdef USE_ATOMICS
               tuple->add_len = tuple->max;
#endif
               if (!tuple_grow_membership(tdata)) {
                    dprint("max tuple length reached..");
                    WS_SPINLOCK_UNLOCK(&tdata->lock);
                    return 0;
               }
          }
          tuple = (wsdt_tuple_t *)tdata->data;
          WS_SPINLOCK_UNLOCK(&tdata->lock);
#ifdef USE_ATOMICS
          tmp = __sync_fetch_and_add(&tuple->add_len, 1);
#else
          WS_SPINLOCK_LOCK(&tuple->lock);
          tmp = tuple->len++;
#endif
     }
     tuple->member[tmp] = member;
#ifdef USE_ATOMICS
     while (tuple->len != tmp) asm("":::"memory"); // compiler fence
     tuple->len = tmp+1;
#else
     WS_SPINLOCK_UNLOCK(&tuple->lock);
#endif
    
     dprint("add_tuple_member %d", tuple->len); 
     wsdata_add_reference(member);
     dprint("add_tuple_member: attach labels"); 
     tuple_attach_member_labels(tdata, member);
     return 1;
}

/**
 *  Perform a deep-copy (recursive) from a source to a destination wsdata_t
 *  @param src  Source Tuple to copy
 *  @param dst  Destination into which the copy should be made
 *  @return 1 for success, 0 for failure
 */
static inline int tuple_deep_copy(wsdata_t* src, wsdata_t* dst) {
     wsdt_tuple_t * src_tuple = (wsdt_tuple_t*)src->data;
     // duplicate container label (not search member labels) ... for hierarchical/nested tuples, these
     // container labels will wind up being the member search labels for the nested subtuples
     wsdata_duplicate_labels(src, dst);
     uint32_t i;
     for (i = 0; i < src_tuple->len; i++) {
          if(src_tuple->member[i]->dtype == dtype_tuple) {
               wsdata_t * dst_sub_tuple = wsdata_alloc(dtype_tuple);
               if(!dst_sub_tuple) {
                    return 0;
               }
               // recursively copy each subtuple
               if(!tuple_deep_copy(src_tuple->member[i], dst_sub_tuple)) {
                    wsdata_delete(dst_sub_tuple);
                    return 0;
               }
               // don't assume that add tuple member will always succeed
               if(!add_tuple_member(dst, dst_sub_tuple)) {
                    wsdata_delete(dst_sub_tuple);
                    return 0;
               }
          }
          else {
               if(!add_tuple_member(dst, src_tuple->member[i])) {
                    return 0;
               }
          }
     }
     return 1;
}

static inline wsdata_t * tuple_member_create_wsdata(wsdata_t * tuple, wsdatatype_t * dtype,
                                                    wslabel_t * label) {
     if (!dtype || !tuple) {
          return NULL;
     }
     wsdata_t * data = wsdata_alloc(dtype);
     if (!data) {
          return NULL;
     }
     if (label) {
          wsdata_add_label(data, label);
     }
     if (!add_tuple_member(tuple, data)) {
          wsdata_delete(data);
          return NULL;
     }
     return data;
}

static inline wsdata_t * tuple_add_subelement(wsdata_t * tuple, wsdata_t * obj,
                                          wssubelement_t * el) {
     wsdata_t * subel = wsdata_get_subelement(obj, el);
     if (subel) {
          add_tuple_member(tuple, subel);
     }
     return subel;
}

static inline void * tuple_member_create(wsdata_t * tuple, wsdatatype_t * dtype,
                                         wslabel_t * label) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype, label);
     if (!data) {
          return NULL;
     }
     return data->data;
}

static inline wsdata_t * tuple_member_create_double(wsdata_t * tuple,
                                                    double dbl,
                                                    wslabel_t * label) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype_double, label);
     if (!data) {
          return NULL;
     }
     wsdt_double_t * odbl = (wsdt_double_t*)data->data;
     *odbl = dbl;
     return data;
}

static inline wsdata_t * tuple_member_add_ptr(wsdata_t * tuple, wsdata_t * ref,
                                              wslabel_t * label) {
     wsdata_t * ptr = wsdata_ptr(ref);
     if (ptr) {
          if (label) {
               wsdata_add_label(ptr, label);
          }
          add_tuple_member(tuple, ptr);
          return ptr;
     }
     return NULL;
}

static inline wsdata_t * tuple_member_add_ptr_mlabel(wsdata_t * tuple, wsdata_t * ref,
                                                     wslabel_t * label1,
                                                     wslabel_t * label2) {
     wsdata_t * ptr = wsdata_ptr(ref);
     if (ptr) {
          if (label1) {
               wsdata_add_label(ptr, label1);
          }
          if (label2) {
               wsdata_add_label(ptr, label2);
          }
          add_tuple_member(tuple, ptr);
          return ptr;
     }
     return NULL;
}

static inline wsdata_t * tuple_member_create_int(wsdata_t * tuple,
                                                  wsdt_int_t u,
                                                  wslabel_t * label) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype_int, label);
     if (!data) {
          return NULL;
     }
     wsdt_int_t * ou = (wsdt_int_t*)data->data;
     *ou = u;
     return data;
}

#define tuple_member_create_int32 tuple_member_create_int

static inline wsdata_t * tuple_member_create_int_mlabel(wsdata_t * tuple,
                                                        wsdt_int_t u,
                                                        wslabel_t * label,
                                                        wslabel_t * label2) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype_int, label);
     if (!data) {
          return NULL;
     }
     if (label2) {
          tuple_add_member_label(tuple, data, label2);
     }
     wsdt_int_t * ou = (wsdt_int_t*)data->data;
     *ou = u;
     return data;
}

static inline wsdata_t * tuple_member_create_uint(wsdata_t * tuple,
                                                  wsdt_uint_t u,
                                                  wslabel_t * label) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype_uint, label);
     if (!data) {
          return NULL;
     }
     wsdt_uint_t * ou = (wsdt_uint_t*)data->data;
     *ou = u;
     return data;
}
#define tuple_member_create_uint32 tuple_member_create_uint

static inline wsdata_t * tuple_member_create_uint64(wsdata_t * tuple,
                                                    wsdt_uint64_t u,
                                                    wslabel_t * label) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype_uint64, label);
     if (!data) {
          return NULL;
     }
     wsdt_uint64_t * ou = (wsdt_uint64_t*)data->data;
     *ou = u;
     return data;
}

static inline wsdata_t * tuple_member_create_int64(wsdata_t * tuple,
                                                    wsdt_int64_t u,
                                                    wslabel_t * label) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype_int64, label);
     if (!data) {
          return NULL;
     }
     wsdt_int64_t * ou = (wsdt_int64_t*)data->data;
     *ou = u;
     return data;
}


static inline wsdata_t * tuple_member_create_uint16(wsdata_t * tuple,
                                                    wsdt_uint16_t u,
                                                    wslabel_t * label) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype_uint16, label);
     if (!data) {
          return NULL;
     }
     wsdt_uint16_t * ou = (wsdt_uint16_t*)data->data;
     *ou = u;
     return data;
}

static inline wsdata_t * tuple_member_create_ts(wsdata_t * tuple,
                                                wsdt_ts_t ts,
                                                wslabel_t * label) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype_ts, label);
     if (!data) {
          return NULL;
     }
     wsdt_ts_t * pts = (wsdt_ts_t*)data->data;
     pts->sec = ts.sec;
     pts->usec = ts.usec;
     return data;
}

static inline wsdata_t * tuple_member_create_sec(wsdata_t * tuple,
                                                time_t sec,
                                                wslabel_t * label) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype_ts, label);
     if (!data) {
          return NULL;
     }
     wsdt_ts_t * pts = (wsdt_ts_t*)data->data;
     pts->sec = sec;
     pts->usec = 0;
     return data;
}

static inline void * tuple_member_create_wdep(wsdata_t * tuple, wsdatatype_t * dtype,
                                              wslabel_t * label, wsdata_t * dep) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype, label);
     if (!data) {
          return NULL;
     }
     if (dep) {
          wsdata_assign_dependency(dep, data);
     }
     return data->data;
}

static inline wsdata_t * tuple_member_create_dep_binary(wsdata_t * tuple,
                                                        wsdata_t * dep,
                                                        wslabel_t * label,
                                                        char * buf, int len) {
     
     wsdata_t * mdata = tuple_member_create_wsdata(tuple, dtype_binary, label);
     if (!mdata) {
          return NULL;
     }
     if (dep) {
          wsdata_assign_dependency(dep, mdata);
     }
     wsdt_binary_t * bin = (wsdt_binary_t *)mdata->data;
     bin->buf = buf;
     bin->len = len;
     return mdata;
}

static inline wsdata_t * tuple_member_create_dep_string(wsdata_t * tuple,
                                                        wsdata_t * dep,
                                                        wslabel_t * label,
                                                        char * buf, int len) {
     
     wsdata_t * mdata = tuple_member_create_wsdata(tuple, dtype_string, label);
     if (!mdata) {
          return NULL;
     }
     if (dep) {
          wsdata_assign_dependency(dep, mdata);
     }
     wsdt_string_t * str = (wsdt_string_t *)mdata->data;
     str->buf = buf;
     str->len = len;
     return mdata;
}

static inline wsdata_t * tuple_member_create_dep_strdetect(wsdata_t * tuple,
                                                           wsdata_t * dep,
                                                           wslabel_t * label,
                                                           char * buf, int len) {
     int i;
     for (i = 0; i < len; i++) {
          if (!isprint(buf[i])) {
               return tuple_member_create_dep_binary(tuple, dep, label, buf, len);
          }
     }
     return tuple_member_create_dep_string(tuple, dep, label, buf, len);
}

static inline void * tuple_member_create_wdep_mlabel(wsdata_t * tuple, wsdatatype_t * dtype,
                                                     wslabel_t * label,
                                                     wslabel_t * label2,
                                                     wsdata_t * dep) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype, label);
     if (!data) {
          return NULL;
     }
     if (label2) {
          tuple_add_member_label(tuple, data, label2);
     }
     if (dep) {
          wsdata_assign_dependency(dep, data);
     }
     return data->data;
}

static inline void * tuple_member_create_mlabel(wsdata_t * tuple, wsdatatype_t * dtype,
                                                wslabel_t * label,
                                                wslabel_t * label2) {
     wsdata_t * data = tuple_member_create_wsdata(tuple, dtype, label);
     if (!data) {
          return NULL;
     }
     if (label2) {
          tuple_add_member_label(tuple, data, label2);
     }
     return data->data;
}

static inline wsdata_t * tuple_create_string_wsdata(wsdata_t * tdata,
                                                    wslabel_t * label,
                                                    int len) {
     char * buf = NULL;
     int dlen = 0;
     wsdata_t * dep = wsdata_create_buffer(len, &buf, &dlen);

     if (dep) {
          wsdata_t * wsstr =
               tuple_member_create_wsdata(tdata, dtype_string,
                                          label);
          if (wsstr) {
               wsdata_assign_dependency(dep, wsstr);

               wsdt_string_t * str = (wsdt_string_t*)wsstr->data;
               str->buf = buf;
               str->len = dlen;
               return wsstr;
          }
          else {
               wsdata_delete(dep);
          }
     }
     return NULL;
}

static inline wsdt_string_t * tuple_create_string(wsdata_t * tdata,
                                                  wslabel_t * label,
                                                  int len) {
     char * buf = NULL;
     int dlen = 0;
     wsdata_t * dep = wsdata_create_buffer(len, &buf, &dlen);

     if (dep) {
          wsdt_string_t * str =
               (wsdt_string_t*) tuple_member_create_wdep(tdata, dtype_string,
                                                         label, dep);
          if (str) { 
               str->buf = buf;
               str->len = dlen;
               return str;
          }
          else {
               wsdata_delete(dep);
          }
     }
     return NULL;
}

static inline wsdata_t * tuple_dupe_string(wsdata_t * tdata,
                                                  wslabel_t * label,
                                                  const char * dupeme,
                                                  int len) {
     wsdata_t * wsd = tuple_create_string_wsdata(tdata, label, len);
     if (wsd) {
          wsdt_string_t * str = (wsdt_string_t *)wsd->data;
          if (str->len >= len) {
               memcpy(str->buf, dupeme, len);
          }
          else {
               memcpy(str->buf, dupeme, str->len);
          }
     }
     return wsd;
}

static inline wsdt_binary_t * tuple_create_binary(wsdata_t * tdata,
                                                  wslabel_t * label,
                                                  int len) {
     char * buf;
     int dlen;
     wsdata_t * dep = wsdata_create_buffer(len, &buf, &dlen);
     if (dep) {
          wsdt_binary_t * str =
               (wsdt_binary_t*) tuple_member_create_wdep(tdata, dtype_binary,
                                                         label, dep);
          if (str) {
               str->buf = buf;
               str->len = dlen;
               return str;
          }
          else {
               wsdata_delete(dep);
          }
     }
     return NULL;
}

static inline wsdata_t * tuple_create_binary_wsdata(wsdata_t * tdata,
                                                    wslabel_t * label,
                                                    int len) {
     char * buf = NULL;
     int dlen = 0;
     wsdata_t * dep = wsdata_create_buffer(len, &buf, &dlen);

     if (dep) {
          wsdata_t * wsstr =
               tuple_member_create_wsdata(tdata, dtype_binary,
                                          label);
          if (wsstr) {
               wsdata_assign_dependency(dep, wsstr);

               wsdt_binary_t * str = (wsdt_binary_t*)wsstr->data;
               str->buf = buf;
               str->len = dlen;
               return wsstr;
          }
          else {
               wsdata_delete(dep);
          }
     }
     return NULL;
}

static inline wsdt_binary_t * tuple_create_binary_mlabel(wsdata_t * tdata,
                                                         wslabel_t * label,
                                                         wslabel_t * label2,
                                                         int len) {
     wsdata_t * wsd = tuple_create_binary_wsdata(tdata, label, len);
     if (!wsd) {
          return NULL;
     }
     if (label2) {
          tuple_add_member_label(tdata, wsd, label2);
     }

     return (wsdt_binary_t *)wsd->data;
}

static inline wsdt_string_t * tuple_create_string_mlabel(wsdata_t * tdata,
                                                         wslabel_t * label,
                                                         wslabel_t * label2,
                                                         int len) {
     wsdata_t * wsd = tuple_create_string_wsdata(tdata, label, len);
     if (!wsd) {
          return NULL;
     }
     if (label2) {
          tuple_add_member_label(tdata, wsd, label2);
     }

     return (wsdt_string_t *)wsd->data;
}

static inline wsdata_t * tuple_dupe_binary(wsdata_t * tdata,
                                           wslabel_t * label,
                                           const char * dupeme,
                                           int len) {
     wsdata_t * wsd = tuple_create_binary_wsdata(tdata, label, len);
     if (wsd) {
          wsdt_binary_t * str = (wsdt_binary_t *)wsd->data;
          if (str->len >= len) {
               memcpy(str->buf, dupeme, len);
          }
          else {
               memcpy(str->buf, dupeme, str->len);
          }
     }
     return wsd;
}

//return hashed value of string
static inline uint64_t tuple_hash_string(uint8_t * str, int len) {
     return evahash64(str, len, 0x5EED5EED);
}

static inline int tuple_hash_member_into(wsdata_t * tdata, wslabel_t * label, 
                                                uint64_t * hash) {
     if (!tdata || !label) {
          return 0;
     }
     wsdt_tuple_t * tuple = (wsdt_tuple_t *)tdata->data;
     uint32_t i;
     int j;
     int found = 0;
#ifndef USE_ATOMICS
     WS_SPINLOCK_LOCK(&tuple->lock);
#endif
     for (i = 0; i < tuple->len; i++) {
#ifndef USE_ATOMICS
          WS_SPINLOCK_LOCK(&tuple->member[i]->lock);
#endif
          for (j = 0; j < tuple->member[i]->label_len; j++) {
               if (tuple->member[i]->labels[j] == label) {
                    if (tuple->member[i]->dtype->hash_func) {
                         ws_hashloc_t * hashloc;
                         hashloc =
                              tuple->member[i]->dtype->hash_func(tuple->member[i]);
                         if (hashloc->offset) {
                              *hash += evahash64((uint8_t*)hashloc->offset,
                                                 hashloc->len, 0x5EED5EED);
                              found = 1;
                              break;
                         }
                    }
               }
          }
#ifndef USE_ATOMICS
          WS_SPINLOCK_UNLOCK(&tuple->member[i]->lock);
#endif
          if (found) break;
     }
#ifndef USE_ATOMICS
     WS_SPINLOCK_UNLOCK(&tuple->lock);
#endif
     return found;
}

//search element
typedef struct _ws_tsearchel_t {
     wslabel_t * label;
     int id;
     struct _ws_tsearchel_t * next;
} ws_tsearchel_t;


static inline void tuple_set_primary_hash(wsdata_t * tdata, uint64_t * hash) {
     tdata->hashloc.offset = (void *)hash; 
     tdata->hashloc.len = sizeof(uint64_t);
     tdata->has_hashloc = 1;
}


/* tuple search map..
     given a label and a hash of the label..

     want a way to more quickly search tuple..
     properties of search:
          fast initialization of data struct.. alloc on use..
          constant memory alloc for search table..
          minimal lookup time..
          search by label.. get member set..

*/

// return 0 if item cannot be fit in search table.. 1 if it can be searched
static inline int wstuple_add_search_label(wsdata_t * wsd_tuple,
                                           wsdata_t * member, wslabel_t * label) {
     dprint("tuple search add label");
     if (!label->index_id) {
          return 0;
     }
     dprint("here at add_search_label");
     wsdt_tuple_t * tuple = (wsdt_tuple_t*)wsd_tuple->data;

     dprint("tuple search here");
     int id = label->index_id - 1;

     if (id >= (int)tuple->index_len) {
          status_print("tuple search label map not big enough:  id %d, tuple->index_len %d\n", 
                       id, tuple->index_len);
          return 0;
          
     }
     else if (tuple->icnt[id] >= (int)tuple->max) {
          status_print("tuple search label map not big enough:  id %d, tuple->icnt[id] %d, tuple->max %d", 
                       id, tuple->icnt[id], tuple->max);
          return 0;
     }

     wsdata_t ** index_array = TUPLE_INDEX_ARRAY(tuple, id);
     index_array[tuple->icnt[id]] = member;
     tuple->icnt[id]++;
     dprint("here at add_search_label - has %d labels", tuple->icnt[id]);
     return 1;
}

static inline int tuple_attach_member_labels(wsdata_t * wsd_tuple,
                                             wsdata_t * member) {
     int i;

     dprint("tuple search add member labels");
     int rtn = 1;
     if (!wsd_tuple) {
          return 0;
     }

     for (i = 0; i < member->label_len; i++) {
          rtn &= wstuple_add_search_label(wsd_tuple, member, member->labels[i]);
     }
     return rtn;
}

static inline int tuple_add_member_label(wsdata_t * wsd_tuple,
                                         wsdata_t * member, wslabel_t * label) {
     dprint("tuple search add member label");
     if (!wsd_tuple || !member || !label) {
          return 0;
     }
     wsdata_add_label(member, label);
     return wstuple_add_search_label(wsd_tuple, member, label);
}

static inline void tuple_duplicate_member_labels(wsdata_t * tdata, wsdata_t * src, wsdata_t * dst) {
     int i;
     for (i = 0; i < src->label_len; i++) {
          tuple_add_member_label(tdata, dst, src->labels[i]);
     }
}

// search for label in tuple.. return list of matched members
static inline int tuple_find_label(wsdata_t * wsd_tuple, wslabel_t * label,
                                   int * m_len, wsdata_t *** tp_members) {
     dprint("tuple search find label");
     if (!label->index_id) {
          return 0;
     }
     dprint("here at tuple_find_label");
     wsdt_tuple_t * tuple = (wsdt_tuple_t*)wsd_tuple->data;

     int id = label->index_id - 1;

     if (id >= (int)tuple->index_len) {
          dprint("tuple index mapper failure");
          return 0;
     }

     if (!tuple->icnt[id]) {
          return 0;
     }
     *tp_members = TUPLE_INDEX_ARRAY(tuple, id);
     *m_len = tuple->icnt[id];

     dprint("here at tuple_find_label - found %d labels", tuple->icnt[id]);

     
     return 1;
}

//compute hash for searched items in tuplesearch list.. store hash in tuple
static inline uint32_t tuple_find_label_hash_ordered(wsdata_t * tdata,
                                                     wslabel_set_t * lset,
                                                     uint64_t * hash, uint32_t key) {
     int i, j;
     uint32_t found = 0;
     int len;
     wsdata_t ** members;

     for (i = 0; i < lset->len; i++) {
          if (tuple_find_label(tdata, lset->labels[i], &len, &members)) {
               for (j = 0; j < len; j++) {
                    if (members[j]->dtype->hash_func) {
                         ws_hashloc_t * hashloc;
                         hashloc =
                              members[j]->dtype->hash_func(members[j]);
                         if (hashloc->offset) {
                              *hash += evahash64((uint8_t*)hashloc->offset,
                                                 hashloc->len, key + i);
                              found++;
                         }
                    }
               }
          }
     }

     return found;
}

//compute hash for searched items in tuplesearch list.. store hash in tuple
static inline uint32_t tuple_find_label_hash(wsdata_t * tdata,
                                             wslabel_set_t * lset,
                                             uint64_t * hash, uint32_t key) {
     int i, j;
     uint32_t found = 0;
     int len;
     wsdata_t ** members;

     for (i = 0; i < lset->len; i++) {
          if (tuple_find_label(tdata, lset->labels[i], &len, &members)) {
               for (j = 0; j < len; j++) {
                    if (members[j]->dtype->hash_func) {
                         ws_hashloc_t * hashloc;
                         hashloc =
                              members[j]->dtype->hash_func(members[j]);
                         if (hashloc && hashloc->offset && hashloc->len) {
                              *hash += evahash64((uint8_t*)hashloc->offset,
                                                 hashloc->len, key);
                              found++;
                         }
                    }
               }
          }
     }

     return found;
}

//return first member that matches a label in a tuple..
static inline wsdata_t * tuple_find_single_label(wsdata_t * tdata, wslabel_t * searchlabel) {
     int len;
     wsdata_t ** members;
     if (tuple_find_label(tdata, searchlabel, &len, &members)) {
          if (len > 0) {
               return members[0];
          }
     }
     return NULL;
}

//return first member that matches a label in a tuple..
static inline wsdata_t * tuple_reversefind_single_label(wsdata_t * tdata,
                                                        wslabel_t * searchlabel) {
     return tuple_find_single_label(tdata, searchlabel);
}

typedef struct _tuple_labelset_iter_t {
     int labelpos;
     int memberpos;
     int mlen;
     wsdata_t * tdata;
     wslabel_set_t * lset;
     wsdata_t ** member_list;
} tuple_labelset_iter_t;

static inline void tuple_init_labelset_iter(tuple_labelset_iter_t * iter, 
                                            wsdata_t * tdata,
                                            wslabel_set_t * lset) {
     iter->tdata = tdata;
     iter->lset = lset;
     iter->mlen = 0;
     iter->labelpos = 0;
     iter->memberpos = 0;
     iter->member_list = NULL;
}

static inline int tuple_search_labelset(tuple_labelset_iter_t * iter,
                                        wsdata_t ** member, wslabel_t ** label,
                                        int * id) {

     if (iter->mlen) {
          iter->memberpos++;
          if (iter->memberpos < iter->mlen) {
               *label = iter->lset->labels[iter->labelpos];
               *id = iter->lset->id[iter->labelpos];
               *member = iter->member_list[iter->memberpos];
               return 1;
          }
          else {
               iter->labelpos++;
               iter->mlen = 0;
          }
     }
     int i;
     int len;
     wsdata_t ** members;

     for (i = iter->labelpos; i < iter->lset->len; i++) {
          if (tuple_find_label(iter->tdata, iter->lset->labels[i], &len, &members)) {
               iter->labelpos = i;
               iter->mlen = len;
               iter->member_list = members;
               iter->memberpos = 0;
               *label = iter->lset->labels[i];
               *id = iter->lset->id[i];
               *member = members[0];
               return 1;
          }
     }
     return 0;
}

typedef int (*tuple_nested_search_callback)(void *, void *, wsdata_t *, wsdata_t *);

static inline int tuple_nested_search_loop(wsdata_t * tdata,
                                           wslabel_nested_set_t * nest,
                                           tuple_nested_search_callback callback,
                                           void * vdata1, void * vdata2,
                                           int sid) {
     int i, j;
     int cnt = 0;
     wsdata_t ** mset;
     int mset_len;
     wslabel_set_t * lset = &nest->lset[sid];

     for (i = 0; i < lset->len; i++) {
          if (tuple_find_label(tdata, lset->labels[i], &mset_len, &mset)) {
               if (lset->id[i]) {
                    for (j = 0; j < mset_len; j++ ) {
                         //search for sublabels
                         if (mset[j]->dtype == dtype_tuple) {
                              cnt += tuple_nested_search_loop(mset[j], nest,
                                                              callback, vdata1, vdata2,
                                                              lset->id[i]);
                         }
                    }
               }
               else {
                    for (j = 0; j < mset_len; j++ ) {
                         cnt += callback(vdata1, vdata2, tdata, mset[j]);
                    }
               }
          }
     }
     return cnt;
}

static inline int tuple_nested_search(wsdata_t * tuple,
                                      wslabel_nested_set_t * nest,
                                      tuple_nested_search_callback callback,
                                      void * vdata1, void * vdata2) {

     return tuple_nested_search_loop(tuple, nest, callback, vdata1, vdata2, 0);

}

typedef int (*tuple_nested_search_callback2)(void *, void *,
                                             wsdata_t * /*subtuple*/,
                                            wsdata_t * /*member*/,
                                            wsdata_t * /*subtuple parent*/);


static inline int tuple_nested_search_loop2(wsdata_t * tdata,
                                            wslabel_nested_set_t * nest,
                                            tuple_nested_search_callback2 callback,
                                            void * vdata1, void * vdata2,
                                            int sid, wsdata_t * tparent) {
     int i, j;
     int cnt = 0;
     wsdata_t ** mset;
     int mset_len;
     wslabel_set_t * lset = &nest->lset[sid];

     for (i = 0; i < lset->len; i++) {
          if (tuple_find_label(tdata, lset->labels[i], &mset_len, &mset)) {
               if (lset->id[i]) {
                    for (j = 0; j < mset_len; j++ ) {
                         //search for sublabels
                         if (mset[j]->dtype == dtype_tuple) {
                              cnt += tuple_nested_search_loop2(mset[j], nest,
                                                               callback, vdata1, vdata2,
                                                               lset->id[i],
                                                               tdata);
                         }
                    }
               }
               else {
                    for (j = 0; j < mset_len; j++ ) {
                         cnt += callback(vdata1, vdata2, tdata, mset[j], tparent);
                    }
               }
          }
     }
     return cnt;
}


static inline int tuple_nested_search2(wsdata_t * tuple,
                                      wslabel_nested_set_t * nest,
                                      tuple_nested_search_callback2 callback,
                                      void * vdata1, void * vdata2) {

     return tuple_nested_search_loop2(tuple, nest, callback, vdata1, vdata2, 0, NULL);

}

typedef int (*tuple_nested_search_ext_callback)(void *, void *, wsdata_t *,
                                                wsdata_t *, wslabel_t * ,
                                                int);

static inline int tuple_nested_search_ext_loop(wsdata_t * tdata,
                                               wslabel_nested_set_ext_t * nest,
                                               tuple_nested_search_ext_callback callback,
                                               void * vdata1, void * vdata2,
                                               int sid) {
     int i, j;
     int cnt = 0;
     wsdata_t ** mset;
     int mset_len;
     wslabel_set_ext_t * lset = &nest->lset[sid];

     for (i = 0; i < lset->len; i++) {
          if (tuple_find_label(tdata, lset->labels[i], &mset_len, &mset)) {
               if (lset->nid[i]) {
                    for (j = 0; j < mset_len; j++ ) {
                         //search for sublabels
                         if (mset[j]->dtype == dtype_tuple) {
                              cnt += tuple_nested_search_ext_loop(mset[j], nest,
                                                                  callback, vdata1, vdata2,
                                                                  lset->nid[i]);
                         }
                    }
               }
               else {
                    for (j = 0; j < mset_len; j++ ) {
                         cnt += callback(vdata1, vdata2, tdata, mset[j],
                                         lset->labels[i], lset->uid[i]);
                    }
               }
          }
     }
     return cnt;
}


static inline int tuple_nested_search_ext(wsdata_t * tuple,
                                          wslabel_nested_set_ext_t * nest,
                                          tuple_nested_search_ext_callback callback,
                                          void * vdata1, void * vdata2) {

     return tuple_nested_search_ext_loop(tuple, nest, callback, vdata1, vdata2, 0);

}


typedef struct _ws_tuplemember_t {
     wsdatatype_t * dtype;
     wslabel_t * label;
} ws_tuplemember_t;

//call this function at processor initialization or input_set to register
// a member type
static inline ws_tuplemember_t * register_tuple_member_type(void * type_table,
                                                            const char * type_name,
                                                            const char * label_name) {
     if (!type_name) {
          return NULL;
     }
     wsdatatype_t * dtype = wsdatatype_get(type_table, type_name);
     if (!dtype) {
          return NULL;
     }
     ws_tuplemember_t * tmember = (ws_tuplemember_t*)calloc(1, sizeof(ws_tuplemember_t));

     // save tmember in a queue, for release of allocations at termination
     if (!dtype->tmembers) {
          dtype->tmembers = queue_init();
     }
     queue_add(dtype->tmembers, tmember);

     tmember->dtype = dtype;
     if (label_name) {
          tmember->label = wsregister_label(type_table, label_name);
     }
     return tmember;
}

// call this function when processing data to allocate a new member based on a
// registered type and label
static inline wsdata_t * tuple_member_alloc_wsdata(wsdata_t * tuple, ws_tuplemember_t* tmember) {
     dprint("tuple_member_alloc_wsdata");
     if (!tmember || !tuple) {
          return NULL;
     }
     wsdata_t * data = wsdata_alloc(tmember->dtype);
     if (!data) {
          return NULL;
     }
     wsdata_add_label(data, tmember->label);
     if (!add_tuple_member(tuple, data)) {
          wsdata_delete(data);
          return NULL;
     }
     return data;
}

// call this function when processing data to allocate a new member based on a
// registered type and label.. variant.. returns data pointer
static inline void * tuple_member_alloc(wsdata_t * tuple, ws_tuplemember_t* tmember) {
     wsdata_t * data = tuple_member_alloc_wsdata(tuple, tmember);

     if (data) {
          return data->data;
     }
     else {
          return NULL;
     }
}

static inline void * tuple_member_alloc_label(wsdata_t * tuple,
                                              ws_tuplemember_t* tmember,
                                              wslabel_t* label) {
     wsdata_t * data = tuple_member_alloc_wsdata(tuple, tmember);
     if (data) {
          if (label) {
               tuple_add_member_label(tuple, data, label);
          }
          return data->data;
     }
     else {
          return NULL;
     }
}

#endif
