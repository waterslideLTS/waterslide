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
// defines the basic data type resolver structure.. includes callback functions

#ifndef _WATERSLIDEDATA_H
#define _WATERSLIDEDATA_H

#include <stdio.h>
#include <time.h>
#include "wsfree_list.h"
#include "listhash.h"
#include "waterslide.h"
#include "wsqueue.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#define WS_MAX_TYPES 5000
#define WS_MAX_LABELS 5000

#define WSDATA_HASH_SEED 0xFEEDF00D

#define MAX_WSD_SEARCH_TERMS 1024

typedef int (*wsdatatype_to_string)(wsdata_t *, char **, int*);
typedef int (*wsdatatype_to_uint64)(wsdata_t *, uint64_t*);
typedef int (*wsdatatype_to_uint32)(wsdata_t *, uint32_t*);
typedef int (*wsdatatype_to_int32)(wsdata_t *,  int32_t*);
typedef int (*wsdatatype_to_int64)(wsdata_t *,  int64_t*);
typedef int (*wsdatatype_to_double)(wsdata_t *, double*);

typedef void (*wsdatatype_init)(wsdata_t *, wsdatatype_t *);

typedef void (*wsdatatype_delete)(wsdata_t *);

typedef wsdata_t * (*wsdatatype_copy)(wsdata_t *);

typedef uint8_t * (*wsdatatype_serialize)(wsdata_t *, int *);

#define WS_PRINTTYPE_TEXT   1
#define WS_PRINTTYPE_BINARY 2
#define WS_PRINTTYPE_XML    3
#define WS_PRINTTYPE_HTML   4
typedef int (*wsdatatype_print)(FILE * , wsdata_t *,
                                uint32_t /*printtype*/);

typedef int (*wsdatatype_snprint)(char * /*buffer*/, int /*len*/,
                                 wsdata_t *, uint32_t /*printtype*/);

typedef int (*wsdatatype_sscan)(wsdata_t *, char *, int len);

typedef ws_hashloc_t * (*wsdatatype_hash)(wsdata_t *);

typedef wsdata_t * (*wsdatatype_subelement)(wsdata_t * /*parent*/,
                                            wsdata_t * /*dst*/,
                                            void * /*aux*/);

typedef struct _wssubelement_t {
     wslabel_t * label;
     char * dtype_name;
     wsdatatype_t * dtype;
     wsdatatype_subelement sub_func;
     void * aux_data;
     int offset;
} wssubelement_t;

#define WSDTYPE_MAX_SUBELEMENTS 64

typedef struct _wsdtype_sub_list_t {
     int len;
     wssubelement_t * subs[WSDTYPE_MAX_SUBELEMENTS];
} wsdtype_sub_list_t;

//generic structure for data type, lengths and hash functions
struct _wsdatatype_t {
     char * name;    //name of data type
     uint64_t namehash;
     int  len;
     wsfree_list_t * freeq;
     wsfree_list_t * freeq_ptr;
     WS_MUTEX_DECL(mutex);
     nhqueue_t * tmembers;
   
     wsdatatype_hash hash_func;

     wsdatatype_init init_func;
     
     wsdatatype_delete delete_func;
     
     wsdatatype_print print_func; 
     wsdatatype_snprint snprint_func; 
     wsdatatype_sscan sscan_func; 
     
     //used to copy isref data
     wsdatatype_copy copy_func;

     wsdatatype_to_string to_string;
     wsdatatype_to_uint64 to_uint64;
     wsdatatype_to_uint32 to_uint32;
     wsdatatype_to_int64  to_int64;
     wsdatatype_to_int32  to_int32;
     wsdatatype_to_double to_double;

     // take data and transmit over network
     wsdatatype_serialize serialize_func;

     void * instance;
     
     int num_subelements;
     wssubelement_t subelements[WSDTYPE_MAX_SUBELEMENTS];
};

void wsdatatype_profile(void *, void *);
void wsdatatype_profile_verbose(void *, void *);
void wsdatatype_free_dtypes(void *, void *);
void wsdatatype_free_label_names(void *, void *);
void wsdatatype_free_module_names(void *, void *);

static inline int wsdatatype_register_subelement(wsdatatype_t * dtype,
                                                 void * type_table,
                                                 const char * label,
                                                 const char * dtypename,
                                                 int offset) {
     if (dtype->num_subelements >= WSDTYPE_MAX_SUBELEMENTS) {
          return 0;
     }

     wssubelement_t * sub = &dtype->subelements[dtype->num_subelements];
     dtype->num_subelements++;

     sub->label = wsregister_label(type_table, label);
     if (dtypename) {
          sub->dtype_name = strdup(dtypename);
     }
     sub->sub_func = NULL;
     sub->aux_data = NULL;
     sub->offset = offset;
     return 1;
}

static inline int wsdatatype_register_subelement_func(wsdatatype_t * dtype,
                                                      void * type_table,
                                                      const char * label,
                                                      const char * dtypename,
                                                      wsdatatype_subelement sub_func,
                                                      void * aux) {
     if (dtype->num_subelements >= WSDTYPE_MAX_SUBELEMENTS) {
          return 0;
     }

     wssubelement_t * sub = &dtype->subelements[dtype->num_subelements];
     dtype->num_subelements++;

     sub->label = wsregister_label(type_table, label);
     if (dtypename) {
          sub->dtype_name = strdup(dtypename);
     }
     sub->sub_func = sub_func;
     sub->aux_data = aux;
     return 1;
}


static inline wssubelement_t * wsdatatype_find_subelement(wsdatatype_t * dtype,
                                                          wslabel_t * label) {
     int i;
     wssubelement_t * sub;
     for (i = 0; i < dtype->num_subelements; i++) {
          sub = &dtype->subelements[i];
          if (sub->label == label) {
               return sub;
          }
     }
     return NULL;
}

static inline int wsdtype_build_subelement_list(wsdatatype_t * dtype,
                                                wslabel_set_t * lset,
                                                wsdtype_sub_list_t * slist) {
     if (!lset || !slist) {
          return 0;
     }
     int i;
     wssubelement_t * sub;
     for (i = 0; i < lset->len; i++) {
          sub = wsdatatype_find_subelement(dtype, lset->labels[i]);
          if (sub) {
               slist->subs[slist->len] = sub;
               slist->len++;
          }
          if (slist->len >= WSDTYPE_MAX_SUBELEMENTS) {
               break;
          }
     }
     return slist->len;
}

static inline void wsdata_core_init(wsdata_t * wsdata, wsdatatype_t * dtype) {
     wsdata->references = 0;
     wsdata->has_hashloc = 0;
     wsdata->label_len = 0;
     wsdata->writer_label_len = 0;
     wsdata->dtype = dtype;
     WS_SPINLOCK_INIT(&wsdata->lock);
}

//allocate & init new data from datatype -- useful for every module
static inline wsdata_t * wsdata_alloc(wsdatatype_t *);

static inline wsdata_t * wsdata_get_subelement(wsdata_t * wsd, wssubelement_t * sub) {
     if (!sub || !wsd) {
          return NULL;
     }
     if (!sub->dtype) {
          if (sub->sub_func) {
               wsdata_t * out = sub->sub_func(wsd, NULL, sub->aux_data);
               if (out && sub->label && !wsdata_check_label(out, sub->label)) {
                    wsdata_add_label(out, sub->label);
               }
               return out;
          }
          return NULL;
     }
     wsdata_t * cpy = wsdata_alloc(sub->dtype);
     if (!cpy) {
          return NULL;
     }
     wsdata_add_label(cpy, sub->label);
     if (sub->sub_func) {
          return sub->sub_func(wsd, cpy, sub->aux_data);
     }
     else {
          //use offset
          memcpy((void * __restrict__)cpy->data, 
                 (void * __restrict__)((uint8_t*)wsd->data + sub->offset),
                 sub->dtype->len);
          return cpy;
     }
}

static inline void wsdata_delete(wsdata_t * data) {
     data->dtype->delete_func(data);
}

int wsdatatype_register_alias(void *, wsdatatype_t *, const char *);
//register a data type in a global type registry.
//  allowed for a centralized control of types
//   a new data type could be loaded from a module/shared object
wsdatatype_t * wsdatatype_register(void *,
                                   const char *, int,
                                   wsdatatype_hash,
                                   wsdatatype_init,
                                   wsdatatype_delete,
                                   wsdatatype_print,
                                   wsdatatype_snprint,
                                   wsdatatype_copy,
                                   wsdatatype_serialize);

wsdatatype_t * wsdatatype_register_generic(void *,
                                           const char *, int);
wsdatatype_t * wsdatatype_register_generic_ts(void *,
                                              const char *, int);

wsdatatype_t * wsdatatype_register_generic_pf(void *,
                                              const char *, int,
                                              wsdatatype_print,
                                              wsdatatype_snprint);
wsdatatype_t * wsdatatype_register_generic_pf_ts(void *,
                                                 const char *, int,
                                                 wsdatatype_print,
                                                 wsdatatype_snprint);

ws_hashloc_t* wsdatatype_default_hash(wsdata_t *);
ws_hashloc_t* wsdatatype_default_hash_ts(wsdata_t * wsdata);
void wsdatatype_default_init(wsdata_t *, wsdatatype_t*);
void wsdatatype_default_delete(wsdata_t *);
wsdata_t * wsdatatype_default_copy(wsdata_t *);
uint8_t * wsdatatype_default_serialize(wsdata_t *, int *);

int wsdatatype_default_print(FILE * , wsdata_t *, uint32_t);
int wsdatatype_default_snprint(char *, int , wsdata_t *, uint32_t);

static inline wsdata_t * wsdata_alloc(wsdatatype_t * dtype) {
     wsdata_t * newdata;

     newdata = (wsdata_t*) wsfree_list_alloc(dtype->freeq);
     if (NULL == newdata) return NULL;

     // we got alloc'd data
     // now init it..
     wsdata_core_init(newdata, dtype);

     if (dtype->init_func) {
          dtype->init_func(newdata, dtype);
     }
     else {
          wsdatatype_default_init(newdata, dtype);
     }

     return newdata;
}

static inline wsdata_t * wsdata_ptr(wsdata_t * ref) {
     wsdata_t * newdata;
     if (!ref) {
          return NULL;
     }

     newdata = (wsdata_t*) wsfree_list_alloc(ref->dtype->freeq_ptr);
     if (NULL == newdata) return NULL;

     // now init it..
     wsdata_core_init(newdata, ref->dtype);
     newdata->data = ref->data;
     newdata->isptr = 1;

     wsdata_assign_dependency(ref, newdata);
     
     return newdata;
}

static inline wsdata_t * wsdata_ptr_wlabels(wsdata_t * ref) {
     wsdata_t * newdata = wsdata_ptr(ref);
     if (newdata && ref) {
          wsdata_duplicate_labels(ref, newdata);
     }
     return newdata;
}

static inline void wsdata_moveto_freeq(wsdata_t* wsdata) {
     if (wsdata->isptr) {
          wsfree_list_free(wsdata->dtype->freeq_ptr, &wsdata->fl_node);
     } else {
          wsfree_list_free(wsdata->dtype->freeq, &wsdata->fl_node);
     }
}

/// NOW to define some default data types...
/// DEFINE a timestamp data type...
#define WSDT_TS_STR "TS_"
#define WSDT_TS_TYPE_STR "TS_TYPE"

//structure for a ip tag tuple..
typedef struct _wsdt_ts_t {
     time_t sec;
     time_t usec;
} wsdt_ts_t;

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WATERSLIDEDATA_H
