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
//#define DEBUG 1
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include "wsfree_list.h"
#include "wsstack.h"
#include "listhash.h"
#include "waterslide.h"
#include "waterslidedata.h"
#include "mimo.h"

// Globals
extern pthread_mutexattr_t mutex_attr;


void wsdatatype_profile_verbose(void * data, void * ignore) {
     wsdatatype_t * dtype = (wsdatatype_t *)data;
     uint32_t allocated = wsfree_list_allocated(dtype->freeq);
     uint32_t allocated_ptr = wsfree_list_allocated(dtype->freeq_ptr);

     if (allocated) {
          uint32_t buffered = wsfree_list_size(dtype->freeq);
          status_print("dtype %s: allocd %u recovered %u %s",
                       dtype->name,
                       allocated, 
                       buffered, 
                       (allocated != buffered) ? "ERROR" : "");
     }
     if (allocated_ptr) {
          uint32_t buffered_ptr = wsfree_list_size(dtype->freeq_ptr);
          status_print("dtype %s ptr: allocd %u recovered %u %s",
                       dtype->name,
                       allocated_ptr, 
                       buffered_ptr, 
                       (allocated_ptr != buffered_ptr) ? "ERROR" : "");
     }
}

void wsdatatype_profile(void * data, void *ignore) {
     wsdatatype_t * dtype = (wsdatatype_t *)data;
     uint32_t allocated = wsfree_list_allocated(dtype->freeq);
     uint32_t allocated_ptr = wsfree_list_allocated(dtype->freeq_ptr);
     int flag_error = 0;

     if (allocated || allocated_ptr) {
          uint32_t buffered = wsfree_list_size(dtype->freeq);
          uint32_t buffered_ptr = wsfree_list_size(dtype->freeq_ptr);
          if ((allocated != buffered) || (allocated_ptr != buffered_ptr)) {
               if (!flag_error) {
                    fprintf(stderr,"\nWS Memory Cleanup Errors:\n");
                    flag_error = 1;
               }
               wsdatatype_profile_verbose(data, ignore);
          }
     }
}

static inline void free_dtype_freeq_data(wsfree_list_t *fl, int hasdata) {

#ifndef WS_PTHREADS
     void * node = wsstack_remove(fl->stack);
     while (node) {
          wsdata_t *newdata = (wsdata_t *)node;

          if (newdata->dependency) {
               wsstack_destroy(newdata->dependency);
          }
          if (hasdata && newdata->data) {
               free(newdata->data);
          }
          free(node);
          node = wsstack_remove(fl->stack);
     }

#elif defined(USE_UNHOMED_TLS_FREE_LIST)
     wsfree_list_local_cache_t * cache = fl->caches;
     wsfree_list_node_t * node;
     while (cache) {
          cache = cache->next;
          node = cache->cache;
          while (node) {
               wsdata_t * newdata = (wsdata_t *)node;
               if (newdata->dependency) {
                    wsstack_destroy(newdata->dependency);
               }
               if (hasdata && newdata->data) {
                    free(newdata->data);
               }
               node = node->next;
          }
     }
#elif defined(USE_ATOMIC_STACK)
     void * node = wsstack_atomic_remove(fl->stack);
     while (node) {
          wsdata_t * newdata = (wsdata_t *)node;

          if (newdata->dependency) {
               wsstack_destroy(newdata->dependency);
          }
          if (hasdata && newdata->data) {
               free(newdata->data);
          }
          free(node);
          node = wsstack_atomic_remove(fl->stack);
     }

#elif defined(USE_MUTEX_HOMED_FREE_LIST) || defined (USE_ATOMIC_HOMED_FREE_LIST)
     wsfree_list_local_cache_t * cache = fl->caches;
     wsfree_list_node_t *element;
     while (cache) {
          element = cache->queue_head;
          while (element) {
               wsdata_t * newdata = (wsdata_t *)element;
               if (newdata->dependency) {
                    wsstack_destroy(newdata->dependency);
               }
               if (hasdata && newdata->data) {
                    free(newdata->data);
               }
               element = element->next;
               free(newdata);
          }
          cache = cache->next;
     }
#endif // !WS_PTHREADS
}

static inline void free_dtype_hugeblock_freeq_data(wsfree_list_t *fl, int hasdata) {

#ifndef WS_PTHREADS
     void * node = wsstack_remove(fl->stack);
     while (node) {
          wsdata_t *newdata = (wsdata_t *)node;

          if (newdata->dependency) {
               wsstack_destroy(newdata->dependency);
          }
          if (hasdata && newdata->data) {
               wsdt_hugeblock_t * hb = (wsdt_hugeblock_t *)newdata->data;
               if (hb->buf) {
                    free(hb->buf);
               }
               free(newdata->data);
          }
          free(node);
          node = wsstack_remove(fl->stack);
     }

#elif defined(USE_UNHOMED_TLS_FREE_LIST)
     wsfree_list_local_cache_t * cache = fl->caches;
     wsfree_list_node_t * node;
     while (cache) {
          cache = cache->next;
          node = cache->cache;
          while (node) {
               wsdata_t * newdata = (wsdata_t *)node;
               if (newdata->dependency) {
                    wsstack_destroy(newdata->dependency);
               }
               if (hasdata && newdata->data) {
                    wsdt_hugeblock_t * hb = (wsdt_hugeblock_t *)newdata->data;
                    if (hb->buf) {
                         free(hb->buf);
                    }
                    free(newdata->data);
               }
               node = node->next;
          }
     }
#elif defined(USE_ATOMIC_STACK)
     void * node = wsstack_atomic_remove(fl->stack);
     while (node) {
          wsdata_t * newdata = (wsdata_t *)node;

          if (newdata->dependency) {
               wsstack_destroy(newdata->dependency);
          }
          if (hasdata && newdata->data) {
               wsdt_hugeblock_t * hb = (wsdt_hugeblock_t *)newdata->data;
               if (hb->buf) {
                    free(hb->buf);
               }
               free(newdata->data);
          }
          free(node);
          node = wsstack_atomic_remove(fl->stack);
     }

#elif defined(USE_MUTEX_HOMED_FREE_LIST) || defined (USE_ATOMIC_HOMED_FREE_LIST)
     wsfree_list_local_cache_t * cache = fl->caches;
     wsfree_list_node_t *element;
     while (cache) {
          element = cache->queue_head;
          while (element) {
               wsdata_t * newdata = (wsdata_t *)element;
               if (newdata->dependency) {
                    wsstack_destroy(newdata->dependency);
               }
               if (hasdata && newdata->data) {
                    wsdt_hugeblock_t * hb = (wsdt_hugeblock_t *)newdata->data;
                    if (hb->buf) {
                         free(hb->buf);
                    }
                    free(newdata->data);
               }
               element = element->next;
               free(newdata);
          }
          cache = cache->next;
     }
#endif // !WS_PTHREADS
}

void wsdatatype_free_dtypes(void * data, void * ignore) {
     wsdatatype_t * dtype = (wsdatatype_t *)data;

     // free dtype name
     if (dtype->name) {
          free(dtype->name);
     }

     // free dtype subelement names
     int i;
     for (i = 0; i < dtype->num_subelements; i++) {
          wssubelement_t * sub = &dtype->subelements[i];
          if (sub->aux_data) {
               free(sub->aux_data);
          }
          if (sub->dtype_name) {
               free(sub->dtype_name);
          }
     }

     // free dtype free queue entries
     // second parameter is set when newdata->data was
     // dynamically allocated and needs to be freed
     if (dtype == dtype_hugeblock) {
          free_dtype_hugeblock_freeq_data(dtype->freeq, 1);
          free_dtype_hugeblock_freeq_data(dtype->freeq_ptr, 0);
     }
     else {
          free_dtype_freeq_data(dtype->freeq, 1);
          free_dtype_freeq_data(dtype->freeq_ptr, 0);
     }

     // free dtype free queues
     wsfree_list_destroy(dtype->freeq_ptr);
     wsfree_list_destroy(dtype->freeq);

     // free dtype members (register_tuple_member_type)
     if (dtype->tmembers) {
          queue_exit(dtype->tmembers);
     }

     // free dtype instance
     if (dtype->instance) {
          wsdt_tuple_freeq_t * fq = dtype->instance;
          wsfree_list_element_destroy(fq->freeq_small);
          wsfree_list_element_destroy(fq->freeq_medium);
          wsfree_list_element_destroy(fq->freeq_large);
          free(dtype->instance);
     }
}

void wsdatatype_free_label_names(void * data, void * ignore) {
     wslabel_t * label = (wslabel_t *)data;

     if (label && label->name) {
          free(label->name);
     }
}

void wsdatatype_free_module_names(void * data, void * ignore) {
     ws_proc_module_t * module = (ws_proc_module_t *)data;

     if (module->name && module->strdup_set) {
          free(module->name);
     }
     if (module->pbkid) {
          free(module->pbkid);
     }
}

int wsregister_label_hash(void * v_type_table, wslabel_t * parent,
                          uint64_t hash) {
     dprint("wsregister_label_hash ");
     dprint("wsregister_label_hash %p %p %lu", v_type_table, parent, hash);
     if (!hash || !parent) {
          return 0;
     }
     wslabel_t * label_alias;
     mimo_datalists_t * mdl = (mimo_datalists_t *)v_type_table;
     label_alias = (wslabel_t*)listhash_find_attach_reference(mdl->label_table,
                                                              (const char *)&hash,
                                                              sizeof(uint64_t),
                                                              parent);
     if (!label_alias) {
          return 0;
     }
     return 1;

}

static inline wslabel_t * wsregister_label_internal(void * v_type_table,
                                                    const char * name){
     dprint("wsregister_label_internal");

     if (!name) {
          return NULL;
     }
     mimo_datalists_t * mdl = (mimo_datalists_t *)v_type_table;
     int namelen = strlen(name);
     wslabel_t * label;
     //look up label..
     label = (wslabel_t*)listhash_find_attach(mdl->label_table, name, namelen);
 
     dprint("wsregister_label_internal %p %p %s", v_type_table, label, name);
     if (!label->name) {
          label->name = strdup(name);
          label->hash = evahash64((uint8_t*)name, namelen, 0x22341133);
          wsregister_label_hash(v_type_table, label, label->hash);
     }

     return label;
}

static inline wslabel_t * wsregister_label_internal_len(void * v_type_table,
                                                        const char * name,
                                                        int len){
     dprint("wsregister_label_internal");

     if (!name) {
          return NULL;
     }
     mimo_datalists_t * mdl = (mimo_datalists_t *)v_type_table;
     wslabel_t * label;
     //look up label..
     label = (wslabel_t*)listhash_find_attach(mdl->label_table, name, len);

     dprint("wsregister_label_internal %p %p %.*s", v_type_table, label, len, name);
     if (!label->name) {
          label->name = calloc(1, len+1);
          if (label->name) {
               memcpy(label->name, name, len);
          }
          else {
               error_print("failed wsregister_label_internal_len calloc of label->name");
               return NULL;
          }
          label->hash = evahash64((uint8_t*)name, len, 0x22341133);
          wsregister_label_hash(v_type_table, label, label->hash);
     }

     return label;
}

static inline void wsset_label_index(void * v_type_table, wslabel_t * label) {
     mimo_datalists_t * mdl = (mimo_datalists_t *)v_type_table;
     if (mdl->index_len < MAX_WSD_SEARCH_TERMS) {
          mdl->index_len++;
          label->index_id = mdl->index_len;
          status_print("searching label %s %d", label->name, mdl->index_len);
     }
     else {
          error_print("search terms exceeded max.. ");
     }
}

wslabel_t * wsregister_label(void * v_type_table, const char * name) {
     dprint("wsregister_label");
     wslabel_t * label = wsregister_label_internal(v_type_table, name);
     if (label && !label->registered) {
          label->registered = 1;
          if (label->search) {
               wsset_label_index(v_type_table, label);
          }
     }
     return label;
}

wslabel_t * wsregister_label_len(void * v_type_table, const char * name, int len) {
     dprint("wsregister_label");
     wslabel_t * label = wsregister_label_internal_len(v_type_table, name, len);
     if (label && !label->registered) {
          label->registered = 1;
          if (label->search) {
               wsset_label_index(v_type_table, label);
          }
     }
     return label;
}

wslabel_t * wssearch_label(void * v_type_table, const char * name) {
     dprint("wssearch_label");
     wslabel_t * label = wsregister_label_internal(v_type_table, name);
     if (label && !label->search) {
          label->search = 1;
          label->registered = 1;
          wsset_label_index(v_type_table, label);
     }
     return label;
}

wslabel_t * wssearch_label_len(void * v_type_table, const char * name, int len) {
     dprint("wssearch_label");
     wslabel_t * label = wsregister_label_internal_len(v_type_table, name, len);
     if (label && !label->search) {
          label->search = 1;
          label->registered = 1;
          wsset_label_index(v_type_table, label);
     }
     return label;
}

wslabel_t * wslabel_find_byhash(void * v_type_table, uint64_t hash) {
     wslabel_t * label;
     mimo_datalists_t * mdl = (mimo_datalists_t *)v_type_table;
     label = (wslabel_t*)listhash_find(mdl->label_table,
                                       (const char *)&hash,
                                       sizeof(uint64_t));
     return label;
}

int wsregister_label_alias(void * v_type_table, wslabel_t * parent,
                           char * alias) {
     if (!alias || !parent) {
          return 0;
     }
     wslabel_t * label_alias;
     mimo_datalists_t * mdl = (mimo_datalists_t *)v_type_table;
     label_alias = (wslabel_t*)listhash_find_attach_reference(mdl->label_table,
                                                              alias,
                                                              strlen(alias),
                                                              parent);
     if (!label_alias) {
          return 0;
     }
     return 1;

}

static int wsdatatype_default_to_string(wsdata_t * wsd, char ** s, int* l) {
     return 0;
}
static int wsdatatype_default_to_uint64(wsdata_t * wsd, uint64_t* u) {
     return 0;
}
static int wsdatatype_default_to_uint32(wsdata_t * wsd, uint32_t* u) {
     return 0;
}
static int wsdatatype_default_to_int64(wsdata_t * wsd, int64_t* i) {
     return 0;
}
static int wsdatatype_default_to_int32(wsdata_t * wsd, int32_t* i) {
     return 0;
}
static int wsdatatype_default_to_double(wsdata_t * wsd, double* d) {
     return 0;
}


static void *wsdata_allocator(void *arg) {
     wsdatatype_t *dtype = (wsdatatype_t*) arg;
     wsdata_t *newdata;

     newdata = (wsdata_t*) calloc(1, sizeof(wsdata_t));
     if (!newdata) {
          error_print("failed wsdata_allocator calloc of newdata");
          return NULL;
     }
     if (dtype->len) {
          newdata->data = (void*) calloc(1, dtype->len);
          if (!newdata->data) {
               error_print("failed wsdata_allocator calloc of newdata->data");
               free(newdata);
               return NULL;
          }
     } else {
          newdata->data = NULL;
     }

     return newdata;
}

static void *wsdata_ptr_allocator(void *arg) {
     wsdata_t *newdata;

     newdata = (wsdata_t*) calloc(1, sizeof(wsdata_t));
     if (!newdata) {
          error_print("failed wsdata_ptr_allocator calloc of newdata");
          return NULL;
     }

     return newdata;
}


//create data structure to store data types.. based on stringname
//register a data type in a global type registry.
//  allowed for a centralized control of types
//   a new data type could be loaded from a module/shared object
wsdatatype_t * wsdatatype_register(void * v_type_table,
                                   const char * name, int len,
                                   wsdatatype_hash hf,
                                   wsdatatype_init initf,
                                   wsdatatype_delete df,
                                   wsdatatype_print pf,
                                   wsdatatype_snprint snpf,
                                   wsdatatype_copy copyf,
                                   wsdatatype_serialize serialf) {
     mimo_datalists_t * mdl = (mimo_datalists_t *)v_type_table;

     wsdatatype_t * dtype;
     int namelen = strlen(name);

     //check if data type is already registered..     
     dtype = (wsdatatype_t*)listhash_find(mdl->dtype_table, name, namelen);
     if (dtype) {
          fprintf(stderr,"data type %s already registered, ignoring new registry", name);
          return dtype;
     }
     dtype = (wsdatatype_t*)listhash_find_attach(mdl->dtype_table, name, namelen);

     dtype->len = len;
     dtype->name = strdup(name);
     dtype->namehash = evahash64((uint8_t*)name, namelen, WSDATA_HASH_SEED);

     /*listhash_find_attach_reference(mdl->dtype_table,
                                    (const char *)&dtype->namehash,
                                    sizeof(uint64_t),
                                    dtype);
                                    */
     dtype->hash_func = hf;
     dtype->init_func = initf;
     dtype->delete_func = df;
     dtype->print_func = pf;
     dtype->snprint_func = snpf;
     dtype->copy_func = copyf;
     dtype->serialize_func = serialf;

     WS_MUTEX_INIT(&dtype->mutex, mutex_attr);

     dtype->freeq_ptr = wsfree_list_init(0, wsdata_ptr_allocator, dtype);
     dtype->freeq = wsfree_list_init(0, wsdata_allocator, dtype);

     dtype->to_string = wsdatatype_default_to_string;
     dtype->to_uint64 = wsdatatype_default_to_uint64;
     dtype->to_uint32 = wsdatatype_default_to_uint32;
     dtype->to_int64 = wsdatatype_default_to_int64;
     dtype->to_int32 = wsdatatype_default_to_int32;
     dtype->to_double = wsdatatype_default_to_double;

     dprint("registered datatype %s\n", name);
     return dtype;
}

wsdatatype_t * wsdatatype_register_generic(void * type_table, const char * name, int len) {
     return wsdatatype_register(type_table,
                                name, len,
                                wsdatatype_default_hash,
                                wsdatatype_default_init,
                                wsdatatype_default_delete,
                                wsdatatype_default_print,
                                wsdatatype_default_snprint,
                                wsdatatype_default_copy,
                                wsdatatype_default_serialize);
}

wsdatatype_t * wsdatatype_register_generic_ts(void * type_table,
                                              const char * name, int len) {
     return wsdatatype_register(type_table,
                                name, len,
                                wsdatatype_default_hash_ts,
                                wsdatatype_default_init,
                                wsdatatype_default_delete,
                                wsdatatype_default_print,
                                wsdatatype_default_snprint,
                                wsdatatype_default_copy,
                                wsdatatype_default_serialize);
}

wsdatatype_t * wsdatatype_register_generic_pf(void * type_table,
                                              const char * name, int len,
                                              wsdatatype_print pf,
                                              wsdatatype_snprint snpf) {
     return wsdatatype_register(type_table,
                                name, len,
                                wsdatatype_default_hash,
                                wsdatatype_default_init,
                                wsdatatype_default_delete,
                                pf, snpf,
                                wsdatatype_default_copy,
                                wsdatatype_default_serialize);
}

wsdatatype_t * wsdatatype_register_generic_pf_ts(void * type_table,
                                                 const char * name,
                                                 int len,
                                                 wsdatatype_print pf,
                                                 wsdatatype_snprint snpf) {

     return wsdatatype_register(type_table,
                                name, len,
                                wsdatatype_default_hash_ts,
                                wsdatatype_default_init,
                                wsdatatype_default_delete,
                                pf, snpf,
                                wsdatatype_default_copy,
                                wsdatatype_default_serialize);
}

//return 0 on fail
int wsdatatype_register_alias(void * v_type_table, wsdatatype_t * wsdtype,  const char * name) {
     mimo_datalists_t * mdl = (mimo_datalists_t *)v_type_table;
     wsdatatype_t * dtype;

     if (!wsdtype || !name) {
          return 0;
     }
     int namelen = strlen(name);
     
     dtype = (wsdatatype_t*)listhash_find_attach_reference(mdl->dtype_table,
                                                           name,
                                                           namelen,
                                                           wsdtype);
     if (!dtype || (dtype != wsdtype)) {
          return 0;  // alias already found..
     }

     return 1;
}

//check registry of types to see if type is registered yet
wsdatatype_t * wsdatatype_get(void * v_type_table, const char * name) {
     mimo_datalists_t * mdl = (mimo_datalists_t *)v_type_table;
     wsdatatype_t * dtype;

     //check if data type is already registered..     
     dtype = (wsdatatype_t *)listhash_find(mdl->dtype_table,
                                           name, strlen(name));
     return dtype;
}

int wsdatatype_match(void * v_type_table, wsdatatype_t * wsdtype, const char * name) {
     // until we get a fancier equivilence test.. check lookup
     wsdatatype_t * lookup = wsdatatype_get(v_type_table, name);
     // check if pointer match
     if (lookup == wsdtype) {
          return 1;
     }
     else {
          return 0;
     }
}
int wslabel_match(void * v_type_table, wslabel_t * label, const char * name) {
     // until we get a fancier equivilence test.. check lookup
     wslabel_t * lookup = wsregister_label(v_type_table, name);
     // check if pointer match
     if (lookup == label) {
          return 1;
     }
     else {
          return 0;
     }
}



//default hash function for generic non-pointer structure data
ws_hashloc_t* wsdatatype_default_hash(wsdata_t * wsdata) {
     if (!wsdata->has_hashloc) {
          wsdata->hashloc.offset = wsdata->data;
          wsdata->hashloc.len = wsdata->dtype->len;
          wsdata->has_hashloc = 1;
     }
     return &wsdata->hashloc;
}

//default delete function for generic non-pointer structure data
// does not actually delete, moves to free q
void wsdatatype_default_delete(wsdata_t * wsdata) {
     int tmp = wsdata_remove_reference(wsdata);

     // no more references.. we can move this data to free q
     if (tmp <= 0) {
          if (wsdata->dependency) {
               wsdata_t * parent;
               //remove references to parent
               while ((parent = wsstack_remove(wsdata->dependency)) != NULL) {
                    parent->dtype->delete_func(parent);
               }
          }
          //add to child free_q
          wsdata_moveto_freeq(wsdata);
     }
}

int wsdatatype_default_print(FILE * fp, wsdata_t *data, uint32_t type) {
     return fwrite(data->data, 1, data->dtype->len, fp);
}
int wsdatatype_default_snprint(char * buf, int buflen,
                               wsdata_t *data, uint32_t type) {
     memcpy((void * __restrict__)buf, (void * __restrict__)data->data, data->dtype->len);
     return data->dtype->len;
}

void wsdatatype_default_init(wsdata_t * wsdata, wsdatatype_t * dtype) {
     if (dtype->len) {
          memset(wsdata->data, 0, dtype->len); 
     }
     else {
          wsdata->data = NULL;
     }
}

wsdata_t * wsdatatype_default_copy(wsdata_t * wsdata){
     wsdata_t * newdata;
     newdata = wsdata_alloc(wsdata->dtype);

     WS_SPINLOCK_LOCK(&wsdata->lock);

     if (wsdata->data) {
          memcpy((void * __restrict__)newdata->data, (void * __restrict__)wsdata->data, wsdata->dtype->len);

          wsdata_duplicate_labels(wsdata,newdata);

          wsstack_node_t * walker = NULL;
          if (wsdata->dependency) {
               walker = wsdata->dependency->head;
               while(walker) {
                    wsdata_t * dep = (wsdata_t *) walker->data;
                    // TODO: Should do recursive call to wsdatatype_default_copy for
                    // most cases.  Some types need special handling, such as
                    // string.  That would leave the new string buffer pointing to the
                    // old string's buffer. For now, changes to the old string
                    // buffer will change the new string buffer.
                    // In other words, a call like:
                    // wsdata_t * newdep = wsdatatype_default_copy(dep), then
                    // followed by: wsdata_assign_dependency(newdep, newdata).
                    wsdata_assign_dependency(dep,newdata);
                    walker = walker->next;
               }
          }
          WS_SPINLOCK_UNLOCK(&wsdata->lock);

          return newdata;
     } else {
          wsdata_delete (newdata);
          WS_SPINLOCK_UNLOCK(&wsdata->lock);
          return NULL;
     }
}

uint8_t * wsdatatype_default_serialize(wsdata_t * wsdata, int * len){
     *len = wsdata->dtype->len;
     return (uint8_t*)wsdata->data;
}

//assume ts is the first n bytes..
ws_hashloc_t* wsdatatype_default_hash_ts(wsdata_t * wsdata) {
     if (!wsdata->has_hashloc) {
          int ts_offset = sizeof(wsdt_ts_t);
          wsdata->hashloc.offset = wsdata->data + ts_offset;
          wsdata->hashloc.len = wsdata->dtype->len - ts_offset;
          wsdata->has_hashloc = 1;
     }
     return &wsdata->hashloc;
}

static int wslabel_nested_recurse(void * type_table,
                                  wslabel_nested_set_t * nest,
                                  const char * search, int pid) {
     char * sub;
     sub = (char *)strchr(search,'.');
     if (sub) {
          dprint("found sub");
          int plen = sub - search;
          sub++;
          wslabel_t * parent = wssearch_label_len(type_table, search,
                                                  plen);
          dprint("found parent %s", parent->name);
          //check if already in lset
          int id = 0;
          int i;
          for (i = 0; i < nest->lset[pid].len; i++) {
               if ((parent == nest->lset[pid].labels[i]) && nest->lset[pid].id[i]) {
                    id = nest->lset[pid].id[i];
               }
          }
          if (!id) {
               if (nest->lset[pid].len >= WSMAX_LABEL_SET) {
                    return 0;
               }
               nest->subsets++;
               if (nest->subsets >= WSLABEL_NEST_SUBSET) {
                    return 0;
               }
               id = nest->subsets;
               nest->lset[pid].id[nest->lset[pid].len] = id;
               nest->lset[pid].labels[nest->lset[pid].len] = parent;
               nest->lset[pid].len++;
          }
          return wslabel_nested_recurse(type_table, nest, sub, id);
     }
     else {
          wslabel_set_add(type_table, &nest->lset[pid], search);
          nest->cnt++;
     }
     return 1;
}

int wslabel_nested_search_build(void * type_table, wslabel_nested_set_t * nest,
                                const char * search) {
     return wslabel_nested_recurse(type_table, nest, search, 0);
}

static int wslabel_nested_recurse_ext(void * type_table,
                                      wslabel_nested_set_ext_t * nest,
                                      const char * search, int pid, int uid) {
     char * sub;
     sub = (char *)strchr(search,'.');
     if (sub) {
          dprint("found sub");
          int plen = sub - search;
          sub++;
          wslabel_t * parent = wssearch_label_len(type_table, search,
                                                  plen);
          dprint("found parent %s", parent->name);
          //check if already in lset
          int id = 0;
          int i;
          for (i = 0; i < nest->lset[pid].len; i++) {
               if ((parent == nest->lset[pid].labels[i]) && nest->lset[pid].nid[i]) {
                    id = nest->lset[pid].nid[i];
               }
          }
          if (!id) {
               if (nest->lset[pid].len >= WSMAX_LABEL_SET) {
                    return 0;
               }
               nest->subsets++;
               if (nest->subsets >= WSLABEL_NEST_SUBSET) {
                    return 0;
               }
               id = nest->subsets;
               nest->lset[pid].nid[nest->lset[pid].len] = id;
               nest->lset[pid].labels[nest->lset[pid].len] = parent;
               nest->lset[pid].len++;
          }
          return wslabel_nested_recurse_ext(type_table, nest, sub, id, uid);
     }
     else {
          if (nest->lset[pid].len >= WSMAX_LABEL_SET) {
               return 0;
          }
          nest->lset[pid].labels[nest->lset[pid].len] = wssearch_label(type_table, search);
          nest->lset[pid].uid[nest->lset[pid].len] = uid;
          nest->lset[pid].len++;
          nest->cnt++;
     }
     return 1;
}

int wslabel_nested_search_build_ext(void * type_table, wslabel_nested_set_ext_t * nest,
                                    const char * search, int uid) {
     return wslabel_nested_recurse_ext(type_table, nest, search, 0, uid);
}

