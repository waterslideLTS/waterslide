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
#include "wsdt_tuple.h"
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "mimo.h"

static int wsdt_print_tuple_wsdata(FILE * stream, wsdata_t * wsdata,
                                   uint32_t printtype) {
     wsdt_tuple_t * tuple = (wsdt_tuple_t*)wsdata->data;
     int rtn = 0;
     int i;
     wsdata_t * member;

     for (i = 0; i < tuple->len; i++) {
          member = tuple->member[i];
          if (member->dtype->print_func) {
               rtn += member->dtype->print_func(stream, member, printtype);
               if (printtype == WS_PRINTTYPE_TEXT) {
                    rtn += fprintf(stream, ";");
               }
          }
     }
     return rtn;
}

//fast init function.. just zero out the length, not the string
void wsdt_init_tuple(wsdata_t * wsdata, wsdatatype_t * dtype) {
     wsdt_tuple_freeq_t * fq = (wsdt_tuple_freeq_t*)dtype->instance;

     fq->index_len = *fq->p_ilen;

     wsdata->data = wsdt_tuple_alloc(fq, fq->freeq_small, WSDT_TUPLE_SMALL_LEN);
}

void wsdt_tuple_custom_delete(wsdata_t * wsdata) {
     int tmp = wsdata_remove_reference(wsdata);
 
     // no more references.. we can move this data to free q
     if (tmp <= 0) {
          wsdata_t **const member = ((wsdt_tuple_t*)wsdata->data)->member;
          const int tuple_len = (((wsdt_tuple_t*)wsdata->data))->len;
          if(1 != wsdata->isptr) { // only original creator of members is allowed to delete
               int i;
               for (i = 0; i < tuple_len; i++) {
                    member[i]->dtype->delete_func(member[i]);
               }
          }

          if (wsdata->dependency) {
               wsdata_t * parent;
               //remove references to parent
               while ((parent = wsstack_remove(wsdata->dependency)) != NULL) {
                    parent->dtype->delete_func(parent);
               }
          }

          wsdt_tuple_recover(wsdata);
          wsdata_moveto_freeq(wsdata);
     }
}

static void* tuple_allocator_small(void *foo) {
     wsdt_tuple_freeq_t *fq = (wsdt_tuple_freeq_t*) foo;
     wsdt_tuple_t * tmp = wsdt_tuple_internal_alloc(fq, WSDT_TUPLE_SMALL_LEN);
     if ( tmp ) tmp->freeq = fq->freeq_small;
     return tmp;
}

static void* tuple_allocator_medium(void *foo) {
     wsdt_tuple_freeq_t *fq = (wsdt_tuple_freeq_t*) foo;
     wsdt_tuple_t * tmp = wsdt_tuple_internal_alloc(fq, WSDT_TUPLE_MEDIUM_LEN);
     if ( tmp ) tmp->freeq = fq->freeq_medium;
     return tmp;
}

static void* tuple_allocator_large(void *foo) {
     wsdt_tuple_freeq_t *fq = (wsdt_tuple_freeq_t*) foo;
     wsdt_tuple_t * tmp = wsdt_tuple_internal_alloc(fq, WSDT_TUPLE_LARGE_LEN);
     if ( tmp ) tmp->freeq = fq->freeq_large;
     return tmp;
}

int datatypeloader_init(void * type_list) {
     wsdatatype_t *fsdt = wsdatatype_register_generic(type_list,
                                                      WSDT_TUPLE_STR,
                                                      0);
     if (!fsdt) {
          return 0;
     }

     fsdt->print_func = wsdt_print_tuple_wsdata;
     fsdt->init_func = wsdt_init_tuple;
     fsdt->delete_func = wsdt_tuple_custom_delete;

     wsdt_tuple_freeq_t * fq =
          (wsdt_tuple_freeq_t*)calloc(1, sizeof(wsdt_tuple_freeq_t));

     if (!fq) {
          return 0;
     }
     fsdt->instance = fq;
                                     
     fq->freeq_small = wsfree_list_init(0, tuple_allocator_small, fq);
     fq->freeq_medium = wsfree_list_init(0, tuple_allocator_medium, fq);
     fq->freeq_large = wsfree_list_init(0, tuple_allocator_large, fq);
#ifndef USE_ATOMICS
     WS_SPINLOCK_INIT(&fq->lock);
#endif

     mimo_datalists_t * mdl = (mimo_datalists_t *)type_list;
     fq->p_ilen = &mdl->index_len; 

     return 1;
}
