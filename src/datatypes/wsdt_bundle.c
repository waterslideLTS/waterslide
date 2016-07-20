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
#include "wsdt_bundle.h"
#include "datatypeloader.h"
#include "waterslidedata.h"

//fast init function.. just zero out the length, not the string
void wsdt_init_bundle(wsdata_t * wsdata, wsdatatype_t * dtype) {
     if (wsdata->data) {
          wsdt_bundle_t * ls = (wsdt_bundle_t*)wsdata->data;
          ls->len = 0;
     }
}

//copied directly from wsdatatype_default_delete func
static void wsdt_delete_bundle(wsdata_t * wsdata) {
     int tmp = wsdata_remove_reference(wsdata);

     // no more references.. we can move this data to free q
     if (tmp <= 0) {

          wsdt_bundle_t * my_bundle = (wsdt_bundle_t *)wsdata->data;
          
          int i;
          for (i=0; i < my_bundle->len; i++) {
               wsdata_delete(my_bundle->wsd[i]);
          }

          if (wsdata->dependency) {
               wsdata_t * parent;
               //remove references to parent
               while ((parent = wsstack_remove(wsdata->dependency)) != NULL) {
                    parent->dtype->delete_func(parent);
               }
          }
          //add to child free_q
          wsdata_moveto_freeq(wsdata);
          //fprintf(stderr,"waterslidedata: data dereferenced\n");
     }
}


int datatypeloader_init(void * type_list) {
     wsdatatype_t *fsdt = wsdatatype_register_generic(type_list,
                                                      WSDT_BUNDLE_STR,
                                                      sizeof(wsdt_bundle_t));
     fsdt->init_func = wsdt_init_bundle;
     fsdt->delete_func = wsdt_delete_bundle;    
     return 1;
}
