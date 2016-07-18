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
#include "wsdt_labelset.h"
#include "datatypeloader.h"
#include "waterslidedata.h"


static int wsdt_print_labelset_wsdata(FILE * stream, wsdata_t * wsdata,
                                   uint32_t printtype) {
     wsdt_labelset_t * ls = (wsdt_labelset_t*)wsdata->data;
     int rtn = 0;
     int i;
     switch (printtype) {
     case WS_PRINTTYPE_HTML:
     case WS_PRINTTYPE_TEXT:
          for (i = 0; i < ls->len; i++) { 
               if (i > 0) {
                    rtn +=fprintf(stream, ":");
               }
               rtn +=fprintf(stream, "%s", ls->labels[i]->name);
          }
          return rtn;
          break;
     default:
          return 0;
     }
}

//fast init function.. just zero out the length, not the string
void wsdt_init_labelset(wsdata_t * wsdata, wsdatatype_t * dtype) {
     if (wsdata->data) {
          wsdt_labelset_t * ls = (wsdt_labelset_t*)wsdata->data;
          ls->len = 0;
     }
}

ws_hashloc_t* wsdt_hash_labelset(wsdata_t * wsdata) {
     wsdt_labelset_t * ls = (wsdt_labelset_t*)wsdata->data;
     if (!wsdata->has_hashloc) {
          wsdata->hashloc.len = sizeof(uint64_t);
          wsdata->has_hashloc = 1;
     }
     int i;
     ls->hash = 0;
     for (i = 0; i < ls->len; i++) { 
          ls->hash += ls->labels[i]->hash;
     }
     wsdata->hashloc.offset = &ls->hash; 
     return &wsdata->hashloc;
}

int datatypeloader_init(void * type_list) {
     wsdatatype_t *fsdt = wsdatatype_register_generic(type_list,
                                                      WSDT_LABELSET_STR,
                                                      sizeof(wsdt_labelset_t));
     fsdt->print_func = wsdt_print_labelset_wsdata;
     fsdt->init_func = wsdt_init_labelset;
     fsdt->hash_func = wsdt_hash_labelset;
     return 1;
}
