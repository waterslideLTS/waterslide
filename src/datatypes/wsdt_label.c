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
#include <stdlib.h>
#include "wsdt_label.h"
#include "datatypeloader.h"
#include "waterslidedata.h"

static int wsdt_print_label(FILE * stream, wsdata_t * wsdata,
                           uint32_t printtype) {

     if (!wsdata || !wsdata->data) {
          return 0;
     }

     wsdt_label_t * mlabel = (wsdt_label_t *)wsdata->data;
     switch (printtype) {
     case WS_PRINTTYPE_HTML: 
     case WS_PRINTTYPE_TEXT: 
          return fprintf(stream, "%s", (*mlabel)->name); break;
     default:
          return 0;
     }
}

static ws_hashloc_t* wsdt_hash_label(wsdata_t * wsdata) {
     if (!wsdata->has_hashloc) {
          wsdt_label_t *mlabel = (wsdt_label_t*)wsdata->data;
          wsdata->hashloc.offset = (*mlabel)->name;
          wsdata->hashloc.len = strlen(wsdata->hashloc.offset);
          wsdata->has_hashloc = 1;
     }
     return &wsdata->hashloc;
}

static int wsdt_to_string_label(wsdata_t *wsdata, char **buf, int *len) {
     wsdt_label_t *label = (wsdt_label_t *) wsdata->data;

     *buf = (*label)->name;
     *len = strlen(*buf);
     return 1;
}

int datatypeloader_init(void * type_list) {
     wsdatatype_t * label_dt = wsdatatype_register_generic(type_list,
                                                          WSDT_LABEL_STR,
                                                          sizeof(wsdt_label_t));
     label_dt->to_string = wsdt_to_string_label; 
     label_dt->print_func = wsdt_print_label;    
     label_dt->hash_func = wsdt_hash_label;    
 
     return 1;
}
