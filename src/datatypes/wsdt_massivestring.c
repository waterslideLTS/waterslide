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
#include <stdint.h>
#include <stddef.h>
#include <ctype.h>
//#define DEBUG 1
#include "waterslidedata.h"
#include "wstypes.h"
#include "wsdt_massivestring.h"

void wsdt_custom_massivestring_delete(wsdata_t * wsdata) {
     int tmp = wsdata_get_reference(wsdata);

     if (tmp <= 1) {
          // NOTE: in the call to wsdatatype_default_delete below, tmp will 
          //       eventually be <= 0, resulting in deleting the wsdata, so 
          //       we free up dynamically allocated buffer here when tmp <= 1
          wsdt_massivestring_t * str = (wsdt_massivestring_t*)wsdata->data;
          if (str->buf) {
               free(str->buf);
               str->buf = NULL;
          }
     }
     wsdatatype_default_delete(wsdata);
}

ws_hashloc_t* wsdt_massivestring_hash(wsdata_t * wsdata) {
     if (!wsdata->has_hashloc) {
          wsdt_massivestring_t *strd = (wsdt_massivestring_t*)wsdata->data;
          wsdata->hashloc.offset = strd->buf; 
          wsdata->hashloc.len = strd->len;
          wsdata->has_hashloc = 1;
     }
     return &wsdata->hashloc;
}

wsdata_t * wsdt_massivestring_alloc(int len, char **pbuf) {
     wsdata_t * dep = wsdata_alloc(dtype_massivestring);
     if (dep) {
          char * buf = (char *)malloc(len);
          if (buf) {
               wsdt_massivestring_t * str = (wsdt_massivestring_t *)dep->data;
               str->buf = buf;
               str->len = len;
               *pbuf = buf;
               return dep;
          }
          wsdata_delete(dep);
     }
     return NULL;
}


int datatypeloader_init(void * tl) {
     wsdatatype_t * dt = wsdatatype_register(tl,
                                             WSDT_MASSIVESTRING_STR,
                                             sizeof(wsdt_massivestring_t),
                                             wsdt_massivestring_hash,
                                             wsdatatype_default_init,
                                             wsdt_custom_massivestring_delete,
                                             NULL,
                                             NULL,
                                             wsdatatype_default_copy,
                                             wsdatatype_default_serialize);
     return (dt != NULL);
}


