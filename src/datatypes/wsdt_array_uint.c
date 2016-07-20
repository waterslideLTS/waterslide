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
#include "wsdt_array_uint.h"
#include "datatypeloader.h"
#include "waterslidedata.h"

static int wsdt_print_array_uint_wsdata(FILE * stream, wsdata_t * wsdata,
                                        uint32_t printtype) {
     wsdt_array_uint_t * au = (wsdt_array_uint_t*)wsdata->data;
     int rtn = 0;
     int i;
     switch (printtype) {
     case WS_PRINTTYPE_HTML:
     case WS_PRINTTYPE_TEXT:
          for (i = 0; i < au->len; i++) { 
               if (i > 0) {
                    rtn +=fprintf(stream, ":");
               }
               rtn +=fprintf(stream, "%u", au->value[i]);
          }
          return rtn;
          break;
     default:
          return 0;
     }
}

//fast init function.. just zero out the length, not the string
void wsdt_init_array_uint(wsdata_t * wsdata, wsdatatype_t * dtype) {
     if (wsdata->data) {
          wsdt_array_uint_t * ls = (wsdt_array_uint_t*)wsdata->data;
          ls->len = 0;
     }
}

int datatypeloader_init(void * type_list) {
     wsdatatype_t *fsdt = wsdatatype_register_generic(type_list,
                                                      WSDT_ARRAY_UINT_STR,
                                                      sizeof(wsdt_array_uint_t));
     fsdt->print_func = wsdt_print_array_uint_wsdata;
     fsdt->init_func = wsdt_init_array_uint;
     return 1;
}
