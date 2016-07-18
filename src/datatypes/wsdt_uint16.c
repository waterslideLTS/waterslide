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
#include "wsdt_uint16.h"
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "wstypes.h"

static int wsdt_print_uint16(FILE * stream, wsdata_t * wsdata,
                           uint32_t printtype) {
      wsdt_uint16_t * muint16 = (wsdt_uint16_t *)wsdata->data;
      switch (printtype) {
         case WS_PRINTTYPE_HTML:
         case WS_PRINTTYPE_TEXT:
              return fprintf(stream, "%u", (*muint16)); break;
         case WS_PRINTTYPE_BINARY: return fwrite(muint16, sizeof(wsdt_uint16_t), 1, stream); break;
        default: return 0;
      }
}

static int wsdt_to_string_uint16(wsdata_t *wsdata, char **buf, int*len) {
     wsdt_uint16_t * u = (wsdt_uint16_t*)wsdata->data;

     char * lbuf = 0;
     int llen = 0;

     wsdata_t * wsdu = wsdata_create_buffer(10, &lbuf, &llen);

     if (!wsdu) {
          return 0;
     }

     llen = snprintf(lbuf, llen, "%u", *u);

     if (llen > 0) {
          wsdata_assign_dependency(wsdu, wsdata);
          *buf = lbuf;
          *len = llen;
     }
     else {
          wsdata_delete(wsdu);
          return 0;
     }
     return 1;
}


static int wsdt_to_uint64_uint16(wsdata_t * wsdata, uint64_t* u64) {
     wsdt_uint16_t * val = (wsdt_uint16_t *)wsdata->data;
     *u64 = (uint64_t)*val;
     return 1;
}

static int wsdt_to_uint32_uint16(wsdata_t * wsdata, uint32_t* u32) {
     wsdt_uint16_t * val = (wsdt_uint16_t *)wsdata->data;

     *u32 = (uint32_t)*val;
     return 1;
}

static int wsdt_to_int64_uint16(wsdata_t * wsdata, int64_t* i64) {
     wsdt_uint16_t * val = (wsdt_uint16_t *)wsdata->data;
     *i64 = (int64_t)*val;
     return 1;
}

static int wsdt_to_int32_uint16(wsdata_t * wsdata, int32_t* i32) {
     wsdt_uint16_t * val = (wsdt_uint16_t *)wsdata->data;
     *i32 = (int32_t)*val;
     return 1;
}

static int wsdt_to_double_uint16(wsdata_t * wsdata, double * dbl) {
     wsdt_uint16_t * val = (wsdt_uint16_t *)wsdata->data;

     *dbl = (double)*val;
     return 1;
}

int datatypeloader_init(void * type_list) {
     wsdatatype_t * uint16_dt = wsdatatype_register_generic(type_list,
                                                          WSDT_UINT16_STR,
                                                          sizeof(wsdt_uint16_t));
     
     uint16_dt->print_func = wsdt_print_uint16;    
     uint16_dt->to_string = wsdt_to_string_uint16;
     uint16_dt->to_uint64 = wsdt_to_uint64_uint16;
     uint16_dt->to_uint32 = wsdt_to_uint32_uint16;
     uint16_dt->to_int64 = wsdt_to_int64_uint16;
     uint16_dt->to_int32 = wsdt_to_int32_uint16;
     uint16_dt->to_double = wsdt_to_double_uint16;

     wsdatatype_register_generic_ts(type_list,
                                    WSDT_TS_STR WSDT_UINT16_STR, 
                                    sizeof(wsdt_ts_t) + sizeof(wsdt_uint16_t));
     return 1;
}
