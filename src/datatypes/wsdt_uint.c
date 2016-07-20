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
#include "wsdt_uint.h"
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "wstypes.h"

static int wsdt_uint_sscan(wsdata_t * wsdata, char * buf, int len) {
     uint32_t *puint = (uint32_t*)wsdata->data;
     if (sscanf(buf,"%u",
                puint) == 1) {
          return 1;
     }
     return 0;
}

static int wsdt_print_uint(FILE * stream, wsdata_t * wsdata,
                           uint32_t printtype) {
      wsdt_uint_t * muint = (wsdt_uint_t *)wsdata->data;
      switch (printtype) {
         case WS_PRINTTYPE_HTML:   
         case WS_PRINTTYPE_TEXT:   
              return fprintf(stream, "%u", *muint); break;
         case WS_PRINTTYPE_BINARY:
              return fwrite(muint, sizeof(wsdt_uint_t), 1, stream); break;
        default: return 0;
      }
}

static int wsdt_to_string_uint(wsdata_t *wsdata, char **buf, int*len) {
     wsdt_uint_t * u = (wsdt_uint_t*)wsdata->data;

     char * lbuf = 0;
     int llen = 0;

     wsdata_t * wsdu = wsdata_create_buffer(32, &lbuf, &llen);

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

static int wsdt_to_uint64_uint(wsdata_t * wsdata, uint64_t* u64) {
     wsdt_uint_t * val = (wsdt_uint_t *)wsdata->data;
     *u64 = (uint64_t)*val;
     return 1;
}

static int wsdt_to_uint32_uint(wsdata_t * wsdata, uint32_t* u32) {
     wsdt_uint_t * val = (wsdt_uint_t *)wsdata->data;

     *u32 = (uint32_t)*val;
     return 1;
}

static int wsdt_to_int64_uint(wsdata_t * wsdata, int64_t* i64) {
     wsdt_uint_t * val = (wsdt_uint_t *)wsdata->data;
     *i64 = (int64_t)*val;
     return 1;
}

static int wsdt_to_int32_uint(wsdata_t * wsdata, int32_t* i32) {
     wsdt_uint_t * val = (wsdt_uint_t *)wsdata->data;
     if(*val <= 0x7FFFFFFF) {
          *i32 = (int32_t)*val;
          return 1;
     }
     //XXX: may want to convert to the negative value?
     return 0;
}

static int wsdt_to_double_uint(wsdata_t * wsdata, double * dbl) {
     wsdt_uint_t * val = (wsdt_uint_t *)wsdata->data;

     *dbl = (double)*val;
     return 1;
}

static wsdata_t * wsdt_uint_subelement_string(wsdata_t *ndata,
                                             wsdata_t * dst, void * aux) {
     wsdt_uint_t * val = (wsdt_uint_t *)ndata->data;

     char * lbuf = 0;
     int llen = 0;
     wsdata_t *wsdu = wsdata_create_buffer(64, &lbuf, &llen);
     if(!wsdu) {
          return NULL;
     }
     wsdata_assign_dependency(wsdu, dst);
     wsdt_string_t * str = (wsdt_string_t *)dst->data;
     str->buf = lbuf;
     str->len = sprintf(str->buf, "%u", *val);

     return dst;
}

static wsdata_t * wsdt_uint_subelement_ts(wsdata_t *ndata,
                                         wsdata_t * dst, void * aux) {
     wsdt_uint_t * val = (wsdt_uint_t *)ndata->data;
     wsdt_ts_t * ts = (wsdt_ts_t *)dst->data;

     // granularity of integer time is the same as that of time_t (in seconds only)
     ts->sec = *val;
     ts->usec = 0;

     return dst;
}

static wsdata_t * wsdt_uint_subelement_double(wsdata_t *ndata,
                                             wsdata_t * dst, void * aux) {
     wsdt_uint_t * val = (wsdt_uint_t *)ndata->data;
     wsdt_double_t * dbl = (wsdt_double_t *)dst->data;
     *dbl = (double)(*val);

     return dst;
}


int datatypeloader_init(void * type_list) {
     wsdatatype_t * uint_dt = wsdatatype_register_generic(type_list,
                                                          WSDT_UINT_STR,
                                                          sizeof(wsdt_uint_t));
     
     uint_dt->print_func = wsdt_print_uint; 
     uint_dt->sscan_func = wsdt_uint_sscan;
     uint_dt->to_string = wsdt_to_string_uint;
     uint_dt->to_uint64 = wsdt_to_uint64_uint;
     uint_dt->to_uint32 = wsdt_to_uint32_uint;
     uint_dt->to_int64 = wsdt_to_int64_uint;
     uint_dt->to_int32 = wsdt_to_int32_uint;
     uint_dt->to_double = wsdt_to_double_uint;

     wsdatatype_register_subelement_func(uint_dt, type_list,
                                         "STRING", "STRING_TYPE",
                                         wsdt_uint_subelement_string,
                                         NULL);
     wsdatatype_register_subelement_func(uint_dt, type_list,
                                         "DATETIME", "TS_TYPE",
                                         wsdt_uint_subelement_ts,
                                         NULL);
     wsdatatype_register_subelement_func(uint_dt, type_list,
                                         "DOUBLE", "DOUBLE_TYPE",
                                         wsdt_uint_subelement_double,
                                         NULL);

     wsdatatype_register_alias(type_list, uint_dt, WSDT_UINT32_STR);

     wsdatatype_register_generic_ts(type_list,
                                    WSDT_TS_STR WSDT_UINT_STR, 
                                    sizeof(wsdt_ts_t) + sizeof(wsdt_uint_t));
     return 1;
}
