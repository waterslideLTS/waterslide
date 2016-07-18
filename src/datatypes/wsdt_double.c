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
#include <math.h>
#include "wsdt_double.h"
#include "wsdt_uint64.h"
#include "wsdt_int64.h"
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "wstypes.h"

static int wsdt_double_sscan(wsdata_t * wsdata, char * buf, int len) {
     double *pdbl = (double*)wsdata->data;
     *pdbl = atof(buf);
     return 1;
}

static int wsdt_print_double(FILE * stream, wsdata_t * wsdata,
                           uint32_t printtype) {
      wsdt_double_t * mdouble = (wsdt_double_t *)wsdata->data;
      switch (printtype) {
         case WS_PRINTTYPE_HTML:
         case WS_PRINTTYPE_TEXT:
              return fprintf(stream, "%f", *mdouble); break;
         case WS_PRINTTYPE_BINARY:
              return fwrite(mdouble, sizeof(wsdt_double_t), 1, stream); break;
        default: return 0;
      }
}

static wsdata_t * wsdt_double_subelement_round(wsdata_t *ndata,
                                               wsdata_t * dst, void * aux) {
     wsdt_double_t * dbl = (wsdt_double_t *)ndata->data;
     wsdt_int64_t * i64 = (wsdt_int64_t*)dst->data;
     if(*dbl > (double)0x7FFFFFFFFFFFFFFF || *dbl < -(double)0x7FFFFFFFFFFFFFFF) {
          fprintf(stderr, "truncation warning in %s, line %d\n", __FILE__, __LINE__);
     }

     *i64 = round(*dbl);
     return dst;
}

static wsdata_t * wsdt_double_subelement_trunc(wsdata_t *ndata,
                                               wsdata_t * dst, void * aux) {
     wsdt_double_t * dbl = (wsdt_double_t *)ndata->data;
     wsdt_int64_t * i64 = (wsdt_int64_t*)dst->data;
     if(*dbl > (double)0x7FFFFFFFFFFFFFFF || *dbl < -(double)0x7FFFFFFFFFFFFFFF) {
          fprintf(stderr, "truncation warning in %s, line %d\n", __FILE__, __LINE__);
     }

     *i64 = trunc(*dbl);
     return dst;
}
static wsdata_t * wsdt_double_subelement_floor(wsdata_t *ndata,
                                               wsdata_t * dst, void * aux) {
     wsdt_double_t * dbl = (wsdt_double_t *)ndata->data;
     wsdt_int64_t * i64 = (wsdt_int64_t*)dst->data;
     if(*dbl > (double)0x7FFFFFFFFFFFFFFF || *dbl < -(double)0x7FFFFFFFFFFFFFFF) {
          fprintf(stderr, "truncation warning in %s, line %d\n", __FILE__, __LINE__);
     }

     *i64 = floor(*dbl);
     return dst;
}


static wsdata_t * wsdt_double_subelement_ceil(wsdata_t *ndata,
                                               wsdata_t * dst, void * aux) {
     wsdt_double_t * dbl = (wsdt_double_t *)ndata->data;
     wsdt_int64_t * i64 = (wsdt_int64_t*)dst->data;
     if(*dbl > (double)0x7FFFFFFFFFFFFFFF || *dbl < -(double)0x7FFFFFFFFFFFFFFF) {
          fprintf(stderr, "truncation warning in %s, line %d\n", __FILE__, __LINE__);
     }

     *i64 = ceil(*dbl);
     return dst;
}

static wsdata_t * wsdt_double_subelement_ts(wsdata_t *ndata,
                                            wsdata_t * dst, void * aux) {
     wsdt_double_t * val = (wsdt_double_t *)ndata->data;
     wsdt_ts_t * ts = (wsdt_ts_t *)dst->data;

     ts->sec  = floor(*val);
     ts->usec = ceil((*val - floor(*val))*1e6);

     return dst;
}

static wsdata_t * wsdt_double_subelement_string(wsdata_t *ndata,
                                            wsdata_t * dst, void * aux) {
     wsdt_double_t * val = (wsdt_double_t *)ndata->data;

     char * lbuf = 0;
     int llen = 0;
     wsdata_t *wsdu = wsdata_create_buffer(64, &lbuf, &llen);
     if(!wsdu) {
          return NULL;
     }
     wsdata_assign_dependency(wsdu, dst);
     wsdt_string_t * str = (wsdt_string_t *)dst->data;
     str->buf = lbuf;
     str->len = sprintf(str->buf, "%f", *val);

     return dst;
}

static int wsdt_to_uint64_double(wsdata_t * wsdata, uint64_t* u64) {
     wsdt_double_t * val = (wsdt_double_t *)wsdata->data;
     if (*val >= 0) {
          *u64 = (uint64_t)*val;
          return 1;
     }
     else {
          return 0;
     }
}

static int wsdt_to_uint32_double(wsdata_t * wsdata, uint32_t* u32) {
     wsdt_double_t * val = (wsdt_double_t *)wsdata->data;

     if ((*val >= 0) && (*val <= (double)0xFFFFFFFF)) {
          *u32 = (uint32_t)*val;
          return 1;
     }
     return 0;
}

static int wsdt_to_int64_double(wsdata_t * wsdata, int64_t* i64) {
     wsdt_double_t * val = (wsdt_double_t *)wsdata->data;
     *i64 = (int64_t)*val;
     return 1;
}

static int wsdt_to_int32_double(wsdata_t * wsdata, int32_t* i32) {
     wsdt_double_t * val = (wsdt_double_t *)wsdata->data;

     if ((*val >= -(double)0x7FFFFFFF) && (*val <= (double)0x7FFFFFFF)) {
          *i32 = (int32_t)*val;
          return 1;
     }
     return 0;
}

static int wsdt_to_double_double(wsdata_t * wsdata, double* dbl) {
     wsdt_double_t * val = (wsdt_double_t *)wsdata->data;
     *dbl = *val;
     return 1;
}

static int wsdt_to_string_double(wsdata_t *wsdata, char **buf, int*len) {
     wsdt_double_t * u = (wsdt_double_t*)wsdata->data;

     char * lbuf = 0;
     int llen = 0;

     wsdata_t * wsdu = wsdata_create_buffer(54, &lbuf, &llen);

     if (!wsdu) {
          return 0;
     }

     llen = snprintf(lbuf, llen, "%f", *u);

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


int datatypeloader_init(void * type_list) {
     wsdatatype_t * double_dt = wsdatatype_register_generic(type_list,
                                                          WSDT_DOUBLE_STR,
                                                          sizeof(wsdt_double_t));
     
     double_dt->print_func = wsdt_print_double; 
     double_dt->sscan_func = wsdt_double_sscan; 
     double_dt->to_uint64 = wsdt_to_uint64_double;
     double_dt->to_uint32 = wsdt_to_uint32_double;
     double_dt->to_int64 = wsdt_to_int64_double;
     double_dt->to_int32 = wsdt_to_int32_double;
     double_dt->to_double = wsdt_to_double_double;
     double_dt->to_string = wsdt_to_string_double;

     wsdatatype_register_subelement_func(double_dt, type_list,
                                         "ROUND", "INT64_TYPE",
                                         wsdt_double_subelement_round,
                                         NULL); 
     wsdatatype_register_subelement_func(double_dt, type_list,
                                         "TRUNC", "INT64_TYPE",
                                         wsdt_double_subelement_trunc,
                                         NULL); 
     wsdatatype_register_subelement_func(double_dt, type_list,
                                         "FLOOR", "INT64_TYPE",
                                         wsdt_double_subelement_floor,
                                         NULL); 
     wsdatatype_register_subelement_func(double_dt, type_list,
                                         "CEIL", "INT64_TYPE",
                                         wsdt_double_subelement_ceil,
                                         NULL); 
     wsdatatype_register_subelement_func(double_dt, type_list,
                                         "DATETIME", "TS_TYPE",
                                         wsdt_double_subelement_ts,
                                         NULL); 
     wsdatatype_register_subelement_func(double_dt, type_list,
                                         "STRING", "STRING_TYPE",
                                         wsdt_double_subelement_string,
                                         NULL); 

     return 1;
}
