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
#include "wsdt_int.h"
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "wstypes.h"

static int wsdt_int_sscan(wsdata_t * wsdata, char * buf, int len) {
     int *pint = (int*)wsdata->data;
     *pint = atoi(buf);
     return 1;
}

static int wsdt_print_int(FILE * stream, wsdata_t * wsdata,
                          uint32_t printtype) {
     wsdt_int_t * mint = (wsdt_int_t *)wsdata->data;
     switch (printtype) {
     case WS_PRINTTYPE_HTML:
     case WS_PRINTTYPE_TEXT:
          return fprintf(stream, "%d", *mint); break;
     case WS_PRINTTYPE_BINARY: return fwrite(mint, sizeof(wsdt_int_t), 1, stream); break;
     default: return 0;
     }
}

static int wsdt_to_string_int(wsdata_t *wsdata, char **buf, int*len) {
     wsdt_int_t * u = (wsdt_int_t*)wsdata->data;

     char * lbuf = 0;
     int llen = 0;

     wsdata_t * wsdu = wsdata_create_buffer(64, &lbuf, &llen);

     if (!wsdu) {
          return 0;
     }

     llen = snprintf(lbuf, llen, "%d", *u);

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

static int wsdt_to_uint64_int(wsdata_t * wsdata, uint64_t* u64) {
     wsdt_int_t * val = (wsdt_int_t *)wsdata->data;
     if (*val >= 0) {
          *u64 = (uint64_t)*val;
          return 1;
     }
     //XXX: we could really just make this invoke the wsdt_to_int64_int function,
     //     but we decide not to as the calling process will know that a 
     //     failure in conversion has occurred from the return value, i.e., leave
     //     the conversion to the calling process if so desired
     return 0;
}

static int wsdt_to_uint32_int(wsdata_t * wsdata, uint32_t* u32) {
     wsdt_int_t * val = (wsdt_int_t *)wsdata->data;
     if (*val >= 0) {
          *u32 = (uint32_t)*val;
          return 1;
     }
     return 0;
}

static int wsdt_to_int64_int(wsdata_t * wsdata, int64_t* i64) {
     wsdt_int_t * val = (wsdt_int_t *)wsdata->data;
     *i64 = (int64_t)*val;
     return 1;
}

static int wsdt_to_int32_int(wsdata_t * wsdata, int32_t* i32) {
     wsdt_int_t * val = (wsdt_int_t *)wsdata->data;
     *i32 = (int32_t)*val;
     return 1;
}

static int wsdt_to_double_int(wsdata_t * wsdata, double* dbl) {
     wsdt_int_t * val = (wsdt_int_t *)wsdata->data;
     *dbl = (double)*val;
     return 1;
}

static wsdata_t * wsdt_int_subelement_string(wsdata_t *ndata,
                                             wsdata_t * dst, void * aux) {
     wsdt_int_t * val = (wsdt_int_t *)ndata->data;

     char * lbuf = 0;
     int llen = 0;
     wsdata_t *wsdu = wsdata_create_buffer(64, &lbuf, &llen);
     if(!wsdu) {
          return NULL;
     }
     wsdata_assign_dependency(wsdu, dst);
     wsdt_string_t * str = (wsdt_string_t *)dst->data;
     str->buf = lbuf;
     str->len = sprintf(str->buf, "%d", *val);

     return dst;
}

static wsdata_t * wsdt_int_subelement_ts(wsdata_t *ndata,
                                         wsdata_t * dst, void * aux) {
     wsdt_int_t * val = (wsdt_int_t *)ndata->data;
     wsdt_ts_t * ts = (wsdt_ts_t *)dst->data;

     // granularity of integer time is the same as that of time_t (in seconds only)
     ts->sec = *val;
     ts->usec = 0;

     return dst;
}

static wsdata_t * wsdt_int_subelement_double(wsdata_t *ndata,
                                             wsdata_t * dst, void * aux) {
     wsdt_int_t * val = (wsdt_int_t *)ndata->data;
     wsdt_double_t * dbl = (wsdt_double_t *)dst->data;
     *dbl = (double)(*val);

     return dst;
}


int datatypeloader_init(void * type_list) {
     wsdatatype_t * int_dt = wsdatatype_register_generic(type_list,
                                                         WSDT_INT_STR,
                                                         sizeof(wsdt_int_t));

     int_dt->print_func = wsdt_print_int; 
     int_dt->sscan_func = wsdt_int_sscan; 
     int_dt->to_string = wsdt_to_string_int; 
     int_dt->to_uint64 = wsdt_to_uint64_int; 
     int_dt->to_uint32 = wsdt_to_uint32_int; 
     int_dt->to_int64 = wsdt_to_int64_int; 
     int_dt->to_int32 = wsdt_to_int32_int; 
     int_dt->to_double = wsdt_to_double_int; 

     wsdatatype_register_subelement_func(int_dt, type_list,
                                         "STRING", "STRING_TYPE",
                                         wsdt_int_subelement_string,
                                         NULL);
     wsdatatype_register_subelement_func(int_dt, type_list,
                                         "DATETIME", "TS_TYPE",
                                         wsdt_int_subelement_ts,
                                         NULL);
     wsdatatype_register_subelement_func(int_dt, type_list,
                                         "DOUBLE", "DOUBLE_TYPE",
                                         wsdt_int_subelement_double,
                                         NULL);

     wsdatatype_register_generic_ts(type_list,
                                    WSDT_TS_STR WSDT_INT_STR, 
                                    sizeof(wsdt_ts_t) + sizeof(wsdt_int_t));
     return 1;
}
