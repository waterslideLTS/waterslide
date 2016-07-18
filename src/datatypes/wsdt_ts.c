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
#include <stddef.h>
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "wsdt_ts.h"
#include "wstypes.h"

static int wsdt_print_ts_wsdata(FILE * stream, wsdata_t * wsdata,
                                uint32_t printtype) {
     wsdt_ts_t *ts = (wsdt_ts_t*)wsdata->data;
     switch (printtype) {
     case WS_PRINTTYPE_HTML:
     case WS_PRINTTYPE_TEXT:
          return wsdt_print_ts(stream, ts);
          break;
     case WS_PRINTTYPE_BINARY:
          return fwrite(wsdata->data, sizeof(wsdt_ts_t), 1, stream);
          break;
     }
     return 0;
}

static int wsdt_to_string_ts(wsdata_t *wsdata, char **buf, int*len) {
     wsdt_ts_t * ts = (wsdt_ts_t*)wsdata->data;

     char * lbuf = 0;
     int llen = 0;

     wsdata_t * wsdu = wsdata_create_buffer(48, &lbuf, &llen);

     if (!wsdu) {
          return 0;
     }

     llen = wsdt_snprint_ts(lbuf, llen, ts);

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

static int wsdt_to_uint64_ts(wsdata_t * wsdata, uint64_t* u64) {
     wsdt_ts_t * ts = (wsdt_ts_t *)wsdata->data;
     *u64 = (uint64_t)ts->sec;
     return 1;
}

static int wsdt_to_uint32_ts(wsdata_t * wsdata, uint32_t* u32) {
     wsdt_ts_t * ts = (wsdt_ts_t *)wsdata->data;
     *u32 = (uint32_t)ts->sec;
     return 1;
}

static int wsdt_to_double_ts(wsdata_t * wsdata, double* dbl) {
     wsdt_ts_t * ts = (wsdt_ts_t *)wsdata->data;
     *dbl = (double)ts->sec + ((double)ts->usec / 1000000.0);
     return 1;
}

int datatypeloader_init(void * tl) {
     wsdatatype_t * ts_dt =
          wsdatatype_register_generic(tl, WSDT_TS_TYPE_STR, sizeof(wsdt_ts_t));

     ts_dt->print_func = wsdt_print_ts_wsdata;
     ts_dt->to_string = wsdt_to_string_ts;
     ts_dt->to_uint64 = wsdt_to_uint64_ts;
     ts_dt->to_uint32 = wsdt_to_uint32_ts;
     ts_dt->to_double = wsdt_to_double_ts;
     wsdatatype_register_alias(tl, ts_dt, WSDT_TS_STR);

     switch (sizeof(time_t)) {
          case 4:
               wsdatatype_register_subelement(ts_dt, tl, "SEC",
                         "UINT_TYPE", offsetof(wsdt_ts_t, sec));
               wsdatatype_register_subelement(ts_dt, tl, "USEC",
                         "UINT_TYPE", offsetof(wsdt_ts_t, usec));
               break;
          case 8:
               wsdatatype_register_subelement(ts_dt, tl, "SEC",
                         "UINT64_TYPE", offsetof(wsdt_ts_t, sec));
               wsdatatype_register_subelement(ts_dt, tl, "USEC",
                         "UINT64_TYPE", offsetof(wsdt_ts_t, usec));
               break;
          default:
               error_print("Unhandled case - sizeof(time_t) = %u", (uint32_t)sizeof(time_t));
               break;
     }

     return 1;
}

 
