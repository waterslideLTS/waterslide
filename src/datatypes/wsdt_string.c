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
#include "wsdt_string.h"
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "sysutil.h"
#include "wsdt_uint.h"
#include "wsdt_uint64.h"
#include "wsdt_int.h"
#include "wsdt_int64.h"
#include "wsdt_double.h"
#include "timeparse.h"

ws_hashloc_t * wsdt_string_hash(wsdata_t*);
ws_hashloc_t * wsdt_tsstring_hash(wsdata_t*);

static int wsdt_print_string_wsdata(FILE * stream, wsdata_t * wsdata,
                                    uint32_t printtype) {
     wsdt_string_t * str = (wsdt_string_t*)wsdata->data;
     dprint("wsdt_print_string %d, %p", str->len, str->buf);
     if (!str->buf) {
          return 0;
     }
     int rtn = 0;
     switch (printtype) {
     case WS_PRINTTYPE_HTML:
          if (str->len > 256) {
               fprintf(stream, "\n<pre>");
          }
          sysutil_print_content_strings_web(stream, (uint8_t*)str->buf,
                                            str->len, 4);
          if (str->len > 256) {
               fprintf(stream, "</pre>\n");
          }
          return 1;
          break;
     case WS_PRINTTYPE_TEXT:
          dprint("wsdt_print_string before");
          dprint("wsdt_print_string before %c", str->buf[0]);
          return fprintf(stream, "%.*s", str->len, str->buf);
          dprint("wsdt_print_string after");
          break;
     case WS_PRINTTYPE_BINARY:
          rtn = fwrite(&str->len, sizeof(int), 1, stream);
          rtn += fwrite(str->buf, str->len, 1, stream);
          return rtn;
          break;
     default:
          return 0;
     }
}

static wsdata_t * wsdt_string_subelement_uint(wsdata_t *ndata,
                                              wsdata_t * dst, void * aux) {
     wsdt_string_t * bin = (wsdt_string_t *)ndata->data;
     wsdt_uint_t * u = (wsdt_uint_t*)dst->data;
     char buf[11];
     int i;
     for (i = 0; (i < bin->len) && (i < 10) && isdigit(bin->buf[i]); i++) {
          buf[i]=bin->buf[i];
     }
     buf[i] = 0;

     *u = atoi(buf);
     return dst;
}

static wsdata_t * wsdt_string_subelement_uint64(wsdata_t *ndata,
                                              wsdata_t * dst, void * aux) {
     wsdt_string_t * bin = (wsdt_string_t *)ndata->data;
     wsdt_uint64_t * u = (wsdt_uint64_t*)dst->data;
     char buf[21];
     int i;
     for (i = 0; (i < bin->len) && (i < 20) && isdigit(bin->buf[i]); i++) {
          buf[i]=bin->buf[i];
     }
     buf[i] = 0;

     *u = (wsdt_uint64_t)strtoull(buf, NULL, 10);
     return dst;
}

static wsdata_t * wsdt_string_subelement_int(wsdata_t *ndata,
                                              wsdata_t * dst, void * aux) {
     wsdt_string_t * bin = (wsdt_string_t *)ndata->data;
     wsdt_int_t * data = (wsdt_int_t*)dst->data;
     char buf[12];
     int i = 0;
     if(bin->buf[0] == '-') {
          buf[0] = '-';
          i++;
     }
     for (; (i < bin->len) && (i < 11) && isdigit(bin->buf[i]); i++) {
          buf[i]=bin->buf[i];
     }
     buf[i] = 0;

     *data = atoi(buf);
     return dst;
}

static wsdata_t * wsdt_string_subelement_int64(wsdata_t *ndata,
                                              wsdata_t * dst, void * aux) {
     wsdt_string_t * bin = (wsdt_string_t *)ndata->data;
     wsdt_int64_t * data = (wsdt_int64_t*)dst->data;
     char buf[21];
     int i = 0;
     if(bin->buf[0] == '-') {
          buf[0] = '-';
          i++;
     }
     for (; (i < bin->len) && (i < 20) && isdigit(bin->buf[i]); i++) {
          buf[i]=bin->buf[i];
     }
     buf[i] = 0;

     *data = (wsdt_int64_t)strtoll(buf, NULL, 10);
     return dst;
}


static wsdata_t * wsdt_string_subelement_double(wsdata_t *ndata,
                                              wsdata_t * dst, void * aux) {
     wsdt_string_t * bin = (wsdt_string_t *)ndata->data;
     wsdt_double_t * d = (wsdt_double_t*)dst->data;
     char buf[32];
     int i;
     for (i = 0; (i < bin->len) && (i < 31) &&
          (isdigit(bin->buf[i]) || bin->buf[i] == '.'); i++) {
          buf[i]=bin->buf[i];
     }
     buf[i] = 0;

     *d = atof(buf);
     return dst;
}

static wsdata_t * wsdt_string_subelement_ts(wsdata_t *ndata,
                                            wsdata_t * dst, void * aux) {
     wsdt_string_t * bin = (wsdt_string_t *)ndata->data;
     wsdt_ts_t * ts = (wsdt_ts_t*)dst->data;
     if ((timeparse_detect_date(bin->buf, bin->len) != 2) ||
         ((size_t)bin->len > 31)) {
          wsdata_delete(dst);
          return NULL;          
     }

     char buf[32];
     int len = bin->len;
     memcpy(buf, bin->buf, bin->len);
     buf[bin->len] = 0;

     if ((size_t)len > 19) {
          buf[19] = '\0';
          if ((size_t)len > 20) {
               char * ustr = buf + 20;
               ts->usec = atoi(ustr);
          }
     }
     ts->sec = timeparse_str2time(buf, STR2TIME_WSSTD_FORMAT);

     return dst;
}

static int wsdt_to_string_string(wsdata_t *wsdata, char **buf, int*len) {
     wsdt_string_t *str = (wsdt_string_t*)wsdata->data;

     *buf = str->buf; 
     *len = str->len;
     return 1;           
}

static int wsdt_to_uint32_string(wsdata_t *wsdata, uint32_t * u32) {
     wsdt_string_t *str = (wsdt_string_t*)wsdata->data;

     if (str->len && (str->len <= 10)) {
          char buf[11];
          int i;
          for (i = 0; i < str->len; i++) {
               if (isdigit(str->buf[i])) {
                    buf[i] = str->buf[i];
               }
               else {
                    return 0;
               }
          }
          buf[str->len] = '\0';
          *u32 = (uint32_t)atoi(buf);
          return 1;
     }
     return 0;
}

static int wsdt_to_uint64_string(wsdata_t *wsdata, uint64_t * u64) {
     wsdt_string_t *str = (wsdt_string_t*)wsdata->data;

     if (str->len && (str->len <= 20)) {
          char buf[21];
          int i;
          for (i = 0; i < str->len; i++) {
               if (isdigit(str->buf[i])) {
                    buf[i] = str->buf[i];
               }
               else {
                    return 0;
               }
          }
          buf[str->len] = '\0';
          *u64 = (uint64_t)strtoull(buf, NULL, 10);
          return 1;
     }
     return 0;
}

static int wsdt_to_int32_string(wsdata_t *wsdata, int32_t * i32) {
     wsdt_string_t *str = (wsdt_string_t*)wsdata->data;

     if (str->len && (str->len <= 11)) {
          char buf[12];
          int i = 0;
          if(str->buf[0] == '-') {
               buf[0] = '-';
               i++;
          }
          for (; i < str->len; i++) {
               if (isdigit(str->buf[i])) {
                    buf[i] = str->buf[i];
               }
               else {
                    return 0;
               }
          }
          buf[str->len] = '\0';
          *i32 = (int32_t)atoi(buf);
          return 1;
     }
     return 0;
}

static int wsdt_to_int64_string(wsdata_t *wsdata, int64_t * i64) {
     wsdt_string_t *str = (wsdt_string_t*)wsdata->data;

     if (str->len && (str->len <= 20)) {
          char buf[21];
          int i = 0;
          if(str->buf[0] == '-') {
               buf[0] = '-';
               i++;
          }
          for (; i < str->len; i++) {
               if (isdigit(str->buf[i])) {
                    buf[i] = str->buf[i];
               }
               else {
                    return 0;
               }
          }
          buf[str->len] = '\0';
          *i64 = (int64_t)strtoll(buf, NULL, 10);
          return 1;
     }
     return 0;
}

static int wsdt_to_double_string(wsdata_t *wsdata, double * dbl) {
     wsdt_string_t *str = (wsdt_string_t*)wsdata->data;

     if (str->len && (str->len <= 24)) {
          char buf[25];
          int i;
          int decimal = 0;
          int start = 0;
          if (str->buf[0] == '-') {
               if (str->len == 1) {
                    return 0;
               }
               buf[0] = '-';
               start = 1;
          }
          for (i = start; i < str->len; i++) {
               if (isdigit(str->buf[i])) {
                    buf[i] = str->buf[i];
               }
               else if ((i > 0) && (str->buf[i] == '.') && !decimal) {
                    decimal++;
               }
               else {
                    return 0;
               }
          }
          buf[str->len] = '\0';
          *dbl = (double)atof(buf);
          return 1;
     }
     return 0;
}





int datatypeloader_init(void * tl) {
     wsdatatype_t * dt = wsdatatype_register(tl,
                                             WSDT_STRING_STR, sizeof(wsdt_string_t),
                                             wsdt_string_hash,
                                             wsdatatype_default_init,
                                             wsdatatype_default_delete,
                                             wsdt_print_string_wsdata,
                                             wsdatatype_default_snprint,
                                             wsdatatype_default_copy,
                                             wsdatatype_default_serialize);
     dt->to_string = wsdt_to_string_string;
     dt->to_uint32 = wsdt_to_uint32_string;
     dt->to_uint64 = wsdt_to_uint64_string;
     dt->to_int32 = wsdt_to_int32_string;
     dt->to_int64 = wsdt_to_int64_string;
     dt->to_double = wsdt_to_double_string;

     wsdatatype_register_subelement(dt, tl,
                                    "LENGTH", "UINT_TYPE",
                                    offsetof(wsdt_string_t, len));

     wsdatatype_register_subelement_func(dt, tl,
                                         "UINT",
                                         "UINT_TYPE",
                                         wsdt_string_subelement_uint,
                                         NULL);

     wsdatatype_register_subelement_func(dt, tl,
                                         "UINT64",
                                         "UINT64_TYPE",
                                         wsdt_string_subelement_uint64,
                                         NULL);

     wsdatatype_register_subelement_func(dt, tl,
                                         "INT",
                                         "INT_TYPE",
                                         wsdt_string_subelement_int,
                                         NULL);

     wsdatatype_register_subelement_func(dt, tl,
                                         "INT64",
                                         "INT64_TYPE",
                                         wsdt_string_subelement_int64,
                                         NULL);

     wsdatatype_register_subelement_func(dt, tl,
                                         "DOUBLE",
                                         "DOUBLE_TYPE",
                                         wsdt_string_subelement_double,
                                         NULL);

     wsdatatype_register_subelement_func(dt, tl,
                                         "DATETIME",
                                         "TS_TYPE",
                                         wsdt_string_subelement_ts,
                                         NULL);

     wsdatatype_register(tl,
                         WSDT_TS_STR WSDT_STRING_STR, 
                         sizeof(wsdt_ts_t) + sizeof(wsdt_string_t),
                         wsdt_tsstring_hash,
                         wsdatatype_default_init,
                         wsdatatype_default_delete,
                         wsdatatype_default_print,
                         wsdatatype_default_snprint,
                         wsdatatype_default_copy,
                         wsdatatype_default_serialize);
     return 1;
}

ws_hashloc_t* wsdt_string_hash(wsdata_t * wsdata) {
     if (!wsdata->has_hashloc) {
          wsdt_string_t *strd = (wsdt_string_t*)wsdata->data;
          wsdata->hashloc.offset = strd->buf; 
          wsdata->hashloc.len = strd->len;
          wsdata->has_hashloc = 1;
     }
     return &wsdata->hashloc;
}

ws_hashloc_t* wsdt_tsstring_hash(wsdata_t * wsdata) {
     if (!wsdata->has_hashloc) {
          wsdt_string_t *strd =
               (wsdt_string_t*)(wsdata->data + sizeof(wsdt_ts_t));
          wsdata->hashloc.offset = strd->buf; 
          wsdata->hashloc.len = strd->len;
          wsdata->has_hashloc = 1;
     }
     return &wsdata->hashloc;
}
