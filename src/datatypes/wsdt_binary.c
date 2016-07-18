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
#include <ctype.h>
#include <stddef.h>
#include "wsdt_binary.h"
#include "wsdt_uint.h"
#include "wsdt_double.h"
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "wstypes.h"
#include "sysutil.h"

ws_hashloc_t * wsdt_binary_hash(wsdata_t*);
ws_hashloc_t * wsdt_tsbinary_hash(wsdata_t*);

static int wsdt_print_binary_wsdata(FILE * stream, wsdata_t * wsdata,
                                    uint32_t printtype) {
     wsdt_binary_t * str = (wsdt_binary_t*)wsdata->data;
     if (!str->buf) {
          return 0;
     }
     int rtn = 0;
     switch (printtype) {
     case WS_PRINTTYPE_TEXT:
          fprintf(stream,"\n");
          sysutil_print_content(stream, (uint8_t*)str->buf, str->len); 
          return 1;
          break;
     case WS_PRINTTYPE_HTML:
          fprintf(stream,"\n<pre>\n");
          sysutil_print_content_web(stream, (uint8_t*)str->buf, str->len); 
          fprintf(stream,"</pre>\n");
          return 1;
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
static wsdata_t * wsdt_binary_subelement_uint(wsdata_t *ndata,
                                              wsdata_t * dst, void * aux) {
     wsdt_binary_t * bin = (wsdt_binary_t *)ndata->data;
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

static wsdata_t * wsdt_binary_subelement_int(wsdata_t *ndata,
                                              wsdata_t * dst, void * aux) {
     wsdt_binary_t * bin = (wsdt_binary_t *)ndata->data;
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

static wsdata_t * wsdt_binary_subelement_double(wsdata_t *ndata,
                                              wsdata_t * dst, void * aux) {
     wsdt_binary_t * bin = (wsdt_binary_t *)ndata->data;
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

static inline wsdata_t * wsdt_binary_dup_printable(wsdata_t * ndata, wsdata_t * dst) {
     wsdt_binary_t * bin = (wsdt_binary_t *)ndata->data;
     wsdt_string_t * str = (wsdt_string_t*)dst->data;

     wsdata_t * wsd_buffer = wsdata_create_buffer(bin->len, &str->buf,
                                                  &str->len);
     if (!wsd_buffer) {
          return NULL;
     }
     wsdata_assign_dependency(wsd_buffer, dst); 
     int i;
     for (i = 0; i < bin->len; i++) {
          if (!isprint(bin->buf[i])) {
               str->buf[i] = '.';
          }
          else {
               str->buf[i] = bin->buf[i];
          }
     } 
     return dst;
}

static wsdata_t * wsdt_binary_subelement_string(wsdata_t *ndata,
                                                wsdata_t * dst, void * aux) {
     wsdt_binary_t * bin = (wsdt_binary_t *)ndata->data;
     wsdt_string_t * str = (wsdt_string_t*)dst->data;

     //evaluate binary to see if it is already printable
     int i;
     for (i = 0; i < bin->len; i++) {
          if (!isprint(bin->buf[i])) {
               //nonprintable found
               return wsdt_binary_dup_printable(ndata, dst);
          }
     }
     //binary buffer is already printable.. make string assignment
     str->buf = bin->buf;
     str->len = bin->len;
     wsdata_assign_dependency(ndata, dst); 
     return dst;
}

static int wsdt_to_string_binary(wsdata_t *wsdata, char **buf, int*len) {
     wsdt_binary_t *str = (wsdt_binary_t*)wsdata->data;

     *buf = str->buf;
     *len = str->len;
     return 1;
}

static int wsdt_to_uint32_binary(wsdata_t *wsdata, uint32_t * u32) {
     wsdt_binary_t *str = (wsdt_binary_t*)wsdata->data;

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

static int wsdt_to_uint64_binary(wsdata_t *wsdata, uint64_t * u64) {
     wsdt_binary_t *str = (wsdt_binary_t*)wsdata->data;

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

static int wsdt_to_int32_binary(wsdata_t *wsdata, int32_t * i32) {
     wsdt_binary_t *str = (wsdt_binary_t*)wsdata->data;

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
          *i32 = atoi(buf);
          return 1;
     }
     return 0;
}

static int wsdt_to_int64_binary(wsdata_t *wsdata, int64_t * i64) {
     wsdt_binary_t *str = (wsdt_binary_t*)wsdata->data;

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

static int wsdt_to_double_binary(wsdata_t *wsdata, double * dbl) {
     wsdt_binary_t *str = (wsdt_binary_t*)wsdata->data;

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
     wsdatatype_t * bdt = wsdatatype_register(tl,
                                              WSDT_BINARY_STR,
                                              sizeof(wsdt_binary_t),
                                              wsdt_binary_hash,
                                              wsdatatype_default_init,
                                              wsdatatype_default_delete,
                                              wsdt_print_binary_wsdata,
                                              wsdatatype_default_snprint,
                                              wsdatatype_default_copy,
                                              wsdatatype_default_serialize);
     bdt->to_string = wsdt_to_string_binary;
     bdt->to_uint32 = wsdt_to_uint32_binary;
     bdt->to_uint64 = wsdt_to_uint64_binary;
     bdt->to_int32 = wsdt_to_int32_binary;
     bdt->to_int64 = wsdt_to_int64_binary;
     bdt->to_double = wsdt_to_double_binary;

     wsdatatype_register_alias(tl, bdt, "BIN_TYPE");

     wsdatatype_register_subelement_func(bdt, tl,
                                         "UINT",
                                         "UINT_TYPE",
                                         wsdt_binary_subelement_uint,
                                         NULL);

     wsdatatype_register_subelement_func(bdt, tl,
                                         "INT",
                                         "INT_TYPE",
                                         wsdt_binary_subelement_int,
                                         NULL);

     wsdatatype_register_subelement_func(bdt, tl,
                                         "DOUBLE",
                                         "DOUBLE_TYPE",
                                         wsdt_binary_subelement_double,
                                         NULL);
     
     wsdatatype_register_subelement_func(bdt, tl,
                                         "STRING",
                                         "STRING_TYPE",
                                         wsdt_binary_subelement_string,
                                         NULL);
     

     wsdatatype_register_subelement(bdt, tl,
                                    "LENGTH", "INT_TYPE",
                                    offsetof(wsdt_binary_t, len));

     wsdatatype_register(tl,
                         WSDT_TS_STR WSDT_BINARY_STR, 
                         sizeof(wsdt_ts_t) + sizeof(wsdt_binary_t),
                         wsdt_tsbinary_hash,
                         wsdatatype_default_init,
                         wsdatatype_default_delete,
                         wsdatatype_default_print,
                         wsdatatype_default_snprint,
                         wsdatatype_default_copy,
                         wsdatatype_default_serialize);
     return 1;
}

ws_hashloc_t* wsdt_binary_hash(wsdata_t * wsdata) {
     if (!wsdata->has_hashloc) {
          wsdt_binary_t *strd = (wsdt_binary_t*)wsdata->data;
          wsdata->hashloc.offset = strd->buf; 
          wsdata->hashloc.len = strd->len;
          wsdata->has_hashloc = 1;
     }
     return &wsdata->hashloc;
}

ws_hashloc_t* wsdt_tsbinary_hash(wsdata_t * wsdata) {
     if (!wsdata->has_hashloc) {
          wsdt_binary_t *strd =
               (wsdt_binary_t*)(wsdata->data + sizeof(wsdt_ts_t));
          wsdata->hashloc.offset = strd->buf; 
          wsdata->hashloc.len = strd->len;
          wsdata->has_hashloc = 1;
     }
     return &wsdata->hashloc;
}
