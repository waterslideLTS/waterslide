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
#include "wsdt_status.h"
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "wsdt_ts.h"

static int wsdt_status_print(FILE * stream, wsdata_t * wsdata,
                             uint32_t printtype) {
     int n,m;
     wsdt_status_t * status = (wsdt_status_t *)wsdata->data;

     switch (printtype) {
     case WS_PRINTTYPE_HTML:
     case WS_PRINTTYPE_TEXT:
          n = wsdt_print_ts_sec(stream, status->sec);
          if (n <= 0) {
               return n;
          }
          m = fprintf(stream, " %.*s", status->buflen, status->buf);
          if (m <= 0) {
               return m;
          }
          return n+m;
          break;
     case WS_PRINTTYPE_BINARY:
          n = 0;
          n += fwrite(&status->sec, sizeof(time_t), 1, stream);
          n += fwrite(&status->buflen, sizeof(int), 1, stream);
          n += fwrite(status->buf, status->buflen, 1, stream);
          return n;
          break;
     default:
          return 0;
     }
}
static int wsdt_status_snprint(char * buf, int buflen,
                               wsdata_t * wsdata, uint32_t printtype) {
     wsdt_status_t * status = (wsdt_status_t *)wsdata->data;

     switch (printtype) {
     case WS_PRINTTYPE_TEXT:
          // not printing timestamp
          return snprintf(buf, buflen, "%.*s", status->buflen, status->buf);
          break;
     default:
          return 0;
     }
}

int datatypeloader_init(void * tl) {
     wsdatatype_register_generic_pf(tl,
                                    WSDT_STATUS_STR,
                                 sizeof(wsdt_status_t),
                                 wsdt_status_print,
                                 wsdt_status_snprint);
     return 1;
}
