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
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "wsdt_mmap.h"
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "sysutil.h"

ws_hashloc_t * wsdt_mmap_hash(wsdata_t*);

static int wsdt_print_mmap_wsdata(FILE * stream, wsdata_t * wsdata,
                                    uint32_t printtype) {
     wsdt_mmap_t * mm = (wsdt_mmap_t*)wsdata->data;
     if (!mm->buf) {
          return 0;
     }
     int rtn = 0;
     switch (printtype) {
     case WS_PRINTTYPE_HTML:
          fprintf(stream,"\n");
          sysutil_print_content_web(stream, (uint8_t*)mm->buf, mm->len); 
          return 1;
          break;
     case WS_PRINTTYPE_TEXT:
          fprintf(stream,"\n");
          sysutil_print_content(stream, (uint8_t*)mm->buf, mm->len); 
          return 1;
          break;
     case WS_PRINTTYPE_BINARY:
          rtn = fwrite(&mm->len, sizeof(int), 1, stream);
          rtn += fwrite(mm->buf, mm->len, 1, stream);
          return rtn;
          break;
     default:
          return 0;
     }
}

static void wsdt_cleanup_wsdata(wsdata_t * wsdata)
{
     wsdt_mmap_t * mm = (wsdt_mmap_t*)wsdata->data;
     if (mm) {
          if (mm->buf && (mm->srcfd >= 0)) {
               //unmmap the file
               munmap((void *)mm->buf, mm->len);
               close(mm->srcfd);
          }
          // Set to invalid
          mm->len = 0;
          mm->buf = NULL;
          mm->srcfd = -1;
          mm->filename[0] = 0x00;
     }
}

//close the old mmap before re-using..
void wsdt_init_mmap(wsdata_t * wsdata, wsdatatype_t * dtype) {
     // quick init and resets if necessary
     wsdt_cleanup_wsdata(wsdata);
}


static int wsdt_to_string_mmap(wsdata_t *wsdata, char **buf, int*len) {
     wsdt_mmap_t *str = (wsdt_mmap_t*)wsdata->data;

     *buf = str->buf; 
     *len = str->len;
     return 1;           
}

int datatypeloader_init(void * tl) {
     wsdatatype_t * bdt = wsdatatype_register(tl,
                                              WSDT_MMAP_STR, sizeof(wsdt_mmap_t),
                                              wsdt_mmap_hash,
                                              wsdt_init_mmap,
                                              wsdatatype_default_delete,
                                              wsdt_print_mmap_wsdata,
                                              wsdatatype_default_snprint,
                                              wsdatatype_default_copy,
                                              wsdatatype_default_serialize);

     bdt->to_string = wsdt_to_string_mmap;

     wsdatatype_register_alias(tl, bdt, "MMAP_TYPE");

     wsdatatype_register_subelement(bdt, tl,
                                    "LENGTH", "INT_TYPE",
                                    offsetof(wsdt_mmap_t, len));

     return 1;
}

ws_hashloc_t* wsdt_mmap_hash(wsdata_t * wsdata) {
     if (!wsdata->has_hashloc) {
          wsdt_mmap_t *strd = (wsdt_mmap_t*)wsdata->data;
          wsdata->hashloc.offset = strd->buf; 
          wsdata->hashloc.len = strd->len;
          wsdata->has_hashloc = 1;
     }
     return &wsdata->hashloc;
}

