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
/* datatype for storing over 2 items of content*/

#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "wsdt_mediumstring.h"
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "sysutil.h"

static int wsdt_print_mediumstring_wsdata(FILE * stream, wsdata_t * wsdata,
                                   uint32_t printtype) {
     wsdt_mediumstring_t * fs = (wsdt_mediumstring_t*)wsdata->data;
     int rtn = 0;
     switch (printtype) {
     case WS_PRINTTYPE_HTML:
          fprintf(stream, "\n");
          sysutil_print_content_web(stream, (uint8_t * )fs->buf, fs->len);
          break;
     case WS_PRINTTYPE_TEXT:
          fprintf(stream, "\n");
          sysutil_print_content(stream, (uint8_t * )fs->buf, fs->len);
          break;
     case WS_PRINTTYPE_BINARY:
          rtn = fwrite(&fs->len, sizeof(int), 1, stream);
          rtn += fwrite(fs->buf, fs->len, 1, stream); 
          return rtn;
          break;
     default:
          return 0;
     }
     return 0;
}

//fast init function.. just zero out the length, not the string
void wsdt_init_mediumstring(wsdata_t * wsdata, wsdatatype_t * dtype) {
     wsdt_mediumstring_t * fs = (wsdt_mediumstring_t*)wsdata->data;
     fs->len = 0;
}

ws_hashloc_t* wsdt_hash_mediumstring(wsdata_t * wsdata) {
     if (!wsdata->has_hashloc) {
          wsdt_mediumstring_t *fs = (wsdt_mediumstring_t*)wsdata->data;
          wsdata->hashloc.offset = fs->buf; 
          wsdata->hashloc.len = fs->len;
          wsdata->has_hashloc = 1;
     }
     return &wsdata->hashloc;
}

static int wsdt_to_string_mediumstring(wsdata_t *wsdata, char **buf, int*len) {
     wsdt_mediumstring_t *str = (wsdt_mediumstring_t*)wsdata->data;

     *buf = str->buf; 
     *len = str->len;
     return 1;           
}

int datatypeloader_init(void * type_list) {
     wsdatatype_t *fsdt = wsdatatype_register_generic(type_list,
                                                      WSDT_MEDIUMSTRING_STR,
                                                      sizeof(wsdt_mediumstring_t));
     fsdt->print_func = wsdt_print_mediumstring_wsdata;
     fsdt->init_func = wsdt_init_mediumstring;
     fsdt->hash_func = wsdt_hash_mediumstring;
     fsdt->to_string = wsdt_to_string_mediumstring;

     wsdatatype_register_subelement(fsdt, type_list,
                                    "LENGTH", "INT_TYPE",
                                    offsetof(wsdt_mediumstring_t, len));

     wsdatatype_register_alias(type_list, fsdt, "MSTR_TYPE");

     return 1;
}
