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
#include "wsdt_ftype.h"
#include "datatypeloader.h"
#include "waterslidedata.h"

static int wsdt_print_ftype(FILE * stream, wsdata_t * wsdata,
                                   uint32_t printtype) {
      wsdt_ftype_t * mft = (wsdt_ftype_t *)wsdata->data;
      switch (printtype) {
         case WS_PRINTTYPE_HTML:   
         case WS_PRINTTYPE_TEXT:   
              switch (*mft) {
                   case text: fprintf(stream, "text"); break; 
                   case image: fprintf(stream, "image"); break; 
                   case executable: fprintf(stream, "executable"); break; 
                   case compressed: fprintf(stream, "compressed"); break; 
                   case pdf: fprintf(stream, "pdf"); break; 
                   case unknown: fprintf(stream, "unknown"); break; 
              }
              break;
         case WS_PRINTTYPE_BINARY: return 0; break;
        default: return 0;
      }

      return 0;
}

int datatypeloader_init(void * type_list) {
     wsdatatype_t * int_dt = wsdatatype_register_generic(type_list,
                                 WSDT_FTYPE_STR,
                                 sizeof(wsdt_ftype_t));
     
     int_dt->print_func = wsdt_print_ftype;    
 
     return 1;
}
