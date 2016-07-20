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
#include "wsdt_bit32.h"
#include "datatypeloader.h"
#include "waterslidedata.h"

static int wsdt_print_bit32(FILE * stream, wsdata_t * wsdata,
                           uint32_t printtype) {
      wsdt_bit32_t * bit = (wsdt_bit32_t *)wsdata->data;
      int i;
      int rtn = 0;
      switch (printtype) {
         case WS_PRINTTYPE_HTML:
         case WS_PRINTTYPE_TEXT:
              for (i = 0; i < 32; i++) {
                   if (((*bit)>>i) & 0x1) {
                        rtn += fprintf(stream, "x");
                   }
                   else {
                        rtn += fprintf(stream, ".");
                   }
              }
              return rtn;
         case WS_PRINTTYPE_BINARY: return fwrite(bit, sizeof(wsdt_bit32_t), 1, stream); break;
         default: return 0;
      }

}

int datatypeloader_init(void * type_list) {
     wsdatatype_t * uint_dt = wsdatatype_register_generic(type_list,
                                                          WSDT_BIT32_STR,
                                                          sizeof(wsdt_bit32_t));
     
     uint_dt->print_func = wsdt_print_bit32;    

     wsdatatype_register_generic_ts(type_list,
                                    WSDT_TS_STR WSDT_BIT32_STR, 
                                    sizeof(wsdt_ts_t) + sizeof(wsdt_bit32_t));
     return 1;
}
