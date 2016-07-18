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
//#define DEBUG 1
#include "wsdt_monitor.h"
#include "datatypeloader.h"
#include "waterslidedata.h"
#include "wstypes.h"

static int wsdt_print_monitor_wsdata(FILE * stream, wsdata_t * wsdata,
                                    uint32_t printtype) {
     return 0;
}

//fast init function.. just zero out the length, not the string
void wsdt_init_monitor(wsdata_t * wsdata, wsdatatype_t * dtype) {
     wsdt_monitor_t * mon = (wsdt_monitor_t*)wsdata->data;

     mon->tuple = wsdata_alloc(dtype_tuple);

     if (mon->tuple) {
          wsdata_assign_dependency(mon->tuple, wsdata);
     }

}

ws_hashloc_t* wsdt_monitor_hash(wsdata_t * wsdata) {
     return NULL;
}

int datatypeloader_init(void * tl) {
     wsdatatype_register(tl,
                         WSDT_MONITOR_STR,
                         sizeof(wsdt_monitor_t),
                         wsdt_monitor_hash,
                         wsdt_init_monitor,
                         wsdatatype_default_delete,
                         wsdt_print_monitor_wsdata,
                         wsdatatype_default_snprint,
                         wsdatatype_default_copy,
                         wsdatatype_default_serialize);


     return 1;
}

