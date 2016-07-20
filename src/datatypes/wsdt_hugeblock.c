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
#include "waterslidedata.h"
#include "wstypes.h"
#include "wsdt_hugeblock.h"

void wsdt_custom_hugeblock_delete(wsdata_t * wsdata) {

     wsdatatype_default_delete(wsdata);
}

ws_hashloc_t* wsdt_hugeblock_hash(wsdata_t * wsdata) {
     if (!wsdata->has_hashloc) {
          wsdt_hugeblock_t *hb = (wsdt_hugeblock_t*)wsdata->data;
          wsdata->hashloc.offset = hb->buf; 
          wsdata->hashloc.len = hb->len;
          wsdata->has_hashloc = 1;
     }
     return &wsdata->hashloc;
}


//fast init function.. just zero out the length, not the string
void wsdt_init_hugeblock(wsdata_t * wsdata, wsdatatype_t * dtype) {
     if (wsdata->data) {
          wsdt_hugeblock_t * hb = (wsdt_hugeblock_t*)wsdata->data;
          hb->actual_len = 0;
          hb->num_items = 0;
          hb->seqno = 0;
          hb->linklen  = 0;
          hb->linktype = 0;
     }
}


int datatypeloader_init(void * type_list) {
     wsdatatype_t * hb_dt = wsdatatype_register_generic(type_list,
                                                     WSDT_HUGEBLOCK_STR,
                                                     sizeof(wsdt_hugeblock_t));
     if(hb_dt) {
          hb_dt->init_func = wsdt_init_hugeblock;
          hb_dt->delete_func = wsdt_custom_hugeblock_delete;
          hb_dt->hash_func = wsdt_hugeblock_hash;
          // we had a successful datatype registration
          return 1;
     }

     // was not able to register datatype, so return nil
     return 0;
}


