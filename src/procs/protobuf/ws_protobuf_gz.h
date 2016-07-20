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
#ifndef _WS_PROTOBUF_GZ_H_
#define _WS_PROTOBUF_GZ_H_
#include <string>
#include <zlib.h>
#include "wsserial.pb.h"
#include "waterslide.h"
#include "waterslidedata.h"
#include "wstypes.h"
#include "mimo.h"
#include "listhash.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_binary.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_uint16.h"
#include "datatypes/wsdt_uint8.h"
#include "datatypes/wsdt_label.h"
#include "datatypes/wsdt_int.h"
#include "datatypes/wsdt_ip.h"

#ifdef __cplusplus
CPP_OPEN
#endif
static inline int protobuf_writefp_gz(ws_protobuf_t * pb, gzFile fp) {
     wsserial::WSsend * wssend = pb->wssend;
     uint32_t mlen = (uint32_t)wssend->ByteSize();
     //     gzwrite(&mlen, sizeof(uint32_t), 1, fp); 
     gzwrite( fp, &mlen, sizeof(uint32_t) );
     if (!mlen) {
          return 0;
     }
#ifdef FULL_PROTOBUF
     wssend->SerializeToFileDescriptor(fileno(fp));
#else
     char * buf = (char *)malloc(mlen);
     if (!buf) {
          error_print("failed protobuf_writefp_gz malloc of buf");
          return 0;
     }
     wssend->SerializeToArray(buf, mlen); 

     //     gzwrite(buf, mlen, 1, fp); 
     gzwrite( fp, buf, mlen );

     free(buf);
#endif
     return mlen;
}

static inline int protobuf_tuple_writefp_gz(ws_protobuf_t * pb, wsdata_t * input_data,
                                         gzFile fp) {

     wsserial::WSsend * wssend = pb->wssend;
     wssend->Clear();

     if (input_data->dtype == dtype_tuple) {
          wsserial::WStuple * tuple = wssend->mutable_tuple();      
          protobuf_fill_tuple(tuple, input_data, 0);
     }    
     else {
          return 0;
     }

     return protobuf_writefp_gz(pb, fp);
}

static inline int protobuf_labels_writefp_gz(ws_protobuf_t * pb,
                                          gzFile fp) {

     wsserial::WSsend * wssend = pb->wssend;
     wssend->Clear();

     //walk list of labels....
     mimo_datalists_t * mdl = (mimo_datalists_t *)pb->type_table;
     listhash_scour(mdl->label_table, protobuf_callback_addlabel, pb);

     return protobuf_writefp_gz(pb, fp);
}

#ifdef __cplusplus
CPP_CLOSE
#endif
#endif
