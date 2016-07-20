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
#ifndef _WSPROTO_LIB_GZ_H_
#define _WSPROTO_LIB_GZ_H_
#include <string>
#include <zlib.h>
#include "wsproto.pb.h"
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

#ifdef __cplusplus
CPP_OPEN
#endif
static inline int wsproto_writefp_gz(wsproto::wsdata * wsproto, gzFile fp) {
     uint64_t mlen = (uint64_t)wsproto->ByteSize();
     gzwrite( fp, &mlen, sizeof(uint64_t) );
     if (!mlen) {
          return 0;
     }
     char * buf = (char *)malloc(mlen);
     if (!buf) {
          error_print("failed wsproto_writefp_gz malloc of buf");
          return 0;
     }
     wsproto->SerializeToArray(buf, mlen); 

     //     gzwrite(buf, mlen, 1, fp); 
     gzwrite( fp, buf, mlen );

     free(buf);

     return mlen;
}

static inline int wsproto_tuple_writefp_gz(wsproto::wsdata * wsproto, 
                                         wsdata_t * input_data,
                                         gzFile fp) {

     wsproto->Clear();

     // for now, only allow tuples 
     if (input_data->dtype == dtype_tuple) {
          // load the data into the protocol buffer datastructure
          wsproto_fill_data(wsproto, input_data);
     }
     else {
          return 0;
     }

     return wsproto_writefp_gz(wsproto, fp);
}

static inline int wsproto_header_writefp_gz(uint16_t protocol, uint16_t version, gzFile fp) {
     // get the length of the version number
     uint64_t mlen = sizeof(uint16_t) + sizeof(uint16_t);
     
     // write out the length
     gzwrite(fp, &mlen, sizeof(uint64_t));
     if (!mlen) {
          return 0;
     }
     
     // write out the protocol and version
     gzwrite(fp, &protocol, sizeof(uint16_t));
     gzwrite(fp, &version, sizeof(uint16_t));

     return mlen;
}

#ifdef __cplusplus
CPP_CLOSE
#endif
#endif
