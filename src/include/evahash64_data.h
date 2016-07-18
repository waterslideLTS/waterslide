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

#ifndef _EVAHASH64_DATA_H
#define _EVAHASH64_DATA_H

#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#include "evahash64.h"
#include "waterslide.h"
#include "waterslidedata.h"

static inline uint64_t evahash64_data(wsdata_t * wsd, uint32_t seed) {
     uint64_t res = 0;
     if ( wsd ) {
          ws_hashloc_t *hashloc = wsd->dtype->hash_func(wsd);
          if ( hashloc && hashloc->offset ) {
               res = evahash64((uint8_t*)hashloc->offset, hashloc->len, seed);
          }
     }
     return res;
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _EVAHASH64_DATA_H
