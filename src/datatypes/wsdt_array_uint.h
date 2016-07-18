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
#ifndef _WSDT_ARRAY_UINT_H
#define _WSDT_ARRAY_UINT_H
/* datatypes.h
 * here are common input/output datatypes for metadata processing
 */
#include <stdint.h>
#include "waterslide.h"
#define WSDT_ARRAY_UINT_STR "ARRAY_UINT_TYPE"
#define WSDT_ARRAY_UINT_MAX 32

//structure for whatever you want
typedef struct _wsdt_array_uint_t {
     int len;
     uint32_t value[WSDT_ARRAY_UINT_MAX];
} wsdt_array_uint_t;

static inline int wsdt_array_uint_add(wsdt_array_uint_t * au, uint32_t value) {

     if (au->len >= WSDT_ARRAY_UINT_MAX) {
          return 0;
     }
     au->value[au->len] = value;
     au->len++;
     return 1;
}

static inline int wsdt_array_uint_add_wsdata(wsdata_t * auset, uint32_t value) {
     wsdt_array_uint_t * au = (wsdt_array_uint_t*)auset->data;
     return wsdt_array_uint_add(au, value);
}

#endif
