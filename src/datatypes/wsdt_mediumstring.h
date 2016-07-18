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
#ifndef _WSDT_MEDIUMSTRING_H
#define _WSDT_MEDIUMSTRING_H
/* datatypes.h
 * here are common input/output datatypes for metadata processing
 */

#define WSDT_MEDIUMSTRING_STR "MEDIUMSTRING_TYPE"
//#define WSDT_MEDIUMSTRING_LEN 10240
#define WSDT_MEDIUMSTRING_LEN 16376

//structure for whatever you want
typedef struct _wsdt_mediumstring_t {
     int len;
     char buf[WSDT_MEDIUMSTRING_LEN];
} wsdt_mediumstring_t;

typedef wsdt_mediumstring_t wsdt_mstr_t;

static inline int mediumstring_copy(wsdt_mstr_t * mstr, void * buf, int len) {
     if (len == 0) {
          mstr->len = 0;
          return 0;
     }
     if (mstr->len >= WSDT_MEDIUMSTRING_LEN) {
          return 0;
     }
     if (WSDT_MEDIUMSTRING_LEN < (mstr->len + len)) {
          len = WSDT_MEDIUMSTRING_LEN - mstr->len;
     }
     memcpy((void * __restrict__)(mstr->buf + mstr->len), (void * __restrict__)buf, len);
     mstr->len += len;
     return len;
}

#endif
