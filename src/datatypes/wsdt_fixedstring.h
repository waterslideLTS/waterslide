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
#ifndef _WSDT_FIXEDSTRING_H
#define _WSDT_FIXEDSTRING_H
/* datatypes.h
 * here are common input/output datatypes for metadata processing
 */
#include <ctype.h>
#include <stdint.h>
#include <string.h>
#define WSDT_FIXEDSTRING_STR "FIXEDSTRING_TYPE"
#define WSDT_FIXEDSTRING_LEN 1024

//structure for whatever you want
typedef struct _wsdt_fixedstring_t {
     int len;
     char buf[WSDT_FIXEDSTRING_LEN];
} wsdt_fixedstring_t;

typedef wsdt_fixedstring_t wsdt_fstr_t;

static inline int fixedstring_copy(wsdt_fstr_t * fstr, const char * buf, int len) {
     if (len == 0) {
          fstr->len = 0;
          return 0;
     }
     if (len > WSDT_FIXEDSTRING_LEN) {
          len = WSDT_FIXEDSTRING_LEN;
     }
     fstr->len = len;
     memcpy((void * __restrict__)fstr->buf, (void * __restrict__)buf, len);
     return len;
}

static inline int fixedstring_copy_alnum(wsdt_fstr_t * fstr, const char * buf, int len) {
     if (len == 0) {
          fstr->len = 0;
          return 0;
     }
     int outlen = 0;
     int last_space = 1;
     int i;

     for (i = 0; i < len; i++) {
          if (isalnum(buf[i]) || (buf[i] == '.')) {
               last_space = 0;
               fstr->buf[outlen] = buf[i];
               outlen++;
          }
          else if (!last_space) {
               last_space = 1;
               fstr->buf[outlen] = ' ';
               outlen++;
          }
          if (outlen >= WSDT_FIXEDSTRING_LEN) {
               break;
          }
     }
     
     fstr->len = outlen;
     return outlen;
}

#endif
