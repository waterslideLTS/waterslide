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
#ifndef _WSDT_LABELSET_H
#define _WSDT_LABELSET_H
/* datatypes.h
 * here are common input/output datatypes for metadata processing
 */
#include <stdint.h>
#include "waterslide.h"
#define WSDT_LABELSET_STR "LABELSET_TYPE"
#define WSDT_LABELSET_MAX 128

//structure for whatever you want
typedef struct _wsdt_labelset_t {
     int len;
     wslabel_t * labels[WSDT_LABELSET_MAX];
     uint64_t hash;
} wsdt_labelset_t;


static inline int wsdt_labelset_add(wsdt_labelset_t * ls, wslabel_t * label) {

     if (ls->len >= WSDT_LABELSET_MAX) {
          return 0;
     }
     int i;
     for (i = 0; i < ls->len; i ++) {
          if (ls->labels[i] == label) {
               return 1;
          }
     }

     ls->labels[ls->len] = label;
     ls->len++;
     return 2;
}

static inline int wsdt_labelset_add_wsdata(wsdata_t * mlset, wslabel_t * label) {
     wsdt_labelset_t * ls = (wsdt_labelset_t*)mlset->data;
     return wsdt_labelset_add(ls, label);
}

#endif
