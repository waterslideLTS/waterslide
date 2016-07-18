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

#ifndef _KIDSHARE_H
#define _KIDSHARE_H

#include "waterslide.h"
#include "mimo.h"
#include "listhash.h"
#include "sysutil.h"
#include "shared/getrank.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#define WS_MAX_KIDSHARE (100)
typedef struct _mimo_kidshare_t {
     int cnt;  //number of sharers
     void * data;
} mimo_kidshare_t;


static inline void * ws_kidshare_get(void * v_type_table, const char * label) {
     if (!label) {
          return NULL;
     }
     int len = strlen(label);
     if (!len) {
          return NULL;
     }
     mimo_datalists_t * mdl = (mimo_datalists_t *)v_type_table;
     mimo_kidshare_t * ks;
     ks = (mimo_kidshare_t *)listhash_find(mdl->kidshare_table, label, len);
     if (!ks) {
          return NULL;
     }
     ks->cnt++;
     return ks->data;
}

static inline int ws_kidshare_put(void * v_type_table, const char * label, void * data) {
     if (!label) {
          return 0;
     }
     int len = strlen(label);
     if (!len) {
          return 0;
     }
     mimo_datalists_t * mdl = (mimo_datalists_t *)v_type_table;
     mimo_kidshare_t * ks;
     ks = (mimo_kidshare_t *)listhash_find_attach(mdl->kidshare_table, label, len);
     if (!ks) {
          return 0;
     }
     ks->cnt++;
     ks->data = data;
     return 1;
}

//return -1 on error
//return 0-N when unsharing.. if kid receives a 0 -- it means it can control
//deletion of shared data since it is the last to use it..
static inline int ws_kidshare_unshare(void * v_type_table, const char * label) {
     if (!label) {
          return -1; // NULL label: the table is not shared, do nothing
                     // this condition is detected in the stringhash*_destroy functions,
                     // so this return should never happen here
     }
     int len = strlen(label);
     if (!len) {
          return -2; // empty label: something BAD has happened, so act as if the table is not shared
     }
     mimo_datalists_t * mdl = (mimo_datalists_t *)v_type_table;
     mimo_kidshare_t * ks;
     ks = (mimo_kidshare_t *)listhash_find(mdl->kidshare_table, label, len);
     if (!ks) {
          return -3; // this label is unknown: something REALLY BAD has happened!
     }
     ks->cnt--;
     return ks->cnt; // when ks->cnt hits 0, the table can be freed by the caller
}
#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _KIDSHARE_H
