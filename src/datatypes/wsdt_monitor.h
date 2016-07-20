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
#ifndef _WSDT_MONITOR_H
#define _WSDT_MONITOR_H

// The following is used for health and status monitoring of data

#include <stdint.h>
#include "waterslide.h"

#define WSDT_MONITOR_STR "MONITOR_TYPE"

typedef struct _wsdt_monitor_t {
     wsdata_t * tuple;
     uint32_t visit;   //number of monitored kids visited
     WS_SPINLOCK_DECL(lock);
} wsdt_monitor_t;

static inline wsdata_t * wsdt_monitor_get_tuple(wsdata_t * ws) {
     wsdt_monitor_t * mon = (wsdt_monitor_t*)ws->data;
     return mon->tuple;
}

//used for polling
static inline uint32_t wsdt_monitor_get_visits(wsdata_t * ws) {
     wsdt_monitor_t * mon = (wsdt_monitor_t*)ws->data;

#ifdef USE_ATOMICS
     return __sync_fetch_and_add(&mon->visit, 0);
#else
     uint32_t rtn;
     WS_SPINLOCK_LOCK(&mon->lock);
     rtn = mon->visit;
     WS_SPINLOCK_UNLOCK(&mon->lock);
     return rtn;
#endif
}


//used when kid finishes with monitoring
static inline void wsdt_monitor_set_visit(wsdata_t * ws) {
     wsdt_monitor_t * mon = (wsdt_monitor_t*)ws->data;

#ifdef USE_ATOMICS
     __sync_fetch_and_add(&mon->visit, 1);
#else
     WS_SPINLOCK_LOCK(&mon->lock);
     mon->visit++;
     WS_SPINLOCK_UNLOCK(&mon->lock);
#endif
}


#endif
