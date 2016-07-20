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

#ifndef _SETUP_STARTUP_H
#define _SETUP_STARTUP_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "waterslide.h"
#include "sht_registry.h"
#include "shared/create_shared_vars.h"
#include "shared/lock_init.h"
#include "shared/getrank.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

// Macros for noop serial functions
#ifndef WS_PTHREADS
#define SETUP_STARTUP() 1
#else // WS_PTHREADS
#define SETUP_STARTUP() setup_startup()


static inline int setup_startup(void) {

     cpu_set_t cpu_set;

     // Obtain list of available cpus on the processing platform.
     // Use this information to map threads to appropriate cpu_ids.
     CPU_ZERO(&cpu_set);

     if(pthread_getaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set)) {
          error_print("pthread_getaffinity_np failed");
          return 0;
     }

     // init locks
     if (!lock_init()) {
          return 0;
     }

     // init shared variables
     if (!create_shared_vars()) {
          return 0;
     }

     return 1;
}
#endif // WS_PTHREADS

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _SETUP_STARTUP_H
