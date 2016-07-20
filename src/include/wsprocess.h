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

#ifndef _WSPROCESS_H
#define _WSPROCESS_H

#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

// Macros for noop serial functions
#ifdef WS_PTHREADS
#define WS_DO_EXTERNAL_JOBS(mimo,shared_jobq) ws_do_external_jobs(mimo,shared_jobq)
#else // !WS_PTHREADS
#define WS_DO_EXTERNAL_JOBS(mimo,shared_jobq) 0
#endif // WS_PTHREADS

//functions defined in wsprocess.c and invoked in mimo.c
int ws_execute_graph(mimo_t *);
int ws_execute_exiting_graph(mimo_t *);
int ws_add_local_job_source(mimo_t *, wsdata_t *, ws_subscriber_t *);
void ws_destroy_graph(mimo_t *);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WSPROCESS_H
