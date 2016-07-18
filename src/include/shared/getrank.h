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

#ifndef _GETRANK_H
#define _GETRANK_H

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#ifdef __FreeBSD__
#include <pthread_np.h>
typedef cpuset_t cpu_set_t; // unfortunately, cpu_set_t is not defined in FreeBSD
#endif // __FreeBSD__

#ifdef WS_PTHREADS

extern __thread int thread_rank;

#define GETRANK() (thread_rank)

#else // !WS_PTHREADS

#define GETRANK() (0)

#endif // WS_PTHREADS

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _GETRANK_H
