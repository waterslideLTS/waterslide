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

#ifndef _SHT_LOCK_INIT_H
#define _SHT_LOCK_INIT_H

#include "cppwrap.h"
#include "shared/lock_init.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#define SHT_MUTEX_ATTR(attr) WS_MUTEX_ATTR(attr)
#ifdef WS_LOCK_DBG
#define SHT_LOCK_DECL(spin) WS_MUTEX_DECL(spin)
#define SHT_LOCK_INIT(spin,mutex_attr) WS_MUTEX_INIT(spin,mutex_attr)
#define SHT_LOCK_DESTROY(spin) WS_MUTEX_DESTROY(spin)
#define SHT_LOCK(spin) WS_MUTEX_LOCK(spin)
#define SHT_UNLOCK(spin) WS_MUTEX_UNLOCK(spin)
#else
#define SHT_LOCK_DECL(spin) WS_SPINLOCK_DECL(spin)
#define SHT_LOCK_INIT(spin,mutex_attr) WS_SPINLOCK_INIT(spin)
#define SHT_LOCK_DESTROY(spin) WS_SPINLOCK_DESTROY(spin)
#define SHT_LOCK(spin) WS_SPINLOCK_LOCK(spin)
#define SHT_UNLOCK(spin) WS_SPINLOCK_UNLOCK(spin)
#endif // WS_LOCK_DBG

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _SHT_LOCK_INIT_H

