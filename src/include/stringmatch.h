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

#ifndef _STRINGMATCH_H
#define _STRINGMATCH_H

#include <stdint.h>
#include "error_print.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#define BMH_CHARSET_SIZE 256

typedef struct _stringmatch_t {
     int bmshift[BMH_CHARSET_SIZE];
     uint8_t * str;  //string to match
     uint8_t * str2;  //.. other case.. used for nocase
     int len;
     int nocase;
} stringmatch_t;
     

stringmatch_t * stringmatch_init(uint8_t *, int);
uint8_t * stringmatch(stringmatch_t *, uint8_t *, int);
int stringmatch_offset(stringmatch_t *, uint8_t *, int);

stringmatch_t * stringmatch_init_nocase(uint8_t *, int);
uint8_t * stringmatch_nocase(stringmatch_t *, uint8_t *, int);
int stringmatch_offset_nocase(stringmatch_t *, uint8_t *, int);

void stringmatch_free(stringmatch_t *);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _STRINGMATCH_H
