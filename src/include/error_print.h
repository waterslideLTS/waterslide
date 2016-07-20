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

// error_print.h
// supplementary header file needed by a couple of the utilities 
// (extracted from waterslide.h)

#ifndef _ERROR_PRINT_H
#define _ERROR_PRINT_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

// macros for printing help...
#if defined(__GNUC__) && __GNUC__ < 3
#define error_print(str, ...) fprintf(stderr, "ERROR: " str "\n", ##__VA_ARGS__)
#define error_print(str) fprintf(stderr, "ERROR: " str "\n")
#else
#define error_print(str, ...) fprintf(stderr, "ERROR:" str "\n", ##__VA_ARGS__)
#endif

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _ERROR_PRINT_H
