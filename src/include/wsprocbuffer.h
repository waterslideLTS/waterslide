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

#ifndef _WSPROCBUFFER_H
#define _WSPROCBUFFER_H

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <netinet/in.h>
#include <time.h>
#include <assert.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "wstypes.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

typedef int (*wsprocbuffer_sub_init)(void *, void *);
typedef int (*wsprocbuffer_sub_option)(void *, void *, int, const char *);
typedef int (*wsprocbuffer_sub_decode)(void *, wsdata_t *, wsdata_t *, uint8_t *, int);
typedef int (*wsprocbuffer_sub_element)(void *, wsdata_t *, wsdata_t *);
typedef int (*wsprocbuffer_sub_destroy)(void *);

typedef struct _wsprocbuffer_kid_t {
     wsprocbuffer_sub_init init_func;
     wsprocbuffer_sub_option option_func;
     wsprocbuffer_sub_decode decode_func;
     wsprocbuffer_sub_element element_func;
     wsprocbuffer_sub_destroy destroy_func;
     int pass_not_found; // flag to indicate if tuple should be passed if LABEL does not exist in that tuple
     int instance_len;
     char * name;
     char * option_str;
     proc_labeloffset_t * labeloffset;
} wsprocbuffer_kid_t;

int wsprocbuffer_init(int, char * const *, void **, void *, wsprocbuffer_kid_t *);
proc_process_t wsprocbuffer_input_set(void *, wsdatatype_t *, wslabel_t *,
                                      ws_outlist_t*, int, void *);
int wsprocbuffer_destroy(void *);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WSPROCBUFFER_H
