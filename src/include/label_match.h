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

#ifndef _LABEL_MATCH_H
#define _LABEL_MATCH_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "waterslide.h"
#include "ahocorasick.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#define LABEL_MATCH_MAX_LABELS 2000

// instance data structure
typedef struct _label_match_t {
     ahoc_t * ac_struct;
     wslabel_t * label[LABEL_MATCH_MAX_LABELS];
     int label_cnt;
     void * type_table;
} label_match_t;

     //initialize variables
label_match_t* label_match_create(void * type_table);
int label_match_loadfile(label_match_t *, char *);
ahoc_t * label_match_finalize(label_match_t *);
wslabel_t * label_match_get_label(label_match_t *, int);
int label_match_make_label(label_match_t * lm, const char * str);
void label_match_destroy(label_match_t *);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _LABEL_MATCH_H
