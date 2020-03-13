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

#ifndef _WSPROCKEYSTATE_H
#define _WSPROCKEYSTATE_H

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

typedef int (*wsprockeystate_sub_init)(void *, void *, int);
typedef int (*wsprockeystate_sub_init_mvalue)(void *, void *, int, wslabel_t **);
typedef int (*wsprockeystate_sub_option)(void *, void *, int, const char *);
typedef int (*wsprockeystate_sub_update)(void *, void *, wsdata_t *, wsdata_t *);
typedef int (*wsprockeystate_sub_force_expire)(void *, void *, wsdata_t *, wsdata_t *);
typedef int (*wsprockeystate_sub_update_value)(void *, void *, wsdata_t *, wsdata_t *, wsdata_t *);
typedef int (*wsprockeystate_sub_update_value_index)(void *, void *, wsdata_t *, wsdata_t *, wsdata_t *, int);
typedef int (*wsprockeystate_sub_post_update_mvalue)(void *, void *, wsdata_t *, wsdata_t *, int, void *);
typedef void (*wsprockeystate_sub_expire)(void *, void *, ws_doutput_t *, ws_outtype_t *);
typedef void (*wsprockeystate_sub_expire_multi)(void *, void *, ws_doutput_t *, ws_outtype_t *, int, void *);
typedef void (*wsprockeystate_sub_flush)(void *);
typedef int (*wsprockeystate_sub_destroy)(void *);

typedef struct _wsprockeystate_kid_t {
     wsprockeystate_sub_init init_func;
     wsprockeystate_sub_init_mvalue init_mvalue_func;
     wsprockeystate_sub_option option_func;
     wsprockeystate_sub_update update_func;
     wsprockeystate_sub_update_value update_value_func;
     wsprockeystate_sub_update_value_index update_value_index_func;
     wsprockeystate_sub_post_update_mvalue post_update_mvalue_func;
     wsprockeystate_sub_force_expire force_expire_func;
     wsprockeystate_sub_expire expire_func;
     wsprockeystate_sub_flush flush_func;
     wsprockeystate_sub_destroy destroy_func;
     int instance_len;
     int state_len;
     char * name;
     char * option_str;
     proc_labeloffset_t * labeloffset;
     int gradual_expire;

     int multivalue;
     int value_size;
     wsprockeystate_sub_expire_multi expire_multi_func;
} wsprockeystate_kid_t;

int wsprockeystate_init(int, char *const*, void **, void *, wsprockeystate_kid_t *);
proc_process_t wsprockeystate_input_set(void *, wsdatatype_t *, wslabel_t *,
                                        ws_outlist_t*, int, void *);
int wsprockeystate_destroy(void *);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WSPROCKEYSTATE_H
