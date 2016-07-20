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

#ifndef _existdata_table_h
#define _existdata_table_h

#include "stringhash5.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

//generic type
typedef stringhash5_t edt_t;
typedef stringhash5_walker_t edtw_t;
typedef stringhash5_sh_opts_t edt_sh_opts_t;
typedef sh5dataread_callback_t edt_dataread_callback_t;
typedef sh5datadump_callback_t edt_datadump_callback_t;

//aliased, generic function types
typedef void (*edt_callback)(void *, void *);
typedef int (*edt_visit)(void *, void *);

//aliased, generic function names
typedef void (*edt_set_callback)(edt_t *, edt_callback, void *);
typedef void (*edt_sh_opts_alloc)(edt_sh_opts_t **);
typedef void (*edt_sh_opts_free)(edt_sh_opts_t *);
//DEPRECATED
typedef int (*edt_open_table)(edt_t **, void *, const char *, uint64_t *, int, sh_callback_t);
//DEPRECATED
typedef int (*edt_open_table_with_ptrs)(edt_t **, void *, const char *, uint64_t *, int, 
                                 sh_callback_t, edt_dataread_callback_t);
//CURRENT
typedef int (*edt_open_sht_table)(edt_t **, void *, uint64_t, uint32_t, edt_sh_opts_t *);
typedef edt_t * (*edt_create)(uint32_t, uint64_t, uint32_t);
//DEPRECATED
typedef int (*edt_create_shared)(void *, void **, const char *, uint32_t, uint32_t, int *, 
                                 sh_callback_t, void *, int, void *, uint64_t *, int, sh_callback_t);
//DEPRECATED
typedef int (*edt_create_shared_with_ptrs)(void *, void **, const char *, uint32_t, uint32_t, int *, 
                                 sh_callback_t, void *, int, void *, uint64_t *, int, sh_callback_t, 
                                 edt_dataread_callback_t);
//CURRENT
typedef int (*edt_create_shared_sht)(void *, void **, const char *, uint32_t, uint32_t, int *, 
                                 edt_sh_opts_t *);
typedef void (*edt_unlock)(edt_t *);
typedef void * (*edt_find)(edt_t *, void *, int);
typedef void * (*edt_jump_to_entry)(edt_t *, uint32_t, uint32_t);
typedef void (*edt_mark_as_used)(edt_t *, uint32_t, uint32_t);
typedef uint64_t (*edt_drop_cnt)(edt_t *);
typedef void * (*edt_find_attach)(edt_t *, void *, int);
typedef int (*edt_delete)(edt_t *, void *, int);
typedef void (*edt_flush)(edt_t *);
typedef void (*edt_destroy)(edt_t *);
typedef void (*edt_scour)(edt_t *, edt_callback, void *);
typedef void (*edt_scour_and_destroy)(edt_t *, edt_callback, void *);
typedef edt_t * (*edt_read)(FILE *);
typedef edt_t * (*edt_read_with_ptrs)(FILE *, edt_dataread_callback_t);
typedef int (*edt_dump)(edt_t *, FILE *);
typedef int (*edt_dump_with_ptrs)(edt_t *, FILE *, edt_datadump_callback_t);
typedef void * (*edt_find_loc)(edt_t *, ws_hashloc_t *);
typedef void * (*edt_find_wsdata)(edt_t *, wsdata_t *);
typedef void * (*edt_find_attach_loc)(edt_t *, ws_hashloc_t *);
typedef void * (*edt_find_attach_wsdata)(edt_t *, wsdata_t *);
typedef int (*edt_delete_loc)(edt_t *, ws_hashloc_t *);
typedef int (*edt_delete_wsdata)(edt_t *, wsdata_t *);
typedef edtw_t * (*edt_walker_init)(edt_t *, edt_visit, void *);
typedef int (*edt_walker_next)(edtw_t *);
typedef int (*edt_walker_destroy)(edtw_t *);

//aliased, generic function equivalencies
edt_set_callback existdata_table_set_callback = stringhash5_set_callback;
edt_sh_opts_alloc existdata_sh_opts_alloc = stringhash5_sh_opts_alloc;
edt_sh_opts_free existdata_sh_opts_free = stringhash5_sh_opts_free;
//DEPRECATED
edt_open_table existdata_table_open_table = stringhash5_open_table;
//DEPRECATED
edt_open_table_with_ptrs existdata_table_open_table_with_ptrs = stringhash5_open_table_with_ptrs;
//CURRENT
edt_open_sht_table existdata_open_sht_table = stringhash5_open_sht_table;
edt_create existdata_table_create = stringhash5_create;
//DEPRECATED
edt_create_shared existdata_table_create_shared = stringhash5_create_shared;
//DEPRECATED
edt_create_shared_with_ptrs existdata_table_create_shared_with_ptrs = stringhash5_create_shared_with_ptrs;
//CURRENT
edt_create_shared_sht existdata_table_create_shared_sht = stringhash5_create_shared_sht;
edt_unlock existdata_table_unlock = stringhash5_unlock;
edt_find existdata_table_find = stringhash5_find;
edt_jump_to_entry existdata_table_jump_to_entry = stringhash5_jump_to_entry;
edt_mark_as_used existdata_table_mark_as_used = stringhash5_mark_as_used;
edt_drop_cnt existdata_table_drop_cnt = stringhash5_drop_cnt;
edt_find_attach existdata_table_find_attach = stringhash5_find_attach;
edt_delete existdata_table_delete = stringhash5_delete;
edt_flush existdata_table_flush = stringhash5_flush;
edt_destroy existdata_table_destroy = stringhash5_destroy;
edt_scour existdata_table_scour = stringhash5_scour;
edt_scour_and_destroy existdata_table_scour_and_destroy = stringhash5_scour_and_destroy;
edt_read existdata_table_read = stringhash5_read;
edt_read_with_ptrs existdata_table_read_with_ptrs = stringhash5_read_with_ptrs;
edt_dump existdata_table_dump = stringhash5_dump;
edt_dump_with_ptrs existdata_table_dump_with_ptrs = stringhash5_dump_with_ptrs;
edt_find_loc existdata_table_find_loc = stringhash5_find_loc;
edt_find_wsdata existdata_table_find_wsdata = stringhash5_find_wsdata;
edt_find_attach_loc existdata_table_find_attach_loc = stringhash5_find_attach_loc;
edt_find_attach_wsdata existdata_table_find_attach_wsdata = stringhash5_find_attach_wsdata;
edt_delete_loc existdata_table_delete_loc = stringhash5_delete_loc;
edt_delete_wsdata existdata_table_delete_wsdata = stringhash5_delete_wsdata;
edt_walker_init existdata_table_walker_init = stringhash5_walker_init;
edt_walker_next existdata_table_walker_next = stringhash5_walker_next;
edt_walker_destroy existdata_table_walker_destroy = stringhash5_walker_destroy;

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _existdata_table_h

