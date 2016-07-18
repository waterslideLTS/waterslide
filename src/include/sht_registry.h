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

#ifndef _SHT_REGISTRY_H
#define _SHT_REGISTRY_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "error_print.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

extern uint32_t max_kids; // same for all threads
extern uint32_t sht_perf;

typedef struct _sht_registry_t {
     char * sh_type;
     char * sh_kidname;
     char * sh_label;
     void * sht;
     uint64_t size;
     uint64_t expire_cnt;
     uint32_t hash_seed;
} sht_registry_t;

extern sht_registry_t * sh_registry;
extern sht_registry_t ** loc_registry;

// Prototypes
int init_sht_registry(void);
void free_sht_registry(void);
void save_proc_name(const char *);
int move_sht_to_local_registry(void *, int *);
int verify_shared_tables(void);
int enroll_shared_in_sht_registry(void *, const char *, const char *, const uint64_t,
                                  const uint32_t);
int enroll_in_sht_registry(void *, const char *, const uint64_t, const uint32_t);
void get_sht_expire_cnt(void);
void get_sht_shared_expire_cnt(void);
void print_sht_registry(const uint32_t, const uint32_t);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _SHT_REGISTRY_H
