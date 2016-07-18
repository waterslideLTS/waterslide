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

//LISTHASH
// purpose: non-expiring hash table library that is designed for fast lookup
// and balanced insertion..
// uses link lists
// currently cannot remove data except for flushing the entire table..

//important functions are
//  lh = listhash_create( max_record, data_alloc_size);
//  vdata = listhash_find(lh, key_str, keylen);
//  vdata = listhash_find_attach(lh, key_str, keylen);
//  vdata = listhash_find_attach_reference(lh, key_str, keylen, void *);
//  listhash_flush(lh);

#ifndef _LISTHASH_H
#define _LISTHASH_H

#include <stdint.h>
#include "error_print.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#define LH_HASHSEED 0x11F00FED

#define LH_DIGEST_DEPTH 7

struct _listhash_data_t;
typedef struct _listhash_data_t listhash_data_t;

struct _listhash_digest_t;
typedef struct _listhash_digest_t listhash_digest_t;

struct _listhash_row_t;
typedef struct _listhash_row_t listhash_row_t;

typedef struct _listhash_t {
     listhash_row_t      *   table;    
     uint32_t                records;    //number of records in table currently..
     uint32_t                max_index;  // used as an estimate.. 
     uint32_t                mask; // mask for calculating index
     uint32_t                bits;  // mask for setting index
     uint32_t                data_alloc;  //size of auxillary data for each flow rec
} listhash_t;

listhash_t * listhash_create(int max_records, int data_alloc);
//find records... then use them
void * listhash_find(listhash_t * lh, const char * key, int keylen);

void * listhash_find_attach(listhash_t * lh, const char * key, int keylen);

void * listhash_find_attach_reference(listhash_t * lh,
                                      const char * key, int keylen,
                                      void * ref);

//completely clean out table, but keep table intact
void listhash_flush(listhash_t * lh);
//destroy entire table... free all memory
void listhash_destroy(listhash_t * lh);

typedef void (*listhash_func_t)(void * /*data*/, void * /*userdata*/);

//touch all non-reference records with user defined func
void listhash_scour(listhash_t *, listhash_func_t, void *);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _LISTHASH_H
