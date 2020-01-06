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
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <math.h>
#include "listhash.h"
#include "error_print.h"

struct _listhash_data_t {
     void * data;
     int is_reference; // so data does not get allocated 
     char * key;
     int keylen;
};

struct _listhash_digest_t {
     uint32_t depth; //current depth..
     uint32_t digest[LH_DIGEST_DEPTH];
     listhash_data_t data[LH_DIGEST_DEPTH];
     struct _listhash_digest_t * next;
};

struct _listhash_row_t {
     uint32_t depth;
     listhash_digest_t * digest_list;     
};

//compute log2 of an unsigned int
// by Eric Cole - http://graphics.stanford.edu/~seander/bithacks.htm
static inline uint32_t lh_uint32_log2(uint32_t v) {
     static const int MultiplyDeBruijnBitPosition[32] =
     {
          0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8,
          31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9
     };

     v |= v >> 1; // first round down to power of 2
     v |= v >> 2;
     v |= v >> 4;
     v |= v >> 8;
     v |= v >> 16;
     v = (v >> 1) + 1;

     return MultiplyDeBruijnBitPosition[(uint32_t)(v * 0x077CB531U) >> 27];

}

listhash_t * listhash_create(int max_records, int data_alloc) {

     uint32_t bits;
     listhash_t * lh;

     //find out how big to make table
     /// using max_record estimate and taking log2.. its slow but this is done
     // at init time..
     bits = lh_uint32_log2(max_records) + 1;
     
     lh = (listhash_t *)calloc(1, sizeof(listhash_t));
     if (!lh) {
          error_print("failed listhash_create calloc of lh");
          return NULL;
     }

     lh->bits = bits;
     lh->max_index = 1<<bits;
     lh->mask = ((uint32_t)~0)>>(32-bits);
     lh->data_alloc = data_alloc;

     // now to allocate memory...
     lh->table = (listhash_row_t *)calloc(1, lh->max_index * sizeof(listhash_row_t));

     if (!lh->table) {
          free(lh);
          error_print("failed listhash_create calloc of lh->table");
          return NULL;
     }
     return lh;
}

//given an index and digest.. find state data...
static inline void * lh_lookup (listhash_t * lh, uint32_t indx,
                               uint32_t digest, const char *key, int keylen) {
     listhash_row_t * row;
     listhash_digest_t * cursor;
     uint32_t i;

     row = &lh->table[indx];
     for (cursor = row->digest_list; cursor; cursor = cursor->next) {
          for (i = 0 ; i < cursor->depth; i++) {
               if ((cursor->digest[i] == digest) &&
                   (cursor->data[i].keylen == keylen) &&
                   (memcmp(cursor->data[i].key, key, keylen) == 0)) {
                    return cursor->data[i].data;
               }
          }
     }
     return NULL;
}


/* copied from http://burtleburtle.net/bob/hash/evahash.html */
/**/

#define lh_evahash3_mix3(a,b,c) \
{ \
  a-=b;  a-=c;  a^=(c>>13); \
  b-=c;  b-=a;  b^=(a<<8);  \
  c-=a;  c-=b;  c^=(b>>13); \
}

/* copied from http://burtleburtle.net/bob/hash/evahash.html */
// modified slightly to allow for 3 outputs
static inline void lh_evahash3(uint8_t *k, uint32_t length, uint32_t initval,
                     uint32_t * out1, uint32_t * out2, uint32_t * out3) {
    uint32_t a,b,c;
    uint32_t len;

    len = length;
    a = b = 0x9e3779b9;
    c = initval;

    while (len >= 12) {
        a+=(k[0]+((uint32_t)k[1]<<8)+((uint32_t)k[2]<<16) +((uint32_t)k[3]<<24));
        b+=(k[4]+((uint32_t)k[5]<<8)+((uint32_t)k[6]<<16) +((uint32_t)k[7]<<24));
        c+=(k[8]+((uint32_t)k[9]<<8)+((uint32_t)k[10]<<16)+((uint32_t)k[11]<<24));
        lh_evahash3_mix3(a,b,c);
        k += 12; len -= 12;
    }

    c += length;
    switch(len) {
    case 11: c+=((uint32_t)k[10]<<24);
    case 10: c+=((uint32_t)k[9]<<16);
    case 9 : c+=((uint32_t)k[8]<<8);
    case 8 : b+=((uint32_t)k[7]<<24);
    case 7 : b+=((uint32_t)k[6]<<16);
    case 6 : b+=((uint32_t)k[5]<<8);
    case 5 : b+=k[4];
    case 4 : a+=((uint32_t)k[3]<<24);
    case 3 : a+=((uint32_t)k[2]<<16);
    case 2 : a+=((uint32_t)k[1]<<8);
    case 1 : a+=k[0];
    }
    lh_evahash3_mix3(a,b,c);

    *out1 = c;
    *out2 = b;
    *out3 = a;
}


//find records... then use them
void * listhash_find(listhash_t * lh, const char * key, int keylen) {
     uint32_t h1, h2, digest;
     void * data;

     if (key == NULL) {
          return NULL;
     }
     //get lookup hashes
     lh_evahash3((uint8_t*)key, keylen, LH_HASHSEED, &h1, &h2, &digest);

     //lookup in digest.. location1
     h2 &= lh->mask;
     data = lh_lookup(lh, h2, digest, key, keylen); 
     if (data != NULL) {
          return data;
     }

     h1 &= lh->mask;
     data = lh_lookup(lh, h1, digest, key, keylen); 

     return data;
}

static inline listhash_data_t * lh_attach(listhash_t * lh, uint32_t idx1, uint32_t idx2,
                          uint32_t digest, const char * key, int keylen) {
     uint32_t insert_index;
     listhash_digest_t * cursor;
     listhash_data_t * data;

     if (lh->table[idx1].depth <= lh->table[idx2].depth) {
          insert_index = idx1;
     }
     else {
          insert_index = idx2;
     }
     //walk list until free location..
     
     //see if there is a free location
     if (lh->table[insert_index].digest_list == NULL) {
          cursor = (listhash_digest_t *) calloc(1, sizeof(listhash_digest_t));
          if (!cursor) {
               error_print("failed listhash_attach calloc of cursor");
               return NULL;
          }
          lh->table[insert_index].digest_list = cursor;
     }
     else {
          for (cursor = lh->table[insert_index].digest_list; cursor;
               cursor = cursor->next) {
               if (cursor->depth < LH_DIGEST_DEPTH) {
                    break;
               }
               // are we at the end of the list??
               else if (cursor->next == NULL) {
                    cursor->next = 
                         (listhash_digest_t *) calloc(1, sizeof(listhash_digest_t));
                    if (!cursor->next) {
                         error_print("failed listhash_attach calloc of cursor->next");
                         return NULL;
                    }
                    cursor = cursor->next;
                    break;
               }
          }
     }

     if (cursor->depth >= LH_DIGEST_DEPTH) {
          return NULL;
     }

     cursor->digest[cursor->depth] = digest;
     data = &cursor->data[cursor->depth];
     data->key = (char *)malloc(keylen);

     if (!data->key) {
          error_print("failed lh_attach malloc of data->key");
          return NULL;
     }
     memcpy(data->key, key, keylen);
     data->keylen = keylen;

     lh->table[insert_index].depth++;
     lh->records++;
     cursor->depth++;
     return data; 
}

void * listhash_find_attach(listhash_t * lh, const char * key, int keylen) {
     uint32_t h1, h2, digest;
     void *data;
     listhash_data_t * lhdata;

     if (key == NULL) {
          return NULL;
     }
     //get lookup hashes
     lh_evahash3((uint8_t*)key, keylen, LH_HASHSEED, &h1, &h2, &digest);

     /// DO FIND...
     //lookup in digest.. location1
     h2 &= lh->mask;
     data = lh_lookup(lh, h2, digest, key, keylen); 
     if (data != NULL) {
          return data;
     }

     h1 &= lh->mask;
     data = lh_lookup(lh, h1, digest, key, keylen); 
     if (data != NULL) {
          return data;
     }

     lhdata = lh_attach(lh, h2, h1, digest, key, keylen);

     //allocate new data..
     lhdata->data = calloc(1, lh->data_alloc);
     if (!lhdata->data) {
          error_print("failed listhash_attach calloc of lhdata->data");
          return NULL;
     }

     return lhdata->data;
}

void * listhash_find_attach_reference(listhash_t * lh,
                                      const char * key, int keylen,
                                      void * ref) {
     uint32_t h1, h2, digest;
     void *data;
     listhash_data_t * lhdata;

     if (key == NULL) {
          return NULL;
     }
     //get lookup hashes
     lh_evahash3((uint8_t*)key, keylen, LH_HASHSEED, &h1, &h2, &digest);

     /// DO FIND...
     //lookup in digest.. location1
     h2 &= lh->mask;
     data = lh_lookup(lh, h2, digest, key, keylen); 
     if (data != NULL) {
          return data;
     }

     h1 &= lh->mask;
     data = lh_lookup(lh, h1, digest, key, keylen); 
     if (data != NULL) {
          return data;
     }

     lhdata = lh_attach(lh, h2, h1, digest, key, keylen);

     //allocate new data..
     lhdata->data = ref;
     lhdata->is_reference = 1; // so data does not get free'd

     return lhdata->data;
}

// walk all records and call callback function
void listhash_scour(listhash_t * lh, listhash_func_t func,
                    void * vcall) {
     listhash_digest_t *cursor;
     int i, j;
     for (i = 0; i < lh->max_index; i++) {
          for (cursor = lh->table[i].digest_list; cursor;
               cursor = cursor->next) {
               for (j = 0; j < cursor->depth; j++) {
                    if (!cursor->data[j].is_reference) {
                         func(cursor->data[j].data, vcall);
                    }
               }
          }
     }
}

//completely clean out table, but keet table intact
void listhash_flush(listhash_t * lh) {
     listhash_digest_t *cursor;
     listhash_digest_t *next;
     int i, j;

     //free all data
     for ( i = 0; i < lh->max_index; i++ ) {
          cursor = lh->table[i].digest_list;
          while(cursor) {
               next = cursor->next;
               for (j = 0; j < cursor->depth; j++) {
                    free(cursor->data[j].key);
                    if (!cursor->data[j].is_reference) {
                         free(cursor->data[j].data); 
                    }
               }
               free(cursor);
               cursor = next;
          }
     }

     memset(lh->table, 0, 
            sizeof(listhash_row_t) * lh->max_index);
     lh->records = 0;
}

//destroy entire table... free all memory
void listhash_destroy(listhash_t * lh) {
     listhash_flush(lh);
     free(lh->table);
     free(lh);
}


