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

#ifndef _BLOOM_FILTER_H
#define _BLOOM_FILTER_H

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <math.h>
#include "evahash3.h"
#include "sysutil.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#define INITSEED (0x12345678)
#define MAXROUNDS (10)

//some prime numbers
uint32_t bf_round_calcA[] = {0, 0, 0, 122949829, 141650963, 472882049, 32452843, 533000401, 633910099, 751};
uint32_t bf_round_calcB[] = {0, 0, 0, 15485867, 858599509, 49979687, 67867967, 6571, 104395301, 122949823};

typedef union _bf_idkey_t {
  uint32_t u32[MAXROUNDS];
  uint64_t u64[MAXROUNDS/2];
} bf_idkey_t;

typedef struct _bloomfilter_t {
  uint64_t mask;
  uint64_t usedbits;
  uint32_t records;
  uint8_t pwrsize;
  uint8_t rounds;
  bf_idkey_t hashvals;
  void * mmap_start;
  size_t mmap_length;

  FILE * mmap_fp;

  uint32_t *bits;
} bloomfilter_t;

/* destructively (re)size BF */
static inline int bloomfilter_resize(bloomfilter_t * bf, 
                                     uint8_t rnds, uint8_t size) { 
     /* allocate a new bit vector initialized to false */
     if (size > 36) { //check if size exceeds 32bits
          return 0;
     }
     uint32_t words = 1<<(size-5);
     if (bf->bits) {
          if (bf->pwrsize == size) {
               memset(bf->bits,0,words<<2);
          }
          else {
               free(bf->bits);
               bf->bits = NULL;
          }
     }
     if (!bf->bits) {
          bf->bits = (uint32_t*)calloc(sizeof(uint32_t), words);
          if (!bf->bits) {
               error_print("failed bloomfilter_resize calloc of bf->bits");
               return 0;
          }
     }
     if (rnds > MAXROUNDS) {
          rnds = MAXROUNDS;
     }
     else {
          bf->rounds = rnds;
     }
     bf->pwrsize = size;
     /* calculate the mask used to convert
      * the hash output to an index */
     bf->mask    = ((uint64_t)~0)>>(64-bf->pwrsize);
     bf->usedbits = 0;
     bf->records = 0;

     return 1;
}

static inline bloomfilter_t * bloomfilter_init(uint8_t rounds, uint8_t bit_size) {
     bloomfilter_t * bf = (bloomfilter_t*)calloc(1, sizeof(bloomfilter_t));
     if (!bf) {
          error_print("failed bloomfilter_init calloc of bloomfilter");
          return NULL;
     }

     bloomfilter_resize(bf, rounds, bit_size);
     return bf;
}
static inline void bloomfilter_destroy(bloomfilter_t * bf) {
     if (bf->bits) {
          free(bf->bits);
     }
     free(bf);
}

static inline double bloomfilter_getfull(bloomfilter_t * bf) {
     if (bf->pwrsize) return (double)(bf->usedbits)/(double)((uint64_t)1<<(bf->pwrsize)) * 100.0;
     return 0.0;
}

  //functions for replicating table.. used in NDepthBloomFilter
static inline uint32_t bloomfilter_getmapwords(bloomfilter_t * bf) { 
     return 1<<(bf->pwrsize - 5); 
}
  
static inline int bloomfilter_clear(bloomfilter_t * bf) {
     /* zero out memory */
     uint32_t words = 1<<(bf->pwrsize-5);

     if (bf->bits) memset(bf->bits,0,words<<2);
     else {
          bf->bits = (uint32_t*)calloc(sizeof(uint32_t), words);
          if (!bf->bits) {
               error_print("failed bloomfilter_clear calloc of bf->bits");
               return 0;
          }

     }

     bf->usedbits = 0;
     bf->records = 0;
     return 1;
}

static inline bloomfilter_t * bloomfilter_import(const char * filename) {
     FILE *fp;

     /* open file for reading */
     if ( (fp = sysutil_config_fopen(filename, "r")) == NULL) {
          fprintf(stderr, "bloomfilter_import failed to open %s for reading\n", filename);
          return NULL;
     };

     uint8_t size_in;
     uint8_t  rounds_in;
     uint64_t usedbits_in;
     uint32_t records_in;
     int chk = 0;
     chk = fread(&size_in,     1, 1, fp);
     chk = fread(&rounds_in,   1, 1, fp);
     chk = fread(&usedbits_in, 8, 1, fp);
     chk = fread(&records_in,  4, 1, fp);

     if(chk)
     {
	//do nothing; avoid compiler warning
     }

     /*fprintf(stderr,"Importing BF: Bit %u Used %" PRIu64 " Rec %u Rnd %u\n",
             size_in, usedbits_in, records_in, rounds_in);
             */
     bloomfilter_t * bf = (bloomfilter_t*)calloc(1, sizeof(bloomfilter_t));
     if (!bf) {
          error_print("failed bloomfilter_import calloc of bloomfilter");
          return NULL;
     }

     if (size_in != bf->pwrsize) {
          /* resize BF to fit the file parameters */
          bloomfilter_resize(bf, rounds_in, size_in);
     }
     /*
     fprintf(stderr, "Reading %" PRIu64 " bits/%" PRIu64 " bytes from file\n",
             (uint64_t)1<<bf->pwrsize, (uint64_t)1<<(bf->pwrsize-3));
            */
     uint32_t words = 1<<(bf->pwrsize-5);

     uint32_t i;
     for (i=0; i<words; ) {
          i += fread(&bf->bits[i], 4, 1, fp);
          if (ferror(fp) || feof(fp)) {
               fprintf (stderr,"Failed to read %u words, read %u\n", words, i);
               break;
          }
     }

     bf->usedbits = usedbits_in;
     bf->records  = records_in;
     bf->rounds   = rounds_in;

     sysutil_config_fclose(fp);

     return bf;
}


static inline int bf_check(bloomfilter_t * bf, uint32_t i) {
     uint32_t grp = (i & bf->mask)>>5;
     uint32_t bit = 1<<(i&0x1F);
     return (bf->bits[grp] & bit);
}

static inline int bf_checkset(bloomfilter_t * bf, uint32_t i) {
     uint32_t ind = i & bf->mask;
     uint32_t grp = ind>>5;
     uint32_t bit = 1<<(ind&0x1F);
     if (bf->bits[grp] & bit) {
          return 1;
     }
     else {
          bf->bits[grp] |= bit;
          return 0;
     }
}

static inline uint32_t bf_ideal_bits(bloomfilter_t * bf) {
     double b = (log(20*(double)bf->records)/log(2)) + 1;
     return (uint32_t)b;
}

static inline int bloomfilter_export(bloomfilter_t * bf, const char * filename) { /* export BF to file */
     if (!bf->bits) {
          return 0;
     }

     FILE * fp = fopen(filename, "w");
     if (!fp) {
          fprintf(stderr, "failed to open %s for dumping\n", filename);
          return 0;
     }

     int chk;
     chk = fwrite(&bf->pwrsize,  1, 1, fp);
     chk = fwrite(&bf->rounds,   1, 1, fp);
     chk = fwrite(&bf->usedbits, 8, 1, fp);
     chk = fwrite(&bf->records,  4, 1, fp);

     if(chk)
     {
	//do nothing; avoid a compiler warning
     }

     uint32_t words = 1<<(bf->pwrsize-5);
     uint32_t i;
     for (i=0; i<words; ) {
          i += fwrite(&bf->bits[i], 4, 1, fp);
          if (ferror(fp)) {
               fprintf (stderr,"Failed to write %u words, wrote %u\n", words, i);
               break;
          }
     }

     fclose(fp);

     fprintf(stderr,"ideal_bits %u, actual %u, records %u\n",
             bf_ideal_bits(bf),
             bf->pwrsize, bf->records);

     return 1;
}

static inline int bloomfilter_export_reduce(bloomfilter_t * bf, const char * filename, uint32_t bit_reduce) {
     if (!bf->bits) {
          return 0;
     }

     FILE * fp = sysutil_config_fopen(filename, "w");
     if (!fp) {
          fprintf(stderr, "failed to open %s for dumping\n", filename);
          return 0;
     }

     uint8_t newpwr = bf->pwrsize - bit_reduce;
     int chk;
     chk = fwrite(&newpwr,  1, 1, fp);
     chk = fwrite(&bf->rounds,   1, 1, fp);
     chk = fwrite(&bf->usedbits, 8, 1, fp);
     chk = fwrite(&bf->records,  4, 1, fp);

     if(chk)
     {
	//do nothing; avoid compiler warning
     }

     uint32_t words = 1<<(newpwr-5);
     uint32_t wbits, j;
     uint32_t reduce_regions = 1<<bit_reduce;
     uint32_t i;
     for (i=0; i<words; ) {
          wbits = 0;
          for (j = 0; j < reduce_regions; j++) {
               wbits |= bf->bits[i + (words * j)];
          }
          i += fwrite(&wbits, 4, 1, fp);
          if (ferror(fp)) {
               fprintf (stderr,"Failed to write %u words, wrote %u\n", words, i);
               break;
          }
     }
     sysutil_config_fclose(fp);

     return 1;
}

static inline int bloomfilter_export_autoreduce(bloomfilter_t * bf, const char * filename) {
     uint32_t ibits = bf_ideal_bits(bf);
     if (ibits < bf->pwrsize) {
          fprintf(stderr,"resizing bloom filter to %u bits\n", ibits);
          return bloomfilter_export_reduce(bf, filename, bf->pwrsize-ibits);
     }
     else {
          return bloomfilter_export(bf, filename);
     }
}

static inline uint64_t bf_getkey (bloomfilter_t * bf) {
     return bf->hashvals.u64[0];
};
static inline uint32_t * bf_gethashvals (bloomfilter_t * bf) {
     return bf->hashvals.u32;
};

static inline void bf_calc(bloomfilter_t * bf, uint8_t* key, uint32_t len) {
     /* calculate enough hashvals */
     evahash3(key, len, INITSEED,
              &bf->hashvals.u32[0],
              &bf->hashvals.u32[1],
              &bf->hashvals.u32[2]);
     uint32_t i;
     for(i = 3; i < bf->rounds; i++) {
          //do linear combinations of hashes
          bf->hashvals.u32[i] = bf->hashvals.u32[0] * bf_round_calcA[i] + 
               bf->hashvals.u32[1] * bf_round_calcB[i];
     }
}
static inline int bf_query_hvals(bloomfilter_t * bf, uint32_t *hvals) {
     if (!bf->bits) return 0;
     /* check hashvals against bit array */
     uint32_t i;
     for(i = 0; i < bf->rounds; i++) {
          if (!bf_check(bf,hvals[i])) return 0;
     }
     return 1; /* return true if all bits checked have been true */
}

static inline int bloomfilter_query(bloomfilter_t * bf, uint8_t* key, uint32_t len) {
     /* fill hashvals array */
     bf_calc(bf, key, len);

     /* query array from hashvals */
     return bf_query_hvals(bf, bf->hashvals.u32);
}

static inline int bf_set_hvals(bloomfilter_t * bf, uint32_t *hvals) {
     if (!bf->bits) return 0;
     int ret = 1;
     uint32_t i;
     for(i = 0; i < bf->rounds; i++) { /* generate more values */
          if (!bf_checkset(bf,hvals[i])) {         /* check if set */
              ret = 0;
              bf->usedbits++;                    /* increase usebits count */
          }
      }

      if (!ret) bf->records++; /* increase # of records if bits were flipped */
      return ret; /* return true if all bits checked have been true */
  }

static inline int bloomfilter_set(bloomfilter_t * bf, uint8_t* key, uint32_t len) {
     /* fill hashvals array */
     bf_calc(bf, key, len);

     /* set bits from hashvals */
     return bf_set_hvals(bf, bf->hashvals.u32);
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _BLOOM_FILTER_H
