
#ifndef _WATERSLIDE_BIT_H
#define _WATERSLIDE_BIT_H

#include <stdint.h>
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

//taken from http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSet64
//  by Sean Anderson, et al.
static inline uint32_t uint32_count_bits(uint32_t v) {
     uint64_t c;

     c =  (((uint64_t)v & 0xfff) * 0x1001001001001ULL & 0x84210842108421ULL) % 0x1f;
     c += ((((uint64_t)v & 0xfff000) >> 12) * 0x1001001001001ULL & 0x84210842108421ULL) % 0x1f;
     c += (((uint64_t)v >> 24) * 0x1001001001001ULL & 0x84210842108421ULL) % 0x1f;

     return (uint32_t)c;
}
static inline uint32_t uint64_count_bits(uint64_t v) {
     uint32_t u1 = (v & 0xFFFFFFFF);
     uint32_t u2 = ((v>>32) & 0xFFFFFFFF);
     return uint32_count_bits(u1) + uint32_count_bits(u2);
}

//compute log2 of an unsigned int
// by Eric Cole - http://graphics.stanford.edu/~seander/bithacks.htm
static inline uint32_t uint32_log2(uint32_t v) {
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

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WATERSLIDE_BIT_H

