
#ifndef _EVAHASH64_H
#define _EVAHASH64_H

#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

/* copied from http://burtleburtle.net/bob/hash/evahash.html */
#define evahash64_mix(a,b,c) \
{ \
  a-=b;  a-=c;  a^=(c>>13); \
  b-=c;  b-=a;  b^=(a<<8);  \
  c-=a;  c-=b;  c^=(b>>13); \
  a-=b;  a-=c;  a^=(c>>12); \
  b-=c;  b-=a;  b^=(a<<16); \
  c-=a;  c-=b;  c^=(b>>5);  \
  a-=b;  a-=c;  a^=(c>>3);  \
  b-=c;  b-=a;  b^=(a<<10); \
  c-=a;  c-=b;  c^=(b>>15); \
}

/* copied from http://burtleburtle.net/bob/hash/evahash.html */
static inline uint64_t evahash64(uint8_t *k, uint32_t length, uint32_t initval) {
    uint32_t a,b,c;
    uint32_t len;

    len = length;
    a = b = 0x9e3779b9;
    c = initval;

    while (len >= 12) {
        a+=(k[0]+((uint32_t)k[1]<<8)+((uint32_t)k[2]<<16) +((uint32_t)k[3]<<24));
        b+=(k[4]+((uint32_t)k[5]<<8)+((uint32_t)k[6]<<16) +((uint32_t)k[7]<<24));
        c+=(k[8]+((uint32_t)k[9]<<8)+((uint32_t)k[10]<<16)+((uint32_t)k[11]<<24));
        evahash64_mix(a,b,c);
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
    evahash64_mix(a,b,c);

    return ((uint64_t)a<<32) | ((uint64_t)c);
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _EVAHASH64_H
