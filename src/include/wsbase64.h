
#ifndef _WSBASE64_H
#define _WSBASE64_H

#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

/*the following code was taken/modified from 
MODULE NAME:    b64.c

AUTHOR:         Bob Trower 08/04/01

PROJECT:        Crypt Data Packaging

COPYRIGHT:      Copyright (c) Trantor Standard Systems Inc., 2001

NOTE:           This source code may be used as you wish, subject to
                the MIT license.  See the LICENCE section below.

LICENCE:        Copyright (c) 2001 Bob Trower, Trantor Standard Systems Inc.

                Permission is hereby granted, free of charge, to any person
                obtaining a copy of this software and associated
                documentation files (the "Software"), to deal in the
                Software without restriction, including without limitation
                the rights to use, copy, modify, merge, publish, distribute,
                sublicense, and/or sell copies of the Software, and to
                permit persons to whom the Software is furnished to do so,
                subject to the following conditions:

                The above copyright notice and this permission notice shall
                be included in all copies or substantial portions of the
                Software.

                THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY
                KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
                WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
                PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
                OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
                OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
                OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
                SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

/*
 ** Translation Table as described in RFC1113
 */
static const char cb64[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";


/*
 * ** Translation Table to decode (created by author)
 * */
static const char cd64[]="|$$$}rstuvwxyz{$$$$$$$>?@ABCDEFGHIJKLMNOPQRSTUVW$$$$$$XYZ[\\]^_`abcdefghijklmnopq";

/*
** encodeblock
**
** encode 3 8-bit binary bytes as 4 '6-bit' characters
*/
static inline void wsbase64_encodeblock( unsigned char * in, unsigned char * out, int len )
{
    out[0] = cb64[ in[0] >> 2 ];
    out[1] = cb64[ ((in[0] & 0x03) << 4) | ((in[1] & 0xf0) >> 4) ];
    out[2] = (unsigned char) (len > 1 ? cb64[ ((in[1] & 0x0f) << 2) | ((in[2] & 0xc0) >> 6) ] : '=');
    out[3] = (unsigned char) (len > 2 ? cb64[ in[2] & 0x3f ] : '=');
}

/*
** encode
**
** base64 encode a stream adding padding and line breaks as per spec.
*/
static inline int wsbase64_encode(unsigned char * inbuf, int inbuflen, unsigned char * outbuf, int outbuflen )
{
     int j;
     int outlen = 0;
     int inlen = 0;

     int target = inbuflen / 3;

     for (j = 0; j < target; j++) {
          if (outbuflen >= (outlen + 4)) {
               wsbase64_encodeblock(inbuf + inlen, outbuf + outlen, 3);
               inlen += 3;
               outlen += 4;
          }
     }
     //process remainder
     if (outbuflen >= (outlen + 4)) {
          unsigned char tmp[3];
          switch (inbuflen - inlen) {
          case 1:
               tmp[0] = inbuf[inlen];
               tmp[1] = 0;
               tmp[2] = 0;
               wsbase64_encodeblock(tmp, outbuf + outlen, 1);
               outlen += 4;
               break;
          case 2:
               tmp[0] = inbuf[inlen];
               tmp[1] = inbuf[inlen+1];
               tmp[2] = 0;
               wsbase64_encodeblock(tmp, outbuf + outlen, 2);
               outlen += 4;
               break;
          } 
     }
     return outlen;
}


/*
** decodeblock
**
** decode 4 '6-bit' characters into 3 8-bit binary bytes
*/
static inline void wsbase64_decode_block( unsigned char in[4], unsigned char *out)
{   
    out[ 0 ] = (unsigned char ) (in[0] << 2 | in[1] >> 4);
    out[ 1 ] = (unsigned char ) (in[1] << 4 | in[2] >> 2);
    out[ 2 ] = (unsigned char ) (((in[2] << 6) & 0xc0) | in[3]);
}

/*
** decode
**
** decode a base64 encoded stream discarding padding, line breaks and noise
*/
static inline int wsbase64_decode_buffer(unsigned char * inbuf, int inbuflen, unsigned char * outbuf, int outbuflen ) {
     unsigned char in[4], v;
     int outlen = 0;

     int i = 0;
     int j;
     for (j = 0; j < inbuflen; j++) {
          v = inbuf[j];
          v = (unsigned char) ((v < 43 || v > 122) ? 0 : cd64[ v - 43 ]);
          if (v) {
               v = (unsigned char) ((v == '$') ? 0 : v - 61);
               if (v) {
                    in[i] = (unsigned char) (v - 1);
                    i++;
               }
          }
          if (i == 4) {
               if ((outlen + 3) <= outbuflen) {
                    wsbase64_decode_block(in,outbuf + outlen);
                    outlen += 3;
               }
               else {
                    return outlen;
               }
               i = 0;
          }
     }
     //if remainder
     if (i) {
          while (i < 4) {
               in[i] = 0;
               i++;
          }
          if ((outlen + 3) <= outbuflen) {
               wsbase64_decode_block(in,outbuf + outlen);
               outlen += 3;
          }
     }
     return outlen;
}

static inline int wsbase64_decode_buffer_rfc4648(unsigned char * inbuf, int inbuflen, unsigned char * outbuf, int outbuflen ) {
     unsigned char in[4], v;
     int outlen = 0;

     int i = 0;
     int j;
     for (j = 0; j < inbuflen; j++) {
          v = inbuf[j];
          if (v == '-') {
               v = '+';
          }
          else if (v == '_') {
               v = '/';
          }
          v = (unsigned char) ((v < 43 || v > 122) ? 0 : cd64[ v - 43 ]);
          if (v) {
               v = (unsigned char) ((v == '$') ? 0 : v - 61);
               if (v) {
                    in[i] = (unsigned char) (v - 1);
                    i++;
               }
          }
          if (i == 4) {
               if ((outlen + 3) <= outbuflen) {
                    wsbase64_decode_block(in,outbuf + outlen);
                    outlen += 3;
               }
               else {
                    return outlen;
               }
               i = 0;
          }
     }
     //if remainder
     if (i) {
          while (i < 4) {
               in[i] = 0;
               i++;
          }
          if ((outlen + 3) <= outbuflen) {
               wsbase64_decode_block(in,outbuf + outlen);
               outlen += 3;
          }
     }
     return outlen;
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WSBASE64_H
