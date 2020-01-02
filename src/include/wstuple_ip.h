#ifndef _WATERSLIDE_TUPLE_IP_H
#define _WATERSLIDE_TUPLE_IP_H

/*
Copyright 2019 Morgan Stanley

THIS SOFTWARE IS CONTRIBUTED SUBJECT TO THE TERMS OF YOU MAY OBTAIN A COPY OF
THE LICENSE AT https://www.apache.org/licenses/LICENSE-2.0.

THIS SOFTWARE IS LICENSED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE AND ANY
WARRANTY OF NON-INFRINGEMENT, ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE. THIS SOFTWARE MAY BE REDISTRIBUTED TO OTHERS ONLY BY
EFFECTIVELY USING THIS OR ANOTHER EQUIVALENT DISCLAIMER IN ADDITION TO ANY
OTHER REQUIRED LICENSE TERMS
   */

#include <arpa/inet.h>
#include <string.h>
#include "waterslide.h"
#include "datatypes/wsdt_tuple.h"
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

static inline int tuple_add_ipv6(wsdata_t * tdata,
                           void * ipbuf, wslabel_t* label) {
     char out[INET6_ADDRSTRLEN];
     const char* result = inet_ntop(AF_INET6, ipbuf, out, sizeof(out));
     if (!result) {
          return 0;
     }
     tuple_dupe_string(tdata, label, result, strlen(result));
     return 1;
}



static inline int tuple_add_ipv4(wsdata_t * tdata,
                           void * ipbuf, wslabel_t* label) {
     char out[INET_ADDRSTRLEN];
     const char* result = inet_ntop(AF_INET, ipbuf, out, sizeof(out));
     if (!result) {
          return 0;
     }
     tuple_dupe_string(tdata, label, result, strlen(result));
     return 1;
}

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _WATERSLIDE_TUPLE_IP_H

