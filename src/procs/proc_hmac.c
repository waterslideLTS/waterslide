/*

proc_hmac.c - code for cryptographically hashing items

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
#define PROC_NAME "hmac"

#include <openssl/evp.h>

#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "procloader_buffer.h"

int is_procbuffer = 1;
int procbuffer_pass_not_found = 1;

char proc_version[] = "1.0";
char proc_requires[]     = "";
const char *proc_tags[] = { "annotate", NULL };
char proc_name[] = PROC_NAME;
char proc_purpose[] = "compute HMAC hash of strings or buffers";
const char *proc_synopsis[] = 
     { "hmac <LABEL> [-B] [-L <value>]", NULL };
char proc_description[] = "Compute HMAC hash of specified strings or buffers"; 
char *proc_alias[] = { "md5", "sha",  NULL };

proc_example_t proc_examples[]    = {
     {NULL,""}
};
proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
      "option description", <allow multiple>, <required>*/
     {'A',"","algorithm",
     "HMAC algorithm (md5, sha1, sha256)",0,0},
     {'B',"","",
     "return result as binary HMAC",0,0},
     {'L',"","LABEL",
     "label of HMAC",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char *proc_tuple_member_labels[] = {"HMAC", NULL};
char proc_nonswitch_opts[]    = "LABEL of tuple string member to hash";
// proc_input_types and proc_output_types automatically set for procbuffer kids
proc_port_t proc_input_ports[]     = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[]   = {NULL};

//function prototypes for local functions
typedef struct _proc_instance_t {
     wslabel_t * label_hmac;
     int binary_only;
     uint8_t digestbuf[EVP_MAX_MD_SIZE];
     const EVP_MD *md;
     EVP_MD_CTX *mdctx;
} proc_instance_t;

int procbuffer_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"HMAC",offsetof(proc_instance_t, label_hmac)},
     {"",0}
};


char procbuffer_option_str[]    = "a:A:L:B";

int procbuffer_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 'a':
     case 'A':
          proc->md = EVP_get_digestbyname(str);
          if (!proc->md) {
               tool_print("unknown digest %s", str);
               return 0;
          }
          break;
     case 'B':
          proc->binary_only = 1;
          break;
     case 'L':
          proc->label_hmac = wsregister_label(type_table, str);
          break;
     }
     return 1;
}

int procbuffer_init(void * vproc, void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     if (!proc->md) {
          proc->md = EVP_get_digestbyname("md5");
          tool_print("setting HMAC to MD5");
          if (!proc->md) {
               tool_print("unable to set default HMAC to MD5");
               return 0;
          }
     }
     proc->mdctx = EVP_MD_CTX_new();
     if (!proc->mdctx) {
          tool_print("unable to set HMAC context");
          return 0;
     }

     return 1;

}

int procbuffer_decode(void * vproc, wsdata_t * tdata,
                      wsdata_t * member, uint8_t * buf, int buflen) {
     proc_instance_t * proc = (proc_instance_t*)vproc;

     unsigned int dlen;
     EVP_DigestInit_ex(proc->mdctx, proc->md, NULL);
     EVP_DigestUpdate(proc->mdctx, buf, buflen);
     EVP_DigestFinal_ex(proc->mdctx, proc->digestbuf, &dlen);
     if (dlen == 0) {
          return 1;
     }

     if (proc->binary_only) {
          tuple_dupe_binary(tdata, proc->label_hmac, (char *)proc->digestbuf, dlen);
          return 1;
     }

     //print hex
     wsdt_string_t * str = tuple_create_string(tdata,
                                               proc->label_hmac,
                                               dlen * 2);

     uint8_t * db = proc->digestbuf;

     int i;
     for (i = 0; i < dlen; i++) {
          uint8_t upper = (db[i]>>4);
          uint8_t lower = db[i] & 0xF;

          str->buf[i*2] = (upper <= 9) ? ('0' + upper) : (upper - 10) + 'a';
          str->buf[i*2+1] = (lower <= 9) ? ('0' + lower) : (lower - 10) + 'a';
     }
     
     return 1;
}

//return 1 if successful
//return 0 if no..
int procbuffer_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (proc->mdctx) {
          EVP_MD_CTX_free(proc->mdctx);
     }

     return 1;
}

