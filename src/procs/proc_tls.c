/*

proc_tls.c - interpret binary buffer as a TLS frame for decoding

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
#define PROC_NAME "tls"
//#define DEBUG 1

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <netinet/in.h>
#include <time.h>
#include <assert.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_binary.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_bigstring.h"
#include "datatypes/wsdt_mediumstring.h"
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_ftype.h"
#include "procloader.h"
#include "wstypes.h"

#define MAXJA3 2048

char proc_version[]     = "1.0";
char proc_requires[]     = "";
char *proc_menus[]     = { NULL };
char *proc_tags[]      = { "decoder", NULL };
char *proc_alias[]     = { "tlsrec", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "decode TLS content";
char *proc_synopsis[]    = {"tls <LABEL>", NULL};
char proc_description[] = "Decode tls content.\n";

proc_example_t proc_examples[]    = {
     {"... | tls CONTENT | ...","decode as TLS the tuple member with the label CONTENT "},
     {NULL,""}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'L',"","label",
     "label of output buffer",0,0},
     {'E',"","",
     "parse extensions",0,0},
     {'J',"","",
     "compute JA3",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]    = "LABEL of tuple string member to decode";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[]     = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[]   = {NULL};
char *proc_tuple_member_labels[] = {"TLSREC", NULL};

//function prototypes for local functions
static int proc_process_label(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     wslabel_t * label_tls;
     wslabel_t * label_tlsrec;
     wslabel_t * label_tlstype;
     wslabel_t * label_tlsreclen;
     wslabel_t * label_handshake;
     wslabel_t * label_clienthello;
     wslabel_t * label_serverhello;
     wslabel_t * label_serverdone;
     wslabel_t * label_finished;
     wslabel_t * label_clientkey;
     wslabel_t * label_serverkey;
     wslabel_t * label_ciphersuites;
     wslabel_t * label_ciphersuite;
     wslabel_t * label_major;
     wslabel_t * label_minor;
     wslabel_t * label_version;
     wslabel_t * label_extension;
     wslabel_t * label_appdata;
     wslabel_t * label_appdatabuf;
     wslabel_t * label_encrypted;
     wslabel_t * label_changecipher;
     wslabel_t * label_alert;
     wslabel_t * label_random;
     wslabel_t * label_sessionid;
     wslabel_t * label_compression;
     wslabel_t * label_extensions;
     wslabel_t * label_ext;
     wslabel_t * label_extid;
     wslabel_t * label_extdata;
     wslabel_t * label_grease;
     wslabel_t * label_sni;
     wslabel_t * label_ja3;
     wslabel_t * label_hellorequest;
     wslabel_t * label_certificate;
     wslabel_t * label_certrequest;
     wslabel_t * label_certverify;
     wslabel_t * label_truncated;
     wslabel_t * label_buffer;
     wslabel_t * label_padding;
     wslabel_set_t lset;

     int do_extensions;
     int do_ja3;
     char jabuf[MAXJA3];
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc,
                             void * type_table) {
     int op;

     while ((op = getopt(argc, argv, "eEjJL:")) != EOF) {
          switch (op) {
          case 'E':
          case 'e':
               proc->do_extensions = 1;
               break;
          case 'j':
          case 'J':
               proc->do_ja3 = 1;
               break;
          case 'L':
               proc->label_tls = wsregister_label(type_table, optarg);
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset, argv[optind]);
          tool_print("searching for string with label %s",
                     argv[optind]);
          optind++;
     }
     return 1;
}

// the following is a function to take in command arguments and initalize
// this processor's instance..
//  also register as a source here..
// return 1 if ok
// return 0 if fail
int proc_init(wskid_t * kid, int argc, char ** argv, void ** vinstance, ws_sourcev_t * sv,
              void * type_table) {
     
     //allocate proc instance of this processor
     proc_instance_t * proc =
          (proc_instance_t*)calloc(1,sizeof(proc_instance_t));
     *vinstance = proc;

     proc->label_tls = wsregister_label(type_table, "TLS");
     proc->label_tlsrec = wsregister_label(type_table, "TLSREC");
     proc->label_tlstype = wsregister_label(type_table, "TLSTYPE");
     proc->label_tlsreclen = wsregister_label(type_table, "TLSRECLEN");
     proc->label_handshake = wsregister_label(type_table, "TLSHANDSHAKE");
     proc->label_clienthello = wsregister_label(type_table, "CLIENTHELLO");
     proc->label_serverhello = wsregister_label(type_table, "SERVERHELLO");
     proc->label_serverdone = wsregister_label(type_table, "SERVERDONE");
     proc->label_finished = wsregister_label(type_table, "TLSFINISHED");
     proc->label_clientkey = wsregister_label(type_table, "CLIENTKEY");
     proc->label_serverkey = wsregister_label(type_table, "SERVERKEY");
     proc->label_ciphersuites = wsregister_label(type_table, "CIPHERSUITES");
     proc->label_ciphersuite = wsregister_label(type_table, "CIPHERSUITE");
     proc->label_major = wsregister_label(type_table, "TLSMAJORVERSION");
     proc->label_minor = wsregister_label(type_table, "TLSMINORVERSION");
     proc->label_version = wsregister_label(type_table, "CLIENTVERSION");
     proc->label_extension = wsregister_label(type_table, "EXTENSION");
     proc->label_appdata = wsregister_label(type_table, "TLSAPPDATA");
     proc->label_appdatabuf = wsregister_label(type_table, "APPDATABUF");
     proc->label_encrypted = wsregister_label(type_table, "ENCRYPTED");
     proc->label_changecipher = wsregister_label(type_table, "CHANGECIPHER");
     proc->label_buffer = wsregister_label(type_table, "BUFFER");
     proc->label_alert = wsregister_label(type_table, "TLSALERT");
     proc->label_random = wsregister_label(type_table, "RANDOM");
     proc->label_sessionid = wsregister_label(type_table, "SESSIONID");
     proc->label_compression = wsregister_label(type_table, "COMPRESSION");
     proc->label_extensions = wsregister_label(type_table, "EXTENSIONS");
     proc->label_ext = wsregister_label(type_table, "EXTENSION");
     proc->label_extid = wsregister_label(type_table, "ID");
     proc->label_extdata = wsregister_label(type_table, "DATA");
     proc->label_grease = wsregister_label(type_table, "GREASE");
     proc->label_ja3 = wsregister_label(type_table, "JA3");
     proc->label_sni = wsregister_label(type_table, "SNI");
     proc->label_hellorequest = wsregister_label(type_table, "HELLOREQUEST");
     proc->label_certificate = wsregister_label(type_table, "CERTIFICATE");
     proc->label_certrequest = wsregister_label(type_table, "CERTREQUEST");
     proc->label_certverify = wsregister_label(type_table, "CERTVERIFY");
     proc->label_truncated = wsregister_label(type_table, "TRUNCATED");
     proc->label_padding = wsregister_label(type_table, "PADDING");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->lset.len) {
          //add a reasonable default
          wslabel_set_add(type_table, &proc->lset, "CONTENT");
     }
     return 1; 
}


// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
//return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * input_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     proc_instance_t * proc = (proc_instance_t *)vinstance;

     if (!wsdatatype_match(type_table, input_type, "TUPLE_TYPE")) {
          return NULL;  // not matching expected type
     }

     proc->outtype_tuple =
          ws_add_outtype(olist, input_type, NULL);

     return proc_process_label; // a function pointer
}

#define HS_HELLOREQUEST 0
#define HS_CLIENTHELLO 1
#define HS_SERVERHELLO 2
#define HS_CERTIFICATE 11
#define HS_SERVERKEY 12
#define HS_CERTREQUEST 13
#define HS_SERVERDONE 14
#define HS_CERTVERIFY 15
#define HS_CLIENTKEY 16
#define HS_FINISHED 20

#define GREASE_FILTER 0x0a0a

/*
typedef struct _tls_handshake_t {
     uint8_t type;
     uint24_t length;
} tls_handshake_t;
*/
#define CLIENTHELLO_RANDOMLEN (32)
#define CLIENTHELLO_MIN (2 + CLIENTHELLO_RANDOMLEN + 1)

static inline int get_sni(proc_instance_t * proc,
                          wsdata_t * tdata, wsdata_t * dep,
                          uint8_t * buf, int buflen) {
     while (buflen >= 4) {
          uint16_t extid = (buf[0]<<8) + buf[1];

          uint16_t extlen = (buf[2]<<8) + buf[3];
          if (buflen < (extlen + 4)) {
               return 0;
          }
          if ((extid == 0) && (extlen > 5))  {
               //sni
               uint16_t listlen = (buf[4]<<8) + buf[5];
               if (listlen == (extlen - 2)) {
                    uint8_t sntype = buf[6];
                    if (sntype == 0) {
                         uint16_t nlen = (buf[7]<<8) + buf[8];
                         if (nlen && (nlen == (listlen - 3))) {
                              tuple_member_create_dep_strdetect(tdata,dep,
                                                                proc->label_sni,
                                                                (char *)buf + 9, nlen);
                              return 1;
                         }
                    }
               }
          }


          buf    += 4 + extlen;
          buflen -= 4 + extlen;
     }
     return 0;

}


// registered TLS hello extension types:
//        http://www.iana.org/assignments/tls-extensiontype-values/tls-extensiontype-values.xhtml
static inline int parse_extensions(proc_instance_t * proc,
                                   wsdata_t * tdata, wsdata_t * dep,
                                   uint8_t * buf, int buflen) {

     if(buflen < 4) {
          return 0;
     }

     //create extension tuple:
     wsdata_t * extsub = tuple_member_create_wsdata(tdata, dtype_tuple,
                                                    proc->label_extensions);

     
     while (buflen >= 4) {
          uint16_t extid = (buf[0]<<8) + buf[1];

          uint16_t extlen = (buf[2]<<8) + buf[3];
          if (buflen < (extlen + 4)) {
               return 0;
          }
          wsdata_t * ext = tuple_member_create_wsdata(extsub, dtype_tuple,
                                                      proc->label_ext);

          if (((extid & GREASE_FILTER) == GREASE_FILTER) &&
              (buf[0] == buf[1])) {
               tuple_add_member_label(extsub, ext, proc->label_grease);
          }
          else if (extid == 21) {
               tuple_add_member_label(extsub, ext, proc->label_padding);
          }
          tuple_member_create_uint16(ext, extid, proc->label_extid);
         
          if (extlen) { 
               tuple_member_create_dep_binary(ext, dep, proc->label_extdata,
                                              (char *)buf + 4, extlen);
          }

          buf    += 4 + extlen;
          buflen -= 4 + extlen;
     }

     return 1;


}


static inline int snprint_uint16_list(char * jabuf, int max,
                                      uint8_t * buf, int buflen) {
     int jalen = 0;
     int i;
     int hasval = 0;

     for (i = 0; (i + 1) < buflen; i+=2) {
          //check grease
          if ((buf[i] == buf[i+1]) && 
              ((buf[i] & 0xf) == 0xa) && 
              ((buf[i+1] & 0xf) == 0xa)) {
               //grease!!
               dprint("GREASE: %02x%02x",buf[i], buf[i+1]);
          }
          else {
               uint16_t val = (buf[i]<<8) + buf[i+1];
               int res;
               if (hasval) {
                    res = snprintf(jabuf + jalen, max - jalen, "-%u", val); 
               }
               else {
                    res = snprintf(jabuf + jalen, max - jalen, "%u", val); 
                    hasval = 1;
               }
               if ((res < 0) || (res >= (max - jalen))) {
                    dprint("snprint list length exceeded");
                    break;
               }
               jalen += res;
          }
     }
     return jalen;
}


static int make_ja3(proc_instance_t * proc,
                    wsdata_t * tdata, uint16_t version,
                    uint8_t * ciphersuites, int cipherlen,
                    uint8_t * exbuf, int exbuflen) {

     dprint("starting ja3 %u %d %d", version, cipherlen, exbuflen);

     char * jabuf = proc->jabuf; //use preallocated buffer
     int jalen = 0;
     int sres;

     sres = snprintf(jabuf + jalen, MAXJA3 - jalen, "%u,", version); 
     if ((sres < 0) || (sres >= (MAXJA3 - jalen))) {
          return 0;
     }
     jalen += sres;

     sres = snprint_uint16_list(jabuf + jalen, MAXJA3 - jalen,
                                  ciphersuites, cipherlen);
     if ((sres < 0) || (sres >= (MAXJA3 - jalen))) {
          return 0;
     }
     jalen += sres;

     sres = snprintf(jabuf + jalen, MAXJA3 - jalen, ","); 
     if ((sres < 0) || (sres >= (MAXJA3 - jalen))) {
          return 0;
     }
     jalen += sres;

     int hasext = 0;

     uint8_t * ecf = NULL;
     int ecflen = 0;

     uint8_t * ec = NULL;
     int eclen = 0;

     while (exbuflen >= 4) {
          uint16_t extid = (exbuf[0]<<8) + exbuf[1];

          uint16_t extlen = (exbuf[2]<<8) + exbuf[3];
          if (exbuflen < (extlen + 4)) {
               break;
          }

          if (((extid & 0x0f0f) == GREASE_FILTER) &&
              (exbuf[0] == exbuf[1])) {
               //grease!
          }
          else {
               if (hasext) {
                    sres = snprintf(jabuf + jalen, MAXJA3 - jalen, "-%u", extid); 
               }
               else {
                    hasext = 1;
                    sres = snprintf(jabuf + jalen, MAXJA3 - jalen, "%u", extid); 
               }

               if ((sres < 0) || (sres >= (MAXJA3 - jalen))) {
                    break;
               }
               jalen += sres;
          }
          if (extid == 0x000a) {
               ec = exbuf + 4;
               eclen = extlen;
          }

          if (extid == 0x000b) {
               ecf = exbuf + 4;
               ecflen = extlen;
          }

          exbuf    += 4 + extlen;
          exbuflen -= 4 + extlen;
     }

     sres = snprintf(jabuf + jalen, MAXJA3 - jalen, ","); 
     if ((sres < 0) || (sres >= (MAXJA3 - jalen))) {
          return 0;
     }
     jalen += sres;

     if (ec && (eclen >2)) {
          dprint("hasec %d", eclen);
          int tlen = (ec[0]<<8) + ec[1];
          if (tlen == (eclen - 2)) {
               sres = snprint_uint16_list(jabuf + jalen, MAXJA3 - jalen,
                                            ec + 2, eclen - 2);
               if ((sres < 0) || (sres >= (MAXJA3 - jalen))) {
                    return 0;
               }
               jalen += sres;
          }
     }
     sres = snprintf(jabuf + jalen, MAXJA3 - jalen, ","); 
     if ((sres < 0) || (sres >= (MAXJA3 - jalen))) {
          return 0;
     }
     jalen += sres;

     if (ecf && (ecflen > 1)) {
          dprint("hasecf %d %u", ecflen, ecf[0]);
          int tlen = ecf[0];
          if (tlen == (ecflen - 1)) {
               ecf+=1;
               int i;
               for (i = 0; i < tlen; i++) {
                    if (i > 0) {
                         sres = snprintf(jabuf + jalen, MAXJA3 - jalen, "-%u",
                                           ecf[i]); 
                    } else {
                         sres = snprintf(jabuf + jalen, MAXJA3 - jalen, "%u",
                                           ecf[i]); 
                    }
                    if ((sres < 0) || (sres >= (MAXJA3 - jalen))) {
                         break;
                    }
                    jalen += sres;

               }
          }
     }

     //ok time to output string
     tuple_dupe_string(tdata, proc->label_ja3, jabuf, jalen);
     return 1;
}

static inline int parse_clienthello(proc_instance_t * proc,
                                    wsdata_t * tdata, wsdata_t * dep,
                                    uint8_t * buf, int buflen) {
     if (buflen < CLIENTHELLO_MIN) {
          return 0;
     } 
     uint16_t version = (buf[0]<<8) + buf[1];
     //tuple_member_create_uint16(tdata, version, proc->label_version);
     tuple_member_create_dep_binary(tdata, dep, proc->label_random,
                                    (char *)buf+2, CLIENTHELLO_RANDOMLEN);

     uint8_t *offset = buf + 2 + CLIENTHELLO_RANDOMLEN;
     int offlen = buflen - 2 - CLIENTHELLO_RANDOMLEN;

     if (offlen < 1) {
          return 0;
     }

     int sid_len = offset[0];

     if (offlen < (sid_len + 1)) {
          return 0;
     }

     if (sid_len) {
          tuple_member_create_dep_binary(tdata, dep, proc->label_sessionid,
                                         (char *)offset + 1, sid_len);
     }

     offset += sid_len + 1;
     offlen -= sid_len + 1;

     if (offlen < 2) {
          return 0;
     }

     int cipherlen = (offset[0]<<8) + offset[1];
     if (offlen < (cipherlen + 2)) {
          return 0;
     }

     uint8_t * ciphersuites = offset + 2;
     
     tuple_member_create_dep_binary(tdata, dep, proc->label_ciphersuites,
                                    (char *)offset + 2, cipherlen);

     offset += 2 + cipherlen;
     offlen -= 2 + cipherlen;

     if (offlen < 1) {
          return 0;
     }
     int complen = offset[0];
     if (offlen < (complen + 1)) {
          return 0;
     }
     if ((complen == 1) && (offset[1] == 0)) {
          //ignore this basic compression setting
     }
     else {
          tuple_member_create_dep_binary(tdata, dep, proc->label_compression,
                                         (char *)offset + 1, complen);
     }

     offset += 1 + complen;
     offlen -= 1 + complen;

     if (offlen < 2) {
          //no extensions
          return 1;  //still valid
     }
     int extlen = (offset[0]<<8) + (offset[1]);
     if (offlen < (extlen + 2)) {
          return 0;
     }
     uint8_t * extbuf = offset + 2;
     /*
        tuple_member_create_dep_binary(tdata, dep, proc->label_extensions,
                                    (char *)offset + 2, extlen);
      */


     get_sni(proc, tdata, dep, extbuf, extlen);

     if (proc->do_extensions) {
          parse_extensions(proc, tdata, dep, extbuf, extlen);
     }

     if (proc->do_ja3) {
          make_ja3(proc, tdata, version, ciphersuites, cipherlen, extbuf, extlen);
     }

     return 1;
}
#define SERVERHELLO_MIN (2+32+1+2+1)

static inline int parse_serverhello(proc_instance_t * proc,
                                    wsdata_t * tdata, wsdata_t * dep,
                                    uint8_t * buf, int buflen) {
     if (buflen < SERVERHELLO_MIN) {
          return 0;
     } 
     uint16_t version = (buf[0]<<8) + buf[1];
     tuple_member_create_uint16(tdata, version, proc->label_version);
     tuple_member_create_dep_binary(tdata, dep, proc->label_random,
                                    (char *)buf+2, CLIENTHELLO_RANDOMLEN);

     uint8_t *offset = buf + 2 + CLIENTHELLO_RANDOMLEN;
     int offlen = buflen - 2 - CLIENTHELLO_RANDOMLEN;

     if (offlen < 1) {
          return 0;
     }

     int sid_len = offset[0];

     if (offlen < (sid_len + 1)) {
          return 0;
     }

     if (sid_len) {
          tuple_member_create_dep_binary(tdata, dep, proc->label_sessionid,
                                         (char *)offset + 1, sid_len);
     }

     offset += sid_len + 1;
     offlen -= sid_len + 1;

     if (offlen < 3) {
          return 0;
     }

     //uint16_t ciphersuite = (offset[0] << 8) + offset[1];
     //tuple_member_create_uint16(tdata, ciphersuite, proc->label_ciphersuites);

     
     tuple_member_create_dep_binary(tdata, dep, proc->label_ciphersuite,
                                    (char *)offset, 2);

     uint16_t compression = offset[2];
     tuple_member_create_uint16(tdata, compression, proc->label_compression);


     offset += 3;
     offlen -= 3;

     if (offlen < 2) {
          //no extensions
          return 1;  //still valid
     }
     int extlen = (offset[0]<<8) + (offset[1]);
     if (offlen < (extlen + 2)) {
          return 0;
     }
     uint8_t * extbuf = offset + 2;

     if (proc->do_extensions) {
          parse_extensions(proc, tdata, dep, extbuf, extlen);
     }

     return 1;
}

static inline int parse_handshake(proc_instance_t * proc,
                                  wsdata_t * tdata, wsdata_t * parenttuple, wsdata_t * dep,
                                  uint8_t * buf, int buflen, uint32_t datalen) {
     //TODO: there can be multiple Handshakes in a single TLS record??

     if (buflen < 4) {
          return 0;
     }
     uint8_t htype = buf[0];
     //24 bits
     dprint("handshake len bytes %d %d %d", buf[1], buf[2], buf[3]);
     uint32_t len = (buf[1] << 16) + (buf[2] << 8) + buf[3];

     dprint("handshake len %d, buflen %d, datalen %d", len, buflen,
                datalen);
     if (len > (datalen-4)) {
          tuple_add_member_label(parenttuple, tdata, proc->label_encrypted);
          if (buflen > 4) {
               tuple_member_create_dep_binary(tdata, dep, proc->label_buffer,
                                              (char*)buf + 4, buflen - 4);
          }
          return 0;
     }
     //check if valid type
     switch(htype) {
     case HS_HELLOREQUEST:
          tuple_add_member_label(parenttuple, tdata, proc->label_hellorequest);
          break;
     case HS_CERTIFICATE:
          tuple_add_member_label(parenttuple, tdata, proc->label_certificate);
          break;
     case HS_CERTREQUEST:
          tuple_add_member_label(parenttuple, tdata, proc->label_certrequest);
          break;
     case HS_CERTVERIFY:
          tuple_add_member_label(parenttuple, tdata, proc->label_certverify);
          break;
     case HS_FINISHED:
          tuple_add_member_label(parenttuple, tdata, proc->label_finished);
          break;
     case HS_SERVERDONE:
          tuple_add_member_label(parenttuple, tdata, proc->label_serverdone);
          break;
     case HS_SERVERKEY:
          tuple_add_member_label(parenttuple, tdata, proc->label_serverkey);
          break;
     case HS_CLIENTKEY:
          tuple_add_member_label(parenttuple, tdata, proc->label_clientkey);
          break;
     case HS_SERVERHELLO:
          tuple_add_member_label(parenttuple, tdata, proc->label_serverhello);
          parse_serverhello(proc, tdata, dep, buf + 4, buflen - 4);
          break;
     case HS_CLIENTHELLO:
          tuple_add_member_label(parenttuple, tdata, proc->label_clienthello);
          parse_clienthello(proc, tdata, dep, buf + 4, buflen - 4);
          break;

     default:
          return 0;
     }

     if (buflen > 4) { 
          switch(htype) {
          case HS_HELLOREQUEST:
          case HS_CERTREQUEST:
          case HS_CERTVERIFY:
          case HS_CERTIFICATE:
          case HS_FINISHED:
          case HS_SERVERDONE:
          case HS_SERVERKEY:
          case HS_CLIENTKEY:
          //case HS_SERVERHELLO:
               tuple_member_create_dep_binary(tdata, dep, proc->label_buffer,
                                              (char*)buf + 4, buflen - 4);
               break;
          }
     }

     return 1;
}
/*
typedef struct _tls_rec_t {
     uint8_t ctype;
     uint8_t major;
     uint8_t minor;
     uint16_t len;
} tls_rec_t; 
*/

#define TLS_RECHDR (5)
#define TLS_CHANGECIPHER 20
#define TLS_ALERT 21
#define TLS_HANDSHAKE 22
#define TLS_APPDATA 23
static int decode_tls(proc_instance_t * proc,
                             wsdata_t * tdata, wsdata_t * dep,
                              uint8_t * buf, int buflen) {

     if (buflen < TLS_RECHDR) {
          return 0; // not TLS
     }

     uint8_t ctype = buf[0];
     uint8_t major = buf[1];
     uint8_t minor = buf[2];
     uint16_t rlen = (buf[3]<<8) + buf[4];

     dprint("buf %d, rec %d -- after", buflen, rlen);

     switch(ctype) {
     case TLS_CHANGECIPHER:  
     case TLS_ALERT:
     case TLS_HANDSHAKE:
     case TLS_APPDATA:
          break;
     default:
          //not valid TLS..
          return 0;
     }

     if (major != 3) {
          return 0;
     }
     if (minor > 4) {
          return 0;
     }

     if (!wsdata_check_label(tdata, proc->label_tls)) {
          wsdata_add_label(tdata, proc->label_tls);
     }

     uint8_t * next = buf + TLS_RECHDR;
     int nextlen;

     uint8_t * remainder = NULL;
     int remainlen = 0;
     dprint("buflen %d, rlen: %u", buflen, rlen);
     if (buflen > ((int) rlen + TLS_RECHDR)) {
          //we have multiple recs...
          //TODO
          nextlen = rlen;
          remainder = next + rlen;
          remainlen = buflen - rlen;
     }
     else {
          nextlen = buflen - TLS_RECHDR;
     }

     wslabel_t * label_ctype = NULL;
     switch(ctype) {
     case TLS_CHANGECIPHER:  
          label_ctype = proc->label_changecipher;
          break;
     case TLS_ALERT:
          label_ctype = proc->label_alert;
          break;
     case TLS_HANDSHAKE:
          label_ctype = proc->label_handshake;
          break;
     case TLS_APPDATA:
          label_ctype = proc->label_appdata;
          break;
     }

     wsdata_t * subtuple = tuple_member_create_wsdata(tdata, dtype_tuple,
                                                      proc->label_tlsrec);
     tuple_add_member_label(tdata, subtuple, label_ctype);
     if (!subtuple) {
          return 0;
     }

     if (rlen > buflen) {
          tuple_add_member_label(tdata, subtuple, proc->label_truncated);
     }

     tuple_member_create_uint16(subtuple, ctype, proc->label_tlstype);
     tuple_member_create_uint16(subtuple, minor, proc->label_minor);
     tuple_member_create_uint16(subtuple, rlen, proc->label_tlsreclen);

     switch(ctype) {
     case TLS_CHANGECIPHER:  
          //wsdata_add_label(tdata, proc->label_changecipher);
          dprint("change cipher %d %u", nextlen, rlen);
          tuple_member_create_dep_binary(subtuple, dep, proc->label_buffer,
                                         (char*)next, nextlen);

          break;
     case TLS_ALERT:
          //wsdata_add_label(tdata, proc->label_alert);
          dprint("change alert %d %u", nextlen, rlen);
          tuple_member_create_dep_binary(subtuple, dep, proc->label_buffer,
                                         (char*)next, nextlen);

          break;
     case TLS_HANDSHAKE:
          //wsdata_add_label(tdata, proc->label_handshake);
          parse_handshake(proc, subtuple, tdata, dep, next, nextlen, rlen);
          break;
     case TLS_APPDATA:
          //wsdata_add_label(tdata, proc->label_appdata);
          tuple_member_create_dep_binary(subtuple, dep, proc->label_appdatabuf,
                                         (char*)next, nextlen);
          dprint("change appdata %d %u", nextlen, rlen);
          break;
     }

     //check remainder
     dprint("remainder len %d", remainlen);


     decode_tls(proc, tdata, dep, remainder, remainlen);

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 2 if not output

// Commented out code represents in-place copy, but uses more memory
// The current version deflates to a persistent buffer and copies out (more computation)

static int proc_process_label(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     int id;
     wsdata_t * member;
     wslabel_t * label;
     tuple_labelset_iter_t iter;
     tuple_init_labelset_iter(&iter, input_data,
                              &proc->lset);

     while (tuple_search_labelset(&iter, &member, &label, &id)) {
          char * buf;
          int blen;
          if (dtype_string_buffer(member, &buf, &blen)) {
               decode_tls(proc, input_data, member, (uint8_t*)buf, blen);
          }
     }
     
     ws_set_outdata(input_data, proc->outtype_tuple, dout);
     proc->outcnt++;

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free dynamic allocations
     free(proc);

     return 1;
}

