/*

proc_dns.c - decode buffer containing binary DNS data

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

#define PROC_NAME "dns"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "procloader_buffer.h"
#include "wstuple_ip.h"

int is_procbuffer = 1;
int procbuffer_pass_not_found = 1;

char proc_version[] = "1.0";
char proc_requires[]     = "";
const char *proc_tags[] = { "decode", "parse", NULL };
char proc_name[] = PROC_NAME;
char proc_purpose[] = "decode DNS data";
const char *proc_synopsis[] = 
     { "dns <LABEL OF DNS BUFFER> ", NULL };
char proc_description[] = "decode a DNS payload\n"; 
char *proc_alias[] = { NULL };

proc_example_t proc_examples[]    = {
     {NULL,""}
};
proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
      "option description", <allow multiple>, <required>*/
     //the following must be left as-is to signify the end of the array
     {'L',"","LABEL",
     "name of subtuple for decoded DNS elements",0,0},
     {'t',"","",
     "process as TCP dns",0,0},
     {'T',"","",
     "process as TCP dns",0,0},
     {' ',"","",
     "",0,0}
};

char *proc_tuple_member_labels[] = {"DNS", NULL};
char proc_nonswitch_opts[]    = "LABEL of tuple string member to decode as DNS";
// proc_input_types and proc_output_types automatically set for procbuffer kids
proc_port_t proc_input_ports[]     = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[]   = {NULL};

//function prototypes for local functions
typedef struct _proc_instance_t {
     wslabel_t * label_parent;

     wslabel_t * label_id;
     wslabel_t * label_query;
     wslabel_t * label_response;
     wslabel_t * label_nx;
     wslabel_t * label_opcode;
     wslabel_t * label_qdcount;
     wslabel_t * label_ancount;
     wslabel_t * label_nscount;
     wslabel_t * label_arcount;
     wslabel_t * label_qrec;
     wslabel_t * label_anrec;
     wslabel_t * label_nsrec;
     wslabel_t * label_arrec;
     wslabel_t * label_aa;
     wslabel_t * label_trunc;
     wslabel_t * label_ipv4;
     wslabel_t * label_rd;
     wslabel_t * label_ra;
     wslabel_t * label_z;
     wslabel_t * label_rcode;
     wslabel_t * label_type;
     wslabel_t * label_class;
     wslabel_t * label_name;
     wslabel_t * label_ttl;
     wslabel_t * label_rdata;
     wslabel_t * label_a;
     wslabel_t * label_ptr;
     wslabel_t * label_aaaa;
     wslabel_t * label_ns;
     wslabel_t * label_cname;
     wslabel_t * label_soa;
     wslabel_t * label_mailbox;
     wslabel_t * label_serial;
     wslabel_t * label_refresh;
     wslabel_t * label_retry;
     wslabel_t * label_expire;
     wslabel_t * label_minimum;
     wslabel_t * label_txt;
     wslabel_t * label_mx;
     wslabel_t * label_mxpref;
     wslabel_t * label_edns;
     wslabel_t * label_payload;
     wslabel_t * label_exrcode;
     wslabel_t * label_version;
     wslabel_t * label_dnssecok;
     wslabel_t * label_option;
     wslabel_t * label_optcode;
     wslabel_t * label_optdata;
     wslabel_t * label_caaflag;
     wslabel_t * label_caatag;
     wslabel_t * label_caavalue;
     wslabel_t * label_caa;
     wslabel_t * label_tcpdnslen;
     wslabel_t * label_invalidtcpdns;
     wslabel_t * label_invaliddns;
     int tcpdns;

} proc_instance_t;
   
int procbuffer_instance_size = sizeof(proc_instance_t);

proc_labeloffset_t proc_labeloffset[] =
{
     {"QUERYID",offsetof(proc_instance_t, label_id)},
     {"QUERY",offsetof(proc_instance_t, label_query)},
     {"RESPONSE",offsetof(proc_instance_t, label_response)},
     {"NX",offsetof(proc_instance_t, label_nx)},
     {"OPCODE",offsetof(proc_instance_t, label_opcode)},
     {"QCOUNT",offsetof(proc_instance_t, label_qdcount)},
     {"ANCOUNT",offsetof(proc_instance_t, label_ancount)},
     {"NSCOUNT",offsetof(proc_instance_t, label_nscount)},
     {"ARCOUNT",offsetof(proc_instance_t, label_arcount)},
     {"QUERY",offsetof(proc_instance_t, label_qrec)},
     {"ANREC",offsetof(proc_instance_t, label_anrec)},
     {"NSREC",offsetof(proc_instance_t, label_nsrec)},
     {"ARREC",offsetof(proc_instance_t, label_arrec)},
     {"AUTHANSWER",offsetof(proc_instance_t, label_aa)},
     {"TRUNCATED",offsetof(proc_instance_t, label_trunc)},
     {"RECURSIONDESIRED",offsetof(proc_instance_t, label_rd)},
     {"RECURSIONAVAILABLE",offsetof(proc_instance_t, label_ra)},
     {"Z",offsetof(proc_instance_t, label_z)},
     {"RCODE",offsetof(proc_instance_t, label_rcode)},
     {"TYPE",offsetof(proc_instance_t, label_type)},
     {"CLASS",offsetof(proc_instance_t, label_class)},
     {"NAME",offsetof(proc_instance_t, label_name)},
     {"TTL",offsetof(proc_instance_t, label_ttl)},
     {"RDATA",offsetof(proc_instance_t, label_rdata)},
     {"A",offsetof(proc_instance_t, label_a)},
     {"PTR",offsetof(proc_instance_t, label_ptr)},
     {"AAAA",offsetof(proc_instance_t, label_aaaa)},
     {"IPV4",offsetof(proc_instance_t, label_ipv4)},
     {"NS",offsetof(proc_instance_t, label_ns)},
     {"CNAME",offsetof(proc_instance_t, label_cname)},
     {"SOA",offsetof(proc_instance_t, label_soa)},
     {"MAILBOX",offsetof(proc_instance_t, label_mailbox)},
     {"SERIAL",offsetof(proc_instance_t, label_serial)},
     {"REFRESH",offsetof(proc_instance_t, label_refresh)},
     {"RETRY",offsetof(proc_instance_t, label_retry)},
     {"EXPIRE",offsetof(proc_instance_t, label_expire)},
     {"MINIMUM",offsetof(proc_instance_t, label_minimum)},
     {"TXT",offsetof(proc_instance_t, label_txt)},
     {"MX",offsetof(proc_instance_t, label_mx)},
     {"MXPREF",offsetof(proc_instance_t, label_mxpref)},
     {"EDNS",offsetof(proc_instance_t, label_edns)},
     {"PAYLOAD",offsetof(proc_instance_t, label_payload)},
     {"EX_RCODE",offsetof(proc_instance_t, label_exrcode)},
     {"VERSION",offsetof(proc_instance_t, label_version)},
     {"DNSSEC_OK",offsetof(proc_instance_t, label_dnssecok)},
     {"OPTION",offsetof(proc_instance_t, label_option)},
     {"OPTCODE",offsetof(proc_instance_t, label_optcode)},
     {"OPTDATA",offsetof(proc_instance_t, label_optdata)},
     {"CAAFLAG",offsetof(proc_instance_t, label_caaflag)},
     {"CAATAG",offsetof(proc_instance_t, label_caatag)},
     {"CAAVALUE",offsetof(proc_instance_t, label_caavalue)},
     {"CAA",offsetof(proc_instance_t, label_caa)},
     {"TCPDNSLEN",offsetof(proc_instance_t, label_tcpdnslen)},
     {"INVALIDTCPDNS",offsetof(proc_instance_t, label_invalidtcpdns)},
     {"INVALIDDNS",offsetof(proc_instance_t, label_invaliddns)},
     {"",0}
};


char procbuffer_option_str[]    = "tTL:";

int procbuffer_option(void * vproc, void * type_table, int c, const char * str) {
     proc_instance_t * proc = (proc_instance_t *)vproc;

     switch(c) {
     case 't':
     case 'T':
          proc->tcpdns = 1;
          break;
     case 'L':
          proc->label_parent = wsregister_label(type_table, str);
          break;
     }
     return 1;
}

typedef struct _dnsheader_t {
     uint16_t id;
   
     //chunk1 
     uint8_t code1;
     uint8_t code2;
     /*
     uint8_t qr :1;
     uint8_t opcode :4;
     uint8_t aa:1;
     uint8_t tc:1;
     uint8_t rd:1;
     
     uint8_t ra:1;
     uint8_t z:3;
     uint8_t rcode:4;
*/
     uint16_t qdcount;
     uint16_t ancount;
     uint16_t nscount;
     uint16_t arcount;
} dnsheader_t;


// return -1 on error, return 0 on empty, otherwise return length
static int get_name_length(uint8_t * buf, int buflen, uint8_t * entire,
                           int entirelen, int * roffset, int recursion_depth) {
     if (recursion_depth > 3) {
          dprint("too much recursion");
          return -1;
     }
     int nlen = 0;
     int offset = 0;
     dprint("get name length, recursion:%d", recursion_depth);

     while (offset < buflen) {
          if (buf[offset] == 0) {
               offset += 1;
               break;
          }
          if (buf[offset] > 63) {
               if (((buf[offset] & 0xC0) == 0xC0) && 
                   ((offset + 1) < buflen)) {

                    //its a pointer offeset
                    uint16_t poffset = 
                         (((uint16_t)buf[offset] & 0x3f) << 8) + (uint16_t)buf[offset + 1];
                    if (poffset >= entirelen) {
                         dprint("pointer offset exceeds length");
                         return -1;
                    }
                    int plen = get_name_length(entire + poffset,
                                               entirelen - poffset,
                                               entire, entirelen,
                                               roffset,
                                               recursion_depth+1);
                    if (plen < 0) {
                         dprint("error from pointer return");
                         return -1;
                    }
                    if (nlen) {  //if there is already partial length
                         nlen += 1;
                    }
                    offset += 2;
                    if (roffset) {
                         *roffset = offset;
                    }
                    dprint("return from point check nlen %d plen %d", nlen, plen);
                    return nlen + plen; // always return from pointer - it must be end of name
               }
               else {
                    dprint("invalid length that is not an pointer %u",
                           buf[offset]);
                    return -1;
               }
          }
          else {
               //its a length
               if ((buf[offset] + offset + 1) > buflen) {
                    dprint("exceeded buffer");
                    return -1;
               }
               if (nlen) {
                    nlen += 1; //for period
               }
               nlen += buf[offset];
               offset += buf[offset] + 1;
          }
     }

     if (roffset) {
          *roffset = offset;
     }
     return nlen;
}

//copy encoded dns name onto pre-allocated dest buffer
//return 0 on error
static int dupe_dns_name(wsdt_string_t * dest, int namelen, uint8_t * buf, int buflen,
                         uint8_t * entire, int entirelen, int depth_buf_offset, int recursion_depth) {

     int nlen = depth_buf_offset;  //offset of buffer already written
     int offset = 0;

     while (offset < buflen) {
          if (buf[offset] == 0) {
               break;
          }
          if (buf[offset] > 63) {
               if (((buf[offset] & 0xC0) == 0xC0) && 
                   ((offset + 1) < buflen)) {

                    //its a pointer offeset
                    uint16_t poffset = 
                         (((uint16_t)buf[offset] & 0x3f) << 8) + (uint16_t)buf[offset + 1];
                    if (poffset >= entirelen) {
                         return 0;
                    }
                    return dupe_dns_name(dest, namelen, entire + poffset,
                                         entirelen - poffset, entire, entirelen,
                                         nlen, recursion_depth + 1);
               }
               else {
                    dprint("invalid length");
                    return 0;
               }
          }
          else {
               //its a length
               if ((buf[offset] + offset + 1) > buflen) {
                    dprint("exceeded buffer");
                    return 0;
               }
               if (nlen >= namelen) {
                    dprint("exceeded buffer");
                    return 0;
               }
               if (nlen) {
                    dest->buf[nlen] = '.';
                    nlen += 1; //for period
               }
               if ((nlen + buf[offset]) > namelen) {
                    dprint("exceeded dest buffer");
                    return 0;
               }
               memcpy(dest->buf + nlen, buf + 1 + offset, buf[offset]); 
               nlen += buf[offset];
               offset += buf[offset] + 1;
               dprint("dns output string so far: %.*s, len %d",
                      nlen, dest->buf, nlen);
          }
     }

     return nlen;
}

#define MIN_QREC (5)
//returns offset after record processing
static int process_query(proc_instance_t * proc, wsdata_t * tdata,
                         wsdata_t * member, uint16_t qdcount,
                         uint8_t * buf, int buflen,
                         uint8_t * entire, int entirelen) {
     uint16_t i;
     int offset = 0;
     int startoffset;

     for (i = 0; i < qdcount; i++) {
          if ((buflen - offset) < MIN_QREC) {
               dprint("too small to be a qrec");
               return 0; //too small to continue
          }
          startoffset = offset;
          int newoffset = 0;
          int nlen = get_name_length(buf + offset, buflen - offset, entire,
                                     entirelen, &newoffset, 0);
          dprint("returned get_name_length  nlen: %d, offset %d", nlen, offset);

          if (nlen < 0) {
               dprint("invalid name length");
               return 0;
          }
          offset += newoffset;
          if (offset >= buflen) {
               dprint("query offset exceeded buffer");
               return 0;
          }

          //getting ready to read codes:
          if ((offset + 4) > buflen) {
               dprint("buffer exceeded");
               return 0;
          }
          uint16_t qtype = (buf[offset] <<8) + buf[offset+1];
          uint16_t qclass = (buf[offset+2] <<8) + buf[offset+3];
          offset += 4;

          dprint("qtype %u, qclass %u", qtype, qclass);

          wsdata_t * qrec = tuple_member_create_wsdata(tdata, dtype_tuple,
                                                       proc->label_qrec);
          if (qrec) {
               tuple_member_create_uint16(qrec, qtype, proc->label_type);
               tuple_member_create_uint16(qrec, qclass, proc->label_class);
               if (!nlen) {
                    char * nullstring = ".";
                    tuple_dupe_string(qrec, proc->label_name, nullstring, 1);
               }
               else {
                    wsdt_string_t * str = tuple_create_string(qrec,
                                                              proc->label_name, nlen);
                    if (str) {
                         int dlen = dupe_dns_name(str, nlen, buf + startoffset,
                                                  buflen - startoffset,
                                                  entire, entirelen, 0, 0);
                         if (dlen <= 0) {
                              dprint("error getting dns name");
                              str->len = 0;
                         }
                         if (dlen != nlen) {
                              tool_print("unexpected length mismatch %d %d", dlen,
                                     nlen);
                         }
                    }
               }
          }

          //extract out name from startoffset

     }

     return offset;
}

//returns offset of name or 0 if error
static int extract_rdname(wsdata_t * rrec,
                          wslabel_t * label_name,
                          uint8_t * rdbuf, int rdlen,
                          uint8_t * entire, int entirelen) {
     if (rdlen <= 0) {
          dprint("invalid length %d", rdlen);
          return 0;
     }
     int rdoffset = 0;
     int namelen = get_name_length(rdbuf, rdlen, entire,
                                   entirelen, &rdoffset, 0);
     dprint("rd get_name_length  namelen: %d, rdoffset %d",
            namelen, rdoffset);
     if (namelen < 0) {
          //invalid name
          dprint("invalid ns name");
          return 0;
     }
     if (namelen == 0) {
          char * nullstring = ".";
          tuple_dupe_string(rrec, label_name, nullstring, 1);
     }
     else {
          wsdt_string_t * str = tuple_create_string(rrec,
                                                    label_name,
                                                    namelen);
          int dupelen = 
               dupe_dns_name(str, namelen, rdbuf, rdlen,
                             entire, entirelen, 0, 0);
          if (dupelen != namelen) {

               tool_print("unexpected NS length mismatch %d %d",
                          dupelen,
                          namelen);
          }
     }
     return rdoffset;
}

//return <=0 on error, otherwise returns length of edns record
static int process_edns(proc_instance_t * proc, wsdata_t * tdata, wsdata_t *member,
                        uint8_t * buf, int buflen,
                        uint8_t * entire, int entirelen) {
     int offset = 0;
     dprint("process edns");
     if (buflen < 10) {
          dprint("invalid edns");
          return 0;
     }
     uint16_t rtype = (buf[0] <<8) + buf[1];
     if (rtype != 41) {
          dprint("invalid edns rtype");
          return 0;
     }
     uint16_t payload = (buf[2] <<8) + buf[3];  //payload size
     uint8_t exrcode = buf[4];
     uint8_t version = buf[5];
     uint8_t dnssecok = buf[6] & 0x80;
     uint16_t z = ((buf[6] & 0x7f)<<8) + buf[7];
     uint16_t rdlen = (buf[8]<<8) + buf[9];
     offset += 10;

     wsdata_t * edns = tuple_member_create_wsdata(tdata, dtype_tuple, proc->label_edns);
     if (!edns) {
          return 0;
     }
     tuple_member_create_uint16(edns, payload, proc->label_payload);
     tuple_member_create_uint16(edns, exrcode, proc->label_exrcode);
     tuple_member_create_uint16(edns, version, proc->label_version);
     if (dnssecok) {
          tuple_add_member_label(tdata, edns, proc->label_dnssecok);
     }
     tuple_member_create_uint16(edns, z, proc->label_z);

     dprint("edns rdlen %u", rdlen);

     if ((rdlen + offset) > buflen) {
          dprint("rdlen buffer exceeded");
          return 0;
     }
     //do something with rd-data options
     uint8_t * optbuf = buf + offset;
     int optbuflen = rdlen;

     while(optbuflen) {
          dprint("printing edns option %d", optbuflen);
          if (optbuflen < 4) {
               dprint("invalid option lenght");
               return 0;
          }
          uint16_t optcode = (optbuf[0] <<8) + optbuf[1];  
          uint16_t optlen = (optbuf[2] <<8) + optbuf[3];  
          dprint("optlen %u", optlen);

          if (((int)optlen + 4) > optbuflen) {
               dprint("invalid option lenght %d, %d", optlen, optbuflen);
               return 0;
          }
          wsdata_t * opt = tuple_member_create_wsdata(edns, dtype_tuple,
                                                      proc->label_option);
          if (!opt) {
               return 0;
          }
          tuple_member_create_uint16(opt, optcode, proc->label_optcode);

          if (optlen) {
               tuple_member_create_dep_binary(opt, member, proc->label_optdata,
                                              (char *)(optbuf + 4), optlen);
          }

          optbuf += 4 + optlen;
          optbuflen -= 4 + optlen;
          dprint("optbuflen = %d", optbuflen);
     }

     offset += rdlen;
     dprint("exited edns cleanly");
     return offset;
}
 

#define MIN_RREC (11)
//returns offset after record processing
static int process_rrec(proc_instance_t * proc, wsdata_t * tdata, wsdata_t * member,
                 uint16_t rcount, uint8_t * buf, int buflen,
                 uint8_t * entire, int entirelen,
                 wslabel_t * label_rec) {
     dprint("process_rrec");
     uint16_t i;
     int offset = 0;
     int startoffset;

     for (i = 0; i < rcount; i++) {
          if ((buflen - offset) < MIN_RREC) {
               dprint("too small to be a rrec");
               return 0; //too small to continue
          }
          //read name...
          startoffset = offset;
          int newoffset = 0;
          int nlen = get_name_length(buf + offset, buflen - offset, entire,
                                     entirelen, &newoffset, 0);
          dprint("returned get_name_length  nlen: %d, offset %d", nlen, offset);

          if (nlen < 0) {
               dprint("invalid name length");
               return 0;
          }
          offset += newoffset;
          if (offset >= buflen) {
               dprint("rrec offset exceeded buffer");
               return 0;
          }

          //getting ready to read codes:
          if ((offset + 10) > buflen) {
               dprint("buffer exceeded");
               return 0;
          }
          uint16_t rtype = (buf[offset] <<8) + buf[offset+1];


          if ((rtype == 41) && (nlen == 0) && (newoffset == 1)) {
               int edns_len = process_edns(proc, tdata, member,
                                           buf + offset,
                                           buflen - offset,
                                           entire,
                                           entirelen);
               if (edns_len <= 0) {
                    return 0;
               }
               offset = offset + edns_len;
               continue; // loop to next record
          }

          uint16_t rclass = (buf[offset+2] <<8) + buf[offset+3];
          uint32_t ttl =
               (buf[offset+4]<<24) + (buf[offset+5]<<16) +
               (buf[offset+6]<<8) + (buf[offset+7]);
          uint16_t rdlen = (buf[offset+8]<<8) + buf[offset+9];
          offset += 10;

          if ((rdlen + offset) > buflen) {
               dprint("rdlen buffer exceeded");
               return 0;
          }
          uint8_t * rdbuf = buf + offset;
          offset += rdlen;

          dprint("rtype %u, rclass %u", rtype, rclass);

          wsdata_t * rrec = tuple_member_create_wsdata(tdata, dtype_tuple,
                                                       label_rec);
          if (rrec) {
               tuple_member_create_uint16(rrec, rtype, proc->label_type);
               tuple_member_create_uint16(rrec, rclass, proc->label_class);
               tuple_member_create_uint(rrec, ttl, proc->label_ttl);
               if (!nlen) {
                    char * nullstring = ".";
                    tuple_dupe_string(rrec, proc->label_name, nullstring, 1);
               }
               else {
                    wsdt_string_t * str = tuple_create_string(rrec,
                                                              proc->label_name, nlen);
                    if (str) {
                         int dlen = dupe_dns_name(str, nlen, buf + startoffset,
                                                  buflen - startoffset,
                                                  entire, entirelen, 0, 0);
                         if (dlen <= 0) {
                              dprint("error getting dns name");
                              str->len = 0;
                         }
                         if (dlen != nlen) {
                              tool_print("unexpected length mismatch %d %d", dlen,
                                     nlen);
                         }
                    }
               }
               switch(rtype) {
               case 1: //  IPv4 address
                    if (rdlen == 4) {
                         tuple_add_member_label(tdata, rrec, proc->label_a);
                         tuple_add_ipv4(rrec, rdbuf, proc->label_a);
                    }
                    break;
               case 28: //  IPv4 address
                    if (rdlen == 16) {
                         tuple_add_member_label(tdata, rrec, proc->label_aaaa);
                         tuple_add_ipv6(rrec, rdbuf, proc->label_aaaa);
                    }
                    break;

               case 2:  //NS
                    tuple_add_member_label(tdata, rrec, proc->label_ns);
                    extract_rdname(rrec, proc->label_ns,
                                   rdbuf, rdlen, entire, entirelen);
                    break;
               case 5: //cname
                    tuple_add_member_label(tdata, rrec, proc->label_cname);
                    extract_rdname(rrec, proc->label_cname,
                                   rdbuf, rdlen, entire, entirelen);
                    break;
               case 12: //ptr
                    tuple_add_member_label(tdata, rrec, proc->label_ptr);
                    extract_rdname(rrec, proc->label_ptr,
                                   rdbuf, rdlen, entire, entirelen);
                    break;
               case 16: //txt
                    if (extract_rdname(rrec, proc->label_txt,
                                   rdbuf, rdlen, entire, entirelen) <= 0) {
                         //if name decoding fails -- try binary buffer
                         if (rdlen) {
                              tuple_member_create_dep_binary(rrec, member,
                                                             proc->label_txt,
                                                             (char *)rdbuf, rdlen);
                         }
                    }
                    break;
               case 257: //CAA
                    if (rdlen < 2) {
                         break;
                    }
                    tuple_add_member_label(tdata, rrec, proc->label_caa);
                    tuple_member_create_uint16(rrec, rdbuf[0], proc->label_caaflag);
                    uint8_t caalen = rdbuf[1];
                    if (((int)caalen +2) > rdlen) {
                         break;
                    }
                    tuple_member_create_dep_string(rrec, member,
                                                   proc->label_caatag,
                                                   (char *)(rdbuf + 2), caalen);
                    uint8_t * remainder = rdbuf + 2 + caalen;
                    int remainderlen = rdlen - 2 - caalen;

                    if (remainderlen) {
                         tuple_member_create_dep_string(rrec, member,
                                                        proc->label_caavalue,
                                                        (char *)(remainder),
                                                        remainderlen);
                    }
                    break;
               case 15: //MX
                    {
                         tuple_add_member_label(tdata, rrec, proc->label_mx);
                         if (rdlen < 3) {
                              break;
                         }
                         uint16_t mxpref = (rdbuf[0]<<8) + rdbuf[1];
                         tuple_member_create_uint16(rrec, mxpref,
                                                    proc->label_mxpref);
                         rdbuf+=2;
                         rdlen-=2;
                         extract_rdname(rrec, proc->label_mx,
                                        rdbuf, rdlen, entire, entirelen);
                    break;
                    }
               case 6: //SOA
                    {
                         tuple_add_member_label(tdata, rrec, proc->label_soa);
                         int nameoffset = extract_rdname(rrec, proc->label_soa,
                                                         rdbuf, rdlen, entire, entirelen);
                         if (nameoffset <= 0) {
                              break;
                         }
                         if (nameoffset >= rdlen) {
                              break;
                         }
                         rdbuf += nameoffset;
                         rdlen -= nameoffset;
                         nameoffset = extract_rdname(rrec, proc->label_mailbox,
                                                     rdbuf, rdlen, entire, entirelen);
                         if (nameoffset <= 0) {
                              break;
                         }
                         if (nameoffset >= rdlen) {
                              break;
                         }
                         rdbuf += nameoffset;
                         rdlen -= nameoffset;

                         if (rdlen != 20) {
                              dprint("unusual soa length %d", rdlen);
                              break;
                         }
                         uint32_t serial = ntohl(*(uint32_t*)(rdbuf + 0));
                         uint32_t refresh = ntohl(*(uint32_t*)(rdbuf + 4));
                         uint32_t retry = ntohl(*(uint32_t*)(rdbuf + 8));
                         uint32_t expire = ntohl(*(uint32_t*)(rdbuf + 12));
                         uint32_t minimum = ntohl(*(uint32_t*)(rdbuf + 16));
                         tuple_member_create_uint(rrec, serial, proc->label_serial);
                         tuple_member_create_uint(rrec, refresh, proc->label_refresh);
                         tuple_member_create_uint(rrec, retry, proc->label_retry);
                         tuple_member_create_uint(rrec, expire,
                                                  proc->label_expire);
                         tuple_member_create_uint(rrec, minimum,
                                                  proc->label_minimum);
                         break;
                    }
               default:
                    if (rdlen) {
                         tuple_member_create_dep_binary(rrec, member, proc->label_rdata,
                                                        (char *)rdbuf, rdlen);
                    }
               }
          }

     }

     return offset;
}

static inline void local_add_label(wsdata_t * base, wsdata_t * tdata, wslabel_t * label) {
     if (base) {
          tuple_add_member_label(base, tdata, label);
     }
     else {
          wsdata_add_label(tdata, label);
     }
}

int procbuffer_decode(void * vproc, wsdata_t * tdata,
                      wsdata_t * member, uint8_t * buf, int buflen) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     wsdata_t * base = NULL;

     if (proc->label_parent) {
          wsdata_t * parent =
               tuple_member_create_wsdata(tdata, dtype_tuple, proc->label_parent);

          if (parent) {
               base = tdata;
               tdata = parent;
          }
     }

     if (proc->tcpdns) {
          if (buflen < 2) {
               local_add_label(base, tdata, proc->label_invalidtcpdns);
               return 1;
          }
          uint16_t tcpbuflen = (buf[0]<<8) + buf[1];
          tuple_member_create_uint16(tdata, tcpbuflen, proc->label_tcpdnslen);
          if (buflen + 2 < tcpbuflen) {
               local_add_label(base, tdata, proc->label_invalidtcpdns);
               dprint("truncated dns");
          }
          //force offset
          buflen-=2;
          buf+=2;
     }

     if (buflen < sizeof(dnsheader_t)) {
          dprint("invalid header");
          local_add_label(base, tdata, proc->label_invaliddns);
          return 1;
     }
     dnsheader_t * dhead = (dnsheader_t *)buf;


     tuple_member_create_uint16(tdata, ntohs(dhead->id), proc->label_id);
     dprint("bits:  %02x %02x, %02x %02x", dhead->code1, dhead->code2, buf[2], buf[3]);

     uint8_t qr = (dhead->code1 >> 7) & 0x1;
     uint8_t opcode = (dhead->code1 >> 3) & 0xF;
     uint8_t aa = (dhead->code1 >> 2) & 0x1;
     uint8_t tc = (dhead->code1 >> 1) & 0x1;
     uint8_t rd = dhead->code1 & 0x1;
     uint8_t ra = (dhead->code2 >> 7) & 0x1;
     uint8_t z = (dhead->code2 >> 4) & 0x7;
     uint8_t rcode = dhead->code2 & 0xF;

     if (qr == 0) {
          local_add_label(base, tdata, proc->label_query);
     }
     else {
          local_add_label(base, tdata, proc->label_response);
     }

     dprint("decode %x %04x %x %x %x %x %03x %04x", qr, opcode, aa,
            tc, rd, ra, z, rcode);

     tuple_member_create_uint16(tdata, opcode, proc->label_opcode);
                                
     if (aa) {
          local_add_label(base, tdata, proc->label_aa);
     }
     if (tc) {
          local_add_label(base, tdata, proc->label_trunc);
     }
     if (rd) {
          local_add_label(base, tdata, proc->label_rd);
     }
     if (ra) {
          local_add_label(base, tdata, proc->label_ra);
     }
     if (z) {
          tuple_member_create_uint16(tdata, z, proc->label_z);
     }
     if (rcode) {
          wsdata_t * rcd = tuple_member_create_uint16(tdata, rcode, proc->label_rcode);
          if (rcode == 3) {
               local_add_label(base, tdata, proc->label_nx);
               tuple_add_member_label(tdata, rcd, proc->label_nx);
          }
     }

     uint16_t qdcount = ntohs(dhead->qdcount);
     uint16_t ancount = ntohs(dhead->ancount);
     uint16_t nscount = ntohs(dhead->nscount);
     uint16_t arcount = ntohs(dhead->arcount);


     //TODO - check if counts are sane
     int recmin = ((int)qdcount * MIN_QREC) + 
          (((int)ancount + (int)nscount + (int)arcount) * MIN_RREC);

     //prepare rrec processing
     uint8_t * remain = buf + sizeof(dnsheader_t);
     int remainlen = buflen - sizeof(dnsheader_t);

     if (remainlen < recmin) {
          local_add_label(base, tdata, proc->label_invaliddns);
          //truncated record detection..
          return 1;
     }

     tuple_member_create_uint16(tdata, qdcount, proc->label_qdcount);
     tuple_member_create_uint16(tdata, ancount, proc->label_ancount);
     tuple_member_create_uint16(tdata, nscount, proc->label_nscount);
     tuple_member_create_uint16(tdata, arcount, proc->label_arcount);

     //now start processing queries and resource records..

   
     int rlen = 0; 
     if (qdcount) {
          rlen = process_query(proc, tdata, member, qdcount, remain,
                                   remainlen, buf, buflen);
          if (rlen <= 0) { 
               dprint("exit early from process_query");
               return 1;
          }
          if (rlen >= remainlen) {
               return 1;
          }
          remain += rlen;
          remainlen -= rlen;
     }
     
     if (ancount) {
          rlen = process_rrec(proc, tdata, member, ancount, remain, remainlen, buf,
                              buflen, proc->label_anrec);
          if (rlen <= 0) { 
               dprint("exit early from anrec process_rrec");
               return 1;
          }
          if (rlen >= remainlen) {
               return 1;
          }
          remain += rlen;
          remainlen -= rlen;
     }
     if (nscount) {
          rlen = process_rrec(proc, tdata, member, nscount, remain, remainlen, buf,
                              buflen, proc->label_nsrec);
          if (rlen <= 0) { 
               dprint("exit early from nsrec process_rrec");
               return 1;
          }
          if (rlen >= remainlen) {
               return 1;
          }
          remain += rlen;
          remainlen -= rlen;
     }
     if (arcount) {
          rlen = process_rrec(proc, tdata, member, arcount, remain, remainlen, buf,
                              buflen, proc->label_arrec);
          if (rlen <= 0) { 
               dprint("exit early from arrec process_rrec");
               return 1;
          }
          if (rlen >= remainlen) {
               return 1;
          }
          remain += rlen;
          remainlen -= rlen;
     }
          
     return 1;
}

