/*

proc_pcapin.c -- pcap source from network interface or file
   
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
#define PROC_NAME "pcapin"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/ip6.h>
#include <netinet/tcp.h>
#include <netinet/if_ether.h>
#include <net/ethernet.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>
#include <pcap.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "timeparse.h"
#include "wstypes.h"
#include "datatypes/wsdt_tuple.h"
#include "mimo.h"
#include "evahash64.h"
#include "wstuple_ip.h"

char proc_name[]	= PROC_NAME;
char *proc_tags[]	= { "input", NULL };
char proc_purpose[]	= "Read pcap from files or interface";
char *proc_synopsis[]	= {"pcapin <BPF> [-i interface]", NULL};
char proc_description[]	= "Reads in packet data from file or interface.\n" \
                            "If no iterface is specified, it will read from pcap from stdin";
proc_example_t proc_examples[]	= {
     {NULL, NULL}
};
char *proc_alias[]  = {"pcap", "pcap_in", NULL};
char proc_version[] = "1.0";
char proc_requires[]     = "";
char *proc_input_types[]	= {"tuple", NULL};
char *proc_output_types[]     = {"tuple", NULL};
proc_port_t proc_input_ports[]     = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[]   = {NULL};
char *proc_tuple_member_labels[]	= {NULL};
proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
         "option description", <allow multiple>, <required>*/
     {'i',"","interface",
     "read from interface",0,0},
     {'r',"","file",
     "read a single file ('-' for stdin)",0,0},
     {'g',"","",
     "label CLIENT or SERVER based on low port",0,0},
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]	= "specify a BPF packet filter";

#define DEFAULT_SNAPLEN (16384)
#define RING_BUFFER_SIZE (256*DEFAULT_SNAPLEN)

// processing module instance data structure
typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t badline_cnt;
     uint64_t outcnt;

     char error_buffer[PCAP_ERRBUF_SIZE];
     pcap_t * handler;
     char * iface_name;
     char * bpf_string;
     struct bpf_program bpfp;
	int bpf_compiled;
     int linktype;
     uint32_t link_layer_length;

     wslabel_t * label_datetime;
     wslabel_t * label_pcap;
     wslabel_t * label_pkt;
     wslabel_t * label_pktlen;
     wslabel_t * label_origpktlen;
     wslabel_t * label_srcmac;
     wslabel_t * label_dstmac;
     wslabel_t * label_ethtype;
     wslabel_t * label_ip_version;
     wslabel_t * label_ip_length;
     wslabel_t * label_ip_proto;
     wslabel_t * label_ip_src;
     wslabel_t * label_ip_dst;
     wslabel_t * label_ip_ttl;
     wslabel_t * label_ipv4;
     wslabel_t * label_ipv6;
     wslabel_t * label_tcp;
     wslabel_t * label_udp;
     wslabel_t * label_icmp;
     wslabel_t * label_srcport;
     wslabel_t * label_dstport;
     wslabel_t * label_content;
     wslabel_t * label_contentlen;
     wslabel_t * label_biflow;
     wslabel_t * label_dirflow;
     wslabel_t * label_flags;
     wslabel_t * label_syn;
     wslabel_t * label_synack;
     wslabel_t * label_ack;
     wslabel_t * label_fin;
     wslabel_t * label_rst;
     wslabel_t * label_push;
     wslabel_t * label_urgent;
     wslabel_t * label_truncated;
     wslabel_t * label_tcpseq;
     wslabel_t * label_tcpack;
     wslabel_t * label_empty;
     wslabel_t * label_client;
     wslabel_t * label_server;
     wslabel_t * label_tcpmss;
     ws_outtype_t * outtype_tuple;

     int guess_server;

     char * single_filename;
     int done_multifile;
     int multifile;
	char * filename;
} proc_instance_t;

//function prototypes for local functions
static int proc_cmd_options(int, char **, proc_instance_t *, void *);
// file
// stdin and list of files
static int data_source(void *, wsdata_t*, ws_doutput_t*, int);

#define PCAP_FILENAME_MAX 1024
static int read_stdin_filename(proc_instance_t * proc) {
	char buf[PCAP_FILENAME_MAX +1];
	int len;

	if (proc->handler) {
		//close
		pcap_close(proc->handler);
		proc->handler = NULL;
	}

	while (fgets(buf, PCAP_FILENAME_MAX, stdin)) {
          dprint("buf: %s", buf);
          //strip return
          len = strlen(buf);
          if (buf[len - 1] == '\n') {
               buf[len - 1] = '\0';
               len--;
          }
          proc->filename = buf; // includes null terminator
          dprint("proc->filename: %s", proc->filename);
          proc->handler = pcap_open_offline(proc->filename, proc->error_buffer);

		if (proc->handler) {
               tool_print("opened pcap file %s", buf);
			if (proc->bpf_compiled) {
				if (pcap_setfilter(proc->handler, &proc->bpfp) == -1) {
					fprintf(stderr, "Couldn't install filter %s: %s\n",
						   proc->bpf_string, pcap_geterr(proc->handler));
				}
			}

               return 1;
		}
		else {
			tool_print("ERROR opening pcap file %s", proc->error_buffer);
		}
     }
     proc->done_multifile = 1;
	return 0;

}
// The following is a function to take in command arguments and initalize
// this processor's instance, and is called only once for each instance of this
// processor.
// also register as a source here..
// return 1 if ok
// return 0 if fail
int proc_init(wskid_t * kid, int argc, char ** argv, void ** vinstance, 
              ws_sourcev_t * sv, void * type_table) {

     //allocate proc instance of this processor
     proc_instance_t * proc =
          (proc_instance_t*)calloc(1,sizeof(proc_instance_t));
     *vinstance = proc;

     proc->label_datetime = wsregister_label(type_table, "DATETIME");
     proc->label_pcap = wsregister_label(type_table, "PCAP");
     proc->label_pkt = wsregister_label(type_table, "PACKET");
     proc->label_truncated = wsregister_label(type_table, "TRUNCATED");
     proc->label_pktlen = wsregister_label(type_table, "PACKETLEN");
     proc->label_origpktlen = wsregister_label(type_table, "ORIGPACKETLEN");
     proc->label_srcmac = wsregister_label(type_table, "SRCMAC");
     proc->label_dstmac = wsregister_label(type_table, "DSTMAC");
     proc->label_ethtype = wsregister_label(type_table, "ETHTYPE");
     proc->label_ip_version = wsregister_label(type_table, "IP_VERSION");
     proc->label_ip_length = wsregister_label(type_table, "IP_LENGTH");
     proc->label_ip_proto = wsregister_label(type_table, "IP_PROTO");
     proc->label_ip_src = wsregister_label(type_table, "SRCIP");
     proc->label_ip_dst = wsregister_label(type_table, "DSTIP");
     proc->label_ip_ttl = wsregister_label(type_table, "IPTTL");
     proc->label_ipv4 = wsregister_label(type_table, "IPV4");
     proc->label_ipv6 = wsregister_label(type_table, "IPV6");
     proc->label_tcp = wsregister_label(type_table, "TCP");
     proc->label_udp = wsregister_label(type_table, "UDP");
     proc->label_icmp = wsregister_label(type_table, "ICMP");
     proc->label_srcport = wsregister_label(type_table, "SRCPORT");
     proc->label_dstport = wsregister_label(type_table, "DSTPORT");
     proc->label_content = wsregister_label(type_table, "CONTENT");
     proc->label_contentlen = wsregister_label(type_table, "CONTENTLEN");
     proc->label_biflow = wsregister_label(type_table, "BIFLOW");
     proc->label_dirflow = wsregister_label(type_table, "DIRFLOW");
     proc->label_flags = wsregister_label(type_table, "FLAGS");
     proc->label_syn = wsregister_label(type_table, "SYN");
     proc->label_synack = wsregister_label(type_table, "SYNACK");
     proc->label_ack = wsregister_label(type_table, "ACK");
     proc->label_fin = wsregister_label(type_table, "FIN");
     proc->label_rst = wsregister_label(type_table, "RST");
     proc->label_push = wsregister_label(type_table, "PUSH");
     proc->label_urgent = wsregister_label(type_table, "URG");
     proc->label_tcpseq = wsregister_label(type_table, "TCPSEQ");
     proc->label_tcpack = wsregister_label(type_table, "TCPACK");
     proc->label_empty = wsregister_label(type_table, "NOCONTENT");
     proc->label_client = wsregister_label(type_table, "CLIENT");
     proc->label_server = wsregister_label(type_table, "SERVER");
     proc->label_tcpmss = wsregister_label(type_table, "TCPMSS");

     int checkfilter = 0;

     // initialize flags and variables
     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (proc->iface_name) {
          proc->handler = pcap_create(proc->iface_name, proc->error_buffer);

          //pcap_set_rfmon(handler, 1);  //capture wifi packets more broadly
          pcap_set_promisc(proc->handler, 1); /* Capture packets that are not yours */
          pcap_set_snaplen(proc->handler, DEFAULT_SNAPLEN); /* Snapshot length */
          pcap_set_timeout(proc->handler, 1000); /* Timeout in milliseconds */
          checkfilter = 1;
          
     }
     else if (proc->single_filename) {
          tool_print("reading from file %s", proc->single_filename);
          proc->handler = pcap_open_offline(proc->single_filename, proc->error_buffer);
          //tool_print("interface name not specified - Error");
          //return 0;
          checkfilter = 1;
     }
     else {
          proc->multifile = 1;
          if (!read_stdin_filename(proc)) {
               return 0;
          }
     }
     if (!proc->handler) {
          tool_print("ERROR %s", proc->error_buffer);
          return 0;
     }

     
     if(proc->iface_name) {
          if ((pcap_set_buffer_size(proc->handler, RING_BUFFER_SIZE))!=0) {
               printf("ERROR on pcap set buffer size\n");
               exit(-1);
          }

          int ret = pcap_activate(proc->handler);  //start collection
          if (ret) {
               pcap_perror(proc->handler, "pcapin activate");
               tool_print("ERROR on pcap activation %d\n", ret);
               exit(-1);
          }
     }
     if (checkfilter && proc->bpf_string)  {
          if (pcap_compile(proc->handler, &proc->bpfp, proc->bpf_string, 0, PCAP_NETMASK_UNKNOWN) == -1) {
               fprintf(stderr, "Couldn't parse filter %s: %s\n",
                       proc->bpf_string, pcap_geterr(proc->handler));
               return(2);
          }
		else {
			proc->bpf_compiled = 1;
		}
          if (pcap_setfilter(proc->handler, &proc->bpfp) == -1) {
               fprintf(stderr, "Couldn't install filter %s: %s\n",
                  proc->bpf_string, pcap_geterr(proc->handler));
               return(2);
          }
     }


     proc->linktype = pcap_datalink(proc->handler);
     switch(proc->linktype) {
     case DLT_EN10MB:
          proc->link_layer_length = sizeof(struct ether_header);
          break;
     default:
          tool_print("setting no link layer %d", proc->linktype);
          proc->link_layer_length = 0;
     }

     /*if (pcap_setnonblock(handler, 1, error_buffer) != 0) {
       pcap_perror(handler, "setnonblock");
       printf("ERROR on nonblock %s", error_buffer);
       }*/

     // TODO: If no input stream is given, we should gracefully exit ...
     
     // register sources, set output types
     proc->outtype_tuple = 
	     ws_register_source_byname(type_table, "TUPLE_TYPE", data_source, sv);

     if (!proc->outtype_tuple) {
          fprintf(stderr, "waterslide source registration failed\n");
          return 0;
     }

     return 1; 
}

// Process options from command line
static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;
     // qty of labels provided on cmd line for members parsed from input events

     while ((op = getopt(argc, argv, "r:gGi:")) != EOF) {
          switch (op) {
          case 'r':
               proc->single_filename = strdup(optarg);
               break;
          case 'g':
          case 'G':
               proc->guess_server = 1;
               break;
          case 'i': // use first element in event as container label
               proc->iface_name = strdup(optarg);
               break;
          default:
               return 0;
          }
     }
     // store all remaining command line tokens as labels for members parsed
     // from the events received as input
     while (optind < argc) {
          // proc->lset is limited to 128 by WSMAX_LABEL_SET defined in waterslide.h
          int olen = strlen(argv[optind]);
          if (proc->bpf_string) {
               int blen = strlen(proc->bpf_string);
               proc->bpf_string = realloc(proc->bpf_string, olen + blen + 2);
               proc->bpf_string[blen] = ' ';
               memcpy(proc->bpf_string + blen + 1, argv[optind], olen);
               proc->bpf_string[blen + olen + 1] = 0;
          }
          else {
               proc->bpf_string = strdup(argv[optind]);
          }

	  //TODO aggregate bpf into single string
          optind++;
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
     return NULL;
}

static inline int add_ipv6(wsdata_t * tdata,
                           void * ipbuf, wslabel_t* label) {
     char out[INET6_ADDRSTRLEN];
     const char* result = inet_ntop(AF_INET6, ipbuf, out, sizeof(out));
     if (!result) {
          return 0;
     }
     tuple_dupe_string(tdata, label, result, strlen(result));
     return 1;
}


static void add_flow_hashes(proc_instance_t * proc, wsdata_t * tdata, void * ipaddr,
                            int ipaddrlen, uint16_t srcport, uint16_t dstport,
                            uint8_t ipproto) {

     uint8_t base[(INET6_ADDRSTRLEN * 2) + 4 + 1];

     int addroffset = ipaddrlen/2;
     void * srcaddr = ipaddr;
     void * dstaddr = ipaddr + addroffset;

     int dir = 0;  //client to server
     if (srcport > dstport) {
          dir = 1;
     }
     else if ((srcport == dstport) && 
              (memcmp(srcaddr, dstaddr, addroffset) > 0) ) {
          dir = 1;
     }

     int blen = ipaddrlen + 5;

     memcpy(base, ipaddr, ipaddrlen);
     memcpy(base + ipaddrlen, &srcport, 2);
     memcpy(base + ipaddrlen+2, &dstport, 2);
     base[ipaddrlen + 4] = ipproto;
    
     //hash base.. 
     uint64_t hash = evahash64(base, blen, 0xDEADBEEF);
     tuple_member_create_uint64(tdata, hash,
                                proc->label_dirflow);

     if (dir) {
          memcpy(base, dstaddr, addroffset);
          memcpy(base + addroffset, srcaddr, addroffset);
          memcpy(base + ipaddrlen, &dstport, 2);
          memcpy(base + ipaddrlen+2, &srcport, 2);
          hash = evahash64(base, blen, 0xDEADBEEF);
     }
     tuple_member_create_uint64(tdata, hash,
                                proc->label_biflow);

}

static int parse_udp(proc_instance_t * proc, wsdata_t * tdata,
                     char * buf, uint16_t buflen, uint16_t udpremain,
                     void * ipaddr, int ipaddrlen,
                     wsdata_t * wspkt) {
     if (buflen < 8) {
          return 0;
     }
     wsdata_add_label(tdata, proc->label_udp);

     uint16_t srcport = ntohs(*(uint16_t*)buf);
     uint16_t dstport = ntohs(*(uint16_t*)(buf+2));
     uint16_t ulen = ntohs(*(uint16_t*)(buf+4));
     tuple_member_create_uint16(tdata, srcport,
                                proc->label_srcport);
     tuple_member_create_uint16(tdata, dstport,
                                proc->label_dstport);

     if (proc->guess_server) {
          if (srcport <= dstport) {
               wsdata_add_label(tdata, proc->label_server);
          }
          else {
               wsdata_add_label(tdata, proc->label_client);
          }
     }

     add_flow_hashes(proc, tdata, ipaddr, ipaddrlen, srcport, dstport,
                     IPPROTO_UDP);
     if (ulen < 8) {
          return 0;
     }
     uint16_t clen = ulen - 8;
     tuple_member_create_uint16(tdata, clen,
                                proc->label_contentlen);

     if (clen == 0) {
          dprint("empty");
          wsdata_add_label(tdata, proc->label_empty);
     }
     else if (buflen > 8) {
          uint16_t remain = buflen - 8; 
          if (clen <= remain) {
               tuple_member_create_dep_binary(tdata, wspkt, proc->label_content,
                                              buf+8, clen);
          }
          else {
               //display truncated data
               tuple_member_create_dep_binary(tdata, wspkt, proc->label_content,
                                              buf+8, remain);
          }
     }
     return 1;
}
static int parse_tcp(proc_instance_t * proc, wsdata_t * tdata,
                     char * buf, uint16_t buflen, uint16_t tcpremain,
                     void * ipaddr, int ipaddrlen,
                     wsdata_t * wspkt) {

     dprint("parse_tcp %d %d", buflen, ipaddrlen);
     if (buflen < 20) {
          return 0;
     }

     wsdata_add_label(tdata, proc->label_tcp);

     struct tcphdr * thead = (struct tcphdr *)buf;

     uint16_t srcport = ntohs(thead->th_sport);
     uint16_t dstport = ntohs(thead->th_dport);
     tuple_member_create_uint16(tdata, srcport,
                                proc->label_srcport);
     tuple_member_create_uint16(tdata, dstport,
                                proc->label_dstport);
     tuple_member_create_uint(tdata, ntohl(thead->th_seq),
                              proc->label_tcpseq);
     tuple_member_create_uint(tdata, ntohl(thead->th_ack),
                              proc->label_tcpack);
     tuple_member_create_uint16(tdata, thead->th_flags,
                                proc->label_flags);

     if (proc->guess_server) {
          if (srcport <= dstport) {
               wsdata_add_label(tdata, proc->label_server);
          }
          else {
               wsdata_add_label(tdata, proc->label_client);
          }
     }

     if (thead->th_flags & TH_SYN) {
          if (thead->th_flags & TH_ACK) {
               wsdata_add_label(tdata, proc->label_synack);
          }
          else {
               wsdata_add_label(tdata, proc->label_syn);
          }
     }
     else if (thead->th_flags & TH_ACK) {
          wsdata_add_label(tdata, proc->label_ack);
     }
     if (thead->th_flags & TH_FIN) {
          wsdata_add_label(tdata, proc->label_fin);
     }
     if (thead->th_flags & TH_RST) {
          wsdata_add_label(tdata, proc->label_rst);
     }
     if (thead->th_flags & TH_PUSH) {
          wsdata_add_label(tdata, proc->label_push);
     }
     if (thead->th_flags & TH_URG) {
          wsdata_add_label(tdata, proc->label_urgent);
     }

     //flow hash
     add_flow_hashes(proc, tdata, ipaddr, ipaddrlen, srcport, dstport, IPPROTO_TCP);

     uint16_t tcphlen = thead->th_off * 4;
     dprint("tcp buf %d, tcphlen %u", buflen, tcphlen);
     if (tcphlen < 20) {
          return 0;
     }
     if (tcphlen > buflen) {
          return 0;
     }

     if (tcphlen > 20) {
          //ok we have options
          int remainder = tcphlen - 20;
          uint8_t * optbuf = (uint8_t*)buf + 20;
          while(remainder > 0) {
               switch(optbuf[0]) {
               case 1: // NOOP
                    optbuf++;
                    remainder--;
                    break;
               case 2:
                   if (remainder >= 4) {
                        if (optbuf[1] == 4) { 
                             uint16_t mss = (optbuf[2]<<8) + optbuf[3];
                             tuple_member_create_uint16(tdata, mss,
                                                        proc->label_tcpmss);
                        }
                        remainder -= optbuf[1];
                        optbuf += optbuf[1];
                   }
                   else {
                        remainder = 0;
                   }
                   break;
               default:
                   if (remainder >= 2) {
                        if (optbuf[1] && (optbuf[1] <= remainder)) {
                             remainder -= optbuf[1];
                             optbuf += optbuf[1];
                        }
                        else {
                             remainder = 0;
                        }
                   }
                   else {
                        remainder = 0;
                   }
               }
          }

     }

     uint16_t clen = buflen - tcphlen;
     if (buflen < tcpremain) {
          //display true length rather than truncated length
          tuple_member_create_uint16(tdata, tcpremain - tcphlen,
                                     proc->label_contentlen);
     }
     else {
          tuple_member_create_uint16(tdata, clen,
                                     proc->label_contentlen);
     }

     if (clen == 0) {
          wsdata_add_label(tdata, proc->label_empty);
     }
     else {
          tuple_member_create_dep_binary(tdata, wspkt, proc->label_content,
                                         buf+tcphlen, clen);
     }

     return 1;     
}

static int parse_ipv6(proc_instance_t * proc, wsdata_t * tdata,
                     char * buf, int buflen, wsdata_t * wspkt) {
     if (buflen < 40) {
          return 0;
     }
     wsdata_add_label(tdata, proc->label_ipv6);
     struct ip6_hdr * iphead = (struct ip6_hdr *)buf;

     uint16_t plen = ntohs(iphead->ip6_plen);
     tuple_member_create_uint16(tdata,
                                plen + 40,
                                proc->label_ip_length);
     tuple_member_create_uint16(tdata, 
                                iphead->ip6_nxt,
                                proc->label_ip_proto);
     tuple_add_ipv6(tdata, &iphead->ip6_src, proc->label_ip_src);
     tuple_add_ipv6(tdata, &iphead->ip6_dst, proc->label_ip_dst);

     tuple_member_create_uint(tdata,
                              iphead->ip6_hlim,
                              proc->label_ip_ttl);
     int remainder;
     if ((buflen - 40) < plen) {
          wsdata_add_label(tdata, proc->label_truncated);
          dprint("buflen = %d, plen = %u", buflen, plen);
          remainder = buflen - 40;
     }
     else {
          remainder = plen;
     }
     if (!remainder) {
          return 0;
     }
     char * next = buf + 40;

     //parse next header .... TODO
     switch(iphead->ip6_nxt) {
     case IPPROTO_TCP:
          parse_tcp(proc, tdata, next, remainder, plen, &iphead->ip6_src, 32, wspkt);
          break;
     case IPPROTO_UDP:
          parse_udp(proc, tdata, next, remainder, plen, &iphead->ip6_src, 32, wspkt);
          break;
     default:
          //nothing
          break;
     }

     return 1;
}

static int parse_ip(proc_instance_t * proc, wsdata_t * tdata,
                     char * buf, int buflen, wsdata_t * wspkt) {
     if (buflen < 20) {
          return 0;
     }
     struct ip * iphead = (struct ip *)buf;

     if ((iphead->ip_v == 4) || (iphead->ip_v == 6)) {
          tuple_member_create_uint16(tdata, iphead->ip_v,
                                     proc->label_ip_version);
     }
     if (iphead->ip_v == 6) {
          return parse_ipv6(proc, tdata, buf, buflen, wspkt);
     }

     wsdata_add_label(tdata, proc->label_ipv4);
     uint16_t iplen = ntohs(iphead->ip_len);
     uint16_t iphlen = iphead->ip_hl * 4;
     
     tuple_member_create_uint16(tdata,
                                iplen,
                                proc->label_ip_length);
     tuple_member_create_uint16(tdata, iphead->ip_p,
                                proc->label_ip_proto);
     tuple_member_create_uint(tdata,
                              iphead->ip_ttl,
                              proc->label_ip_ttl);
     tuple_add_ipv4(tdata, &iphead->ip_src, proc->label_ip_src);
     tuple_add_ipv4(tdata, &iphead->ip_dst, proc->label_ip_dst);

     dprint("len issues buf %d, ip %d, iph %d", buflen, iplen, iphlen);

     if ((iphlen > iplen) || (buflen < iphlen)) {
          //unexpected pkt
          return 0;
     }
     uint16_t remainder;
     uint16_t dremain = iplen - iphlen;
     if (buflen < iplen) {
          remainder = buflen - iphlen;
          wsdata_add_label(tdata, proc->label_truncated);
     }
     else {
          remainder = iplen - iphlen;
     }
     if (!remainder) {
          dprint("len issues %d %d", iplen, iphlen);
          return 0;
     }
     char * next = buf + iphlen;

     switch(iphead->ip_p) {
     case IPPROTO_TCP:
          parse_tcp(proc, tdata, next, remainder, dremain, &iphead->ip_src, 8, wspkt);
          break;
     case IPPROTO_UDP:
          parse_udp(proc, tdata, next, remainder, dremain, &iphead->ip_src, 8, wspkt);
          break;
     default:
          //nothing
          break;
     }

     return 1;
} 

// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
//
static int data_source(void * vinstance, wsdata_t* source_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     struct pcap_pkthdr * pkthdr = NULL;
     struct ether_header * etherhdr = NULL;
     uint8_t * pktdata = NULL;
     uint16_t ether_type = 0;
     uint32_t pktremain = 0;
     int val = pcap_next_ex(proc->handler, &pkthdr, (const uint8_t **)&pktdata);

     if (val != 1) {
          if (proc->iface_name) {
               //no packet yet..
               return 1;
          }
          else if (proc->multifile) {
			if (!read_stdin_filename(proc)) {
				return 0;
			}
               else {
                    return 1;
               }
		}
          else {
               return 0; //single file
          }
     }
     if (!pkthdr || !pktdata) {
          error_print("unexpected lack of packet and/or headers");
          return 1;
     }

     wsdt_ts_t ts;
     ts.sec = pkthdr->ts.tv_sec;
     ts.usec = pkthdr->ts.tv_usec;
     tuple_member_create_ts(source_data, ts, proc->label_datetime);

     uint32_t caplen = pkthdr->caplen;

     wsdata_t * capd = tuple_member_create_uint(source_data, caplen, proc->label_pktlen);
     if (pkthdr->caplen == pkthdr->len) {
          tuple_add_member_label(source_data, capd, proc->label_origpktlen);
     }
     else {
          tuple_member_create_uint(source_data, pkthdr->len, proc->label_origpktlen);
     }

     wsdata_t * wspkt = tuple_dupe_binary(source_data, proc->label_pkt,
                                          (char *)pktdata, caplen);

     if (!wspkt) {
          error_print("unable to allocate pkt data");
          return 1;
     }
     wsdata_add_label(source_data, proc->label_pcap);
     wsdt_binary_t * binpkt = (wsdt_binary_t *)wspkt->data;

     switch(proc->linktype) {
     case DLT_EN10MB:
          if (pkthdr->caplen > proc->link_layer_length) {
               etherhdr = (struct ether_header*)pktdata;
               tuple_member_create_dep_binary(source_data, wspkt, proc->label_dstmac,
                                       (char *)etherhdr->ether_dhost, ETHER_ADDR_LEN);
               tuple_member_create_dep_binary(source_data, wspkt, proc->label_srcmac,
                                       (char *)etherhdr->ether_shost, 6);

               ether_type = ntohs(etherhdr->ether_type);
               tuple_member_create_uint16(source_data, ether_type,
                                              proc->label_ethtype);
          }
          else {
               //TODO label packet as invalid
          }
     case DLT_RAW:
          if (pkthdr->caplen >= 20) {
               if (pktdata[0] == 0x45) {
                    ether_type = ETHERTYPE_IP;
               }
               else if ((pktdata[0] & 0xF0) == 0x60) {
                    ether_type = ETHERTYPE_IPV6;
               }
          }
          break;
     default:
          //TODO Something wiht other linktypes
          break;
     }

     if (pkthdr->caplen > proc->link_layer_length) {
          pktremain = pkthdr->caplen - proc->link_layer_length;

          //check if IP is next
          switch (ether_type) {
               case ETHERTYPE_IP:
                   parse_ip(proc, source_data,
                            binpkt->buf + proc->link_layer_length, pktremain, wspkt);
                   break;
               case ETHERTYPE_IPV6:
                   parse_ipv6(proc, source_data, binpkt->buf +
                              proc->link_layer_length, pktremain, wspkt);
                   break;
               default:
                   //TODO Something with other ether types
                   dprint("link next %x", ether_type);
                   break;
          }
     }
     ws_set_outdata(source_data, proc->outtype_tuple, dout);
     proc->meta_process_cnt++;

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("badline cnt %" PRIu64, proc->badline_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);


     //free dynamic allocations
     free(proc);

     return 1;
}

