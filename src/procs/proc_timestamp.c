/* 
proc_timestamp.c - add a timestamp (current GMT time) to a tuple

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

#define PROC_NAME "timestamp"

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <math.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_ts.h"
#include "wstypes.h"

#define LOCAL_MAX_TYPES 50

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
char *proc_tags[]              =  { "Source", NULL };
char *proc_alias[]             =  { "addtime", "time_add", "datetime", "add_time", "timeadd",  NULL };
char proc_purpose[]            =  "add current timestamp to tuple";
proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     //the following must be left as-is to signify the end of the array
     {'U',"","",
     "output uint64 as milli seconds from 1970",0,0},
     {'o',"","sec",
     "offset from wall clock time",0,0},
     {'L',"","label",
     "label of timestamp (default: DATETIME)",0,0},
     {'S',"","",
     "output as string",0,0},
     {'z',"","",
     "output as iso1806 string with +00:00 UTC format",0,0},
     {'e',"","",
     "output as epoch double",0,0},
     {'u',"","",
     "output as epoch integer",0,0},
     {'P',"","timespec",
     "parse input string as UTC timestamp per specification",0,0},
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "<LABEL of epoch>";
char *proc_input_types[]       =  { "tuple", NULL };
// (Potential) Output types: flush, tuple, meta[LOCAL_MAX_TYPES]
char *proc_output_types[]      =  { "tuple", NULL };
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  { "add_time", NULL };


//function prototypes for local functions
static int proc_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_value_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;

     int usevalue;
     wslabel_nested_set_t nest;

     wslabel_t * label_datetime;
     ws_outtype_t * outtype_tuple;
     time_t offset;
     int as_uint64;
     int as_uint64milli;
     int as_epoch;
     int as_string;
     int altzulu;
     int as_parse;
     char * parse_string;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;

     while ((op = getopt(argc, argv, "P:eSzuUo:O:L:")) != EOF) {
          switch (op) {
          case 'P':
               proc->parse_string = strdup(optarg);
               proc->as_parse = 1;
               break;
          case 'e':
               proc->as_epoch = 1;
               break;
          case 'u':
               proc->as_uint64 = 1;
               break;
          case 'U':
               proc->as_uint64milli = 1;
               break;
          case 'o':
          case 'O':
               proc->offset = (time_t)atoi(optarg);
               tool_print("using offset %u seconds", (uint32_t)proc->offset);
               break;
          case 'L':
               proc->label_datetime = wssearch_label(type_table, optarg);
               break;
          case 'z':
               proc->altzulu = 1;
               proc->as_string = 1;
               break;
          case 'S':
               proc->as_string = 1;
               break;
               
          default:
               return 0;
          }
     }
     while (optind < argc) {
          //detect sublabels
          wslabel_nested_search_build(type_table, &proc->nest,
                                      argv[optind]);
          proc->usevalue = 1;
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


     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->label_datetime) {
          if (proc->as_uint64) {
               proc->label_datetime = wssearch_label(type_table, "MSEC");
          }
          else  {
               proc->label_datetime = wssearch_label(type_table, "DATETIME");
          }
     }
     return 1; 
}


proc_process_t proc_input_set(void * vinstance, wsdatatype_t * input_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype(olist, dtype_tuple, NULL);
     }
     if (input_type == dtype_tuple) {
          if (proc->usevalue) {
               return proc_value_tuple;
          } else{
               return proc_tuple;
          }
     }
     return NULL;
}

/*
wsdt_string_t * str = tuple_create_string(tdata, proc->label_filename, 48);
int llen = wsdt_snprint_ts_sec_usec(str->buf, 48, ts->sec, (unsigned int) ts->usec);
*/

//derived from datatypes/wsdt_ts.h wsdt_snprint_ts_sec_usec() function
static int snprint_alt_zulu(char * buf, int len,
                           time_t tsec, unsigned int usec) {
     int s;
     struct tm tdata;
     struct tm *tp;
     time_t stime;

     /* these variables are not currently in use
     static unsigned b_sec;
     static unsigned b_usec;
     */

     s = tsec % 86400;
     stime = tsec - s;
     tp = gmtime_r(&stime, &tdata);
     if (usec) {
          return snprintf(buf, len,"%04d-%02d-%02dT%02d:%02d:%02d.%06u+00:00",
                          tp->tm_year+1900,
                          tp->tm_mon+1, tp->tm_mday,
                          s / 3600, (s % 3600) / 60,
                          s % 60,
                          usec);
     }
     else {
          return snprintf(buf, len, "%04d-%02d-%02dT%02d:%02d:%02d+00:00",
                          tp->tm_year+1900,
                          tp->tm_mon+1, tp->tm_mday,
                          s / 3600, (s % 3600) / 60,
                          s % 60);
     }
}

static int add_ts_to_tuple(proc_instance_t * proc, wsdata_t *tdata,
                           time_t epochsec, unsigned int epochusec) {
     epochsec += proc->offset;
     dprint("%u %u", (unsigned int)epochsec, epochusec);
     
     if (proc->as_epoch) {
          double v  = (double)epochsec + ((double)epochusec/1000000.0);
          tuple_member_create_double(tdata, v, proc->label_datetime);
     }
     else if (proc->as_uint64) {
          tuple_member_create_uint64(tdata, epochsec, proc->label_datetime);
     }
     else if (proc->as_uint64milli) {
          uint64_t utime = ((uint64_t)epochsec * 1000) +
               ((uint64_t)epochusec / 1000);
          tuple_member_create_uint64(tdata, utime, proc->label_datetime);
     }
     else if (proc->as_string) {
          wsdt_string_t * str = tuple_create_string(tdata, proc->label_datetime, 48);
          if (str) {
               int llen;
               if (proc->altzulu) {
                    llen = snprint_alt_zulu(str->buf, 48, epochsec,
                                            epochusec);
               }
               else {
                    llen = wsdt_snprint_ts_sec_usec(str->buf, 48, epochsec,
                                                    epochusec);
               }
               str->len = llen;
          }
          else {
               return 0;
          }
     }
     else {
          wsdt_ts_t ts;
          ts.sec = epochsec;
          ts.usec = epochusec;

          tuple_member_create_ts(tdata, ts, proc->label_datetime);
     }
     return 1;
}

static int parse_time_string(proc_instance_t * proc, wsdata_t * tdata,
                             char * buf, int blen) {
     //create a null terminated string
     char * nstring = malloc(blen + 1);
     if (!nstring) {
          return 0;
     }
     memcpy(nstring, buf, blen);
     nstring[blen] = 0;

     struct tm tparse;
     strptime(nstring, proc->parse_string, &tparse);
     time_t sec = timegm(&tparse);
     tuple_member_create_sec(tdata, sec, proc->label_datetime);

     free(nstring);
     return 1;
}

static int nest_search_callback_match(void * vproc, void * vevent,
                                      wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vproc;
     if (member->dtype == dtype_ts) {
          wsdt_ts_t * ts = (wsdt_ts_t*)member->data;
          return add_ts_to_tuple(proc, tdata, ts->sec, ts->usec);
     }
     else if (proc->as_parse && (member->dtype == dtype_string)) {
          char * buf;
          int blen;
          if (dtype_string_buffer(member, &buf, &blen)) {
               return parse_time_string(proc, tdata, buf, blen);
          }
     }
     else if ((member->dtype == dtype_string) || (member->dtype == dtype_double)) {
          double epochd = 0;
          if (dtype_get_double(member, &epochd)) {
               time_t epochsec = (time_t)epochd;
               double intv;
               unsigned int epochusec = 
                    (unsigned int)(modf(epochd, &intv) * 1000000.0);
               return add_ts_to_tuple(proc, tdata, epochsec, epochusec);
          }

     }
     else {
          uint64_t epochuint = 0;
          if (dtype_get_uint(member, &epochuint)) {
               return add_ts_to_tuple(proc, tdata, epochuint, 0);
          }
     }
     return 0;

}
static int proc_value_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->meta_process_cnt++;

     //search nested tuple
     tuple_nested_search(input_data, &proc->nest,
                         nest_search_callback_match,
                         proc, NULL);

     proc->outcnt++;
     ws_set_outdata(input_data, proc->outtype_tuple, dout);

     return 1;
}

static int proc_tuple(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     struct timeval current;
     gettimeofday(&current, NULL);

     add_ts_to_tuple(proc, input_data, current.tv_sec, current.tv_usec);

     proc->outcnt++;
     ws_set_outdata(input_data, proc->outtype_tuple, dout);

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free dynamic allocations
     free(proc);

     return 1;
}

