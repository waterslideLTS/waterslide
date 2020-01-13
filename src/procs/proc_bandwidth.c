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
#define PROC_NAME "bandwidth"

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "procloader.h"

char proc_name[]               =  PROC_NAME;
char proc_version[]            =  "1.5";
// Use to fix proc_tags: 
//char *proc_menus[]             =  {}; 
char *proc_tags[]              =  { "Profiling", NULL };
char *proc_alias[]             =  { "bw", NULL };
char proc_purpose[]            =  "Measures the bandwidth of a data stream";
proc_option_t proc_opts[]      =  {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     //the following must be left as-is to signify the end of the array
     {'t',"","",
     "add timestamp of current time to output tuple",0,0},
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]     =  "";
char *proc_input_types[]       =  {"flush", "binary", "monitor", "any", NULL};
// (Potential) Output types: tuple
char *proc_output_types[]      =  { "None" };
char proc_requires[]           =  "";
// Ports: 
proc_port_t proc_input_ports[] =  {{NULL, NULL}};
char *proc_tuple_container_labels[] =  {NULL};
char *proc_tuple_conditional_container_labels[] =  {NULL};
char *proc_tuple_member_labels[] =  {NULL};
char *proc_synopsis[]          =  { "bandwidth", NULL };
proc_example_t proc_examples[] =  {
	{"... | bandwidth", "run bandwidth kid"},
        {NULL,""}
};
char proc_description[]    = "Used to measure the bandwidth of items in an event stream.  Specifically, this will return information on the total number of events processed," 
			     "how many seconds of processing occurred and how many thousand events per second were processed.  All of this information is given to stderr when the stream is" 
			     " completed (either because it has finished, received a flush from a call to the flush kid at some time interval, or because the program is cancelled via an interrupt such as Control-C).";

//function prototypes for local functions
static int proc_binary(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_monitor(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _proc_instance_t {
     uint64_t byte_cnt;
     uint64_t event_cnt;
     uint64_t outcnt;

     int do_timestamp;
     ws_outtype_t * outtype_tuple;
     time_t first_time;
     time_t last_time;
     int do_init;
     struct timeval real_start_time;
     struct timeval real_end_time;
     wslabel_t * label_event_cnt;
     wslabel_t * label_event_rate;
     wslabel_t * label_byte_cnt;
     wslabel_t * label_bandwidth;
     wslabel_t * label_datetime;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc) {

     int op;

     while ((op = getopt(argc, argv, "tT")) != EOF) {
          switch (op) {
          case 't':
          case 'T':
               proc->do_timestamp = 1;
               break;
          default:
               return 0;
          }
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

     proc->label_byte_cnt = wsregister_label(type_table, "BYTE_CNT");
     proc->label_event_cnt = wsregister_label(type_table, "EVENT_CNT");
     proc->label_event_rate = wsregister_label(type_table, "EVENT_RATE");
     proc->label_bandwidth = wsregister_label(type_table, "BANDWIDTH");
     proc->label_datetime = wsregister_label(type_table, "DATETIME");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc)) {
          return 0;
     }
     
     //other init 

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

     if (wsdatatype_match(type_table, input_type, "FLUSH_TYPE")) {
          proc->outtype_tuple =
                         ws_add_outtype(olist, dtype_tuple, NULL);
          return proc_flush;  // not matching expected type
     }

     if (wsdatatype_match(type_table, input_type, "BINARY_TYPE")) {
          return proc_binary;  // not matching expected type
     }

     if (wsdatatype_match(type_table, input_type, "MONITOR_TYPE")) {
          return proc_monitor;
     }

     // we are happy.. now set the processor function
     return proc_meta; // a function pointer
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 2 if not output
static int proc_binary(void * vinstance, wsdata_t* input_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     wsdt_binary_t * bin = (wsdt_binary_t *)input_data->data;
     proc->event_cnt++;
     proc->byte_cnt += bin->len;

     if (!proc->do_init) {
          proc->do_init=1;
          gettimeofday(&proc->real_start_time, NULL);
     }
     gettimeofday(&proc->real_end_time, NULL);

     return 0;
}

static int proc_meta(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     proc->event_cnt++;

     if (!proc->do_init) {
          proc->do_init=1;
          gettimeofday(&proc->real_start_time, NULL);
     }
     gettimeofday(&proc->real_end_time, NULL);

     return 0;
}

static void print_process_times(double timediff, double totalbytes) {
     double bps;

     if (timediff <= 0) {
       return;
     }

     tool_print("processed %.2f seconds of traffic", timediff);

     bps = totalbytes/timediff;

     if (bps > (1024. * 1024.)) {
       tool_print("Processed %.3f MBytes per second",
                bps / (1024. * 1024.));

       tool_print("Processed %.3f Mbits per second",
                bps * 8 / (1024. * 1024.));
     }
     else if (bps > 1024.) {
       tool_print("Processed %.3f KBytes per second", bps / 1024.);
       tool_print("Processed %.3f Kbits per second", bps * 8 / 1024.);
     }
     else {
       tool_print("Processed %.3f Bytes per second", bps);
       tool_print("Processed %.3f bits per second", bps * 8);
     }
}

static void print_item_times(double timediff, double totalitems) {
     double ips;

     if (timediff <= 0) {
       return;
     }

     tool_print("processed %.2f seconds of events", timediff);

     ips = totalitems/timediff;

     if (ips > (1000. * 1000.)) {
       tool_print("Processed %.3f Million events per second",
                ips / (1000. * 1000.));
     }
     else if (ips > 1000.) {
       tool_print("Processed %.3f Thousand events per second", ips / 1000.);
     }
     else {
       tool_print("Processed %.3f events per second", ips);
     }
}


static void print_stats(proc_instance_t * proc) {
     double timediff;

     //gettimeofday(&real_end_time, NULL);
     
     if (proc->byte_cnt) {
          tool_print("byte cnt %" PRIu64, proc->byte_cnt);
          if (proc->event_cnt) {
               tool_print("average bytes per event %.2f",
                          (double)proc->byte_cnt/
                          (double)proc->event_cnt);
          }
     }
     if (proc->event_cnt) {
          tool_print("metadata event cnt %" PRIu64, proc->event_cnt);
     }

     double t1, t2;

     //convert integer times to decimal seconds 
     t1 =  (double)proc->real_start_time.tv_usec/1000000. +
           (double)proc->real_start_time.tv_sec;
     t2 =  (double)proc->real_end_time.tv_usec/1000000. +
           (double)proc->real_end_time.tv_sec;

     timediff = t2 - t1;

     if (proc->byte_cnt) {
          tool_print("Using Real System times:");
          print_process_times(timediff, proc->byte_cnt);
     }
     if (proc->event_cnt) {
          print_item_times(timediff, proc->event_cnt);
     }
}

static inline void proc_reset_stats(proc_instance_t * proc) {
     proc->real_start_time.tv_sec = proc->real_end_time.tv_sec;
     proc->real_start_time.tv_usec = proc->real_end_time.tv_usec;
     proc->event_cnt = 0;
     proc->byte_cnt = 0;
}

static void print_item_stats_tuple(proc_instance_t * proc,
                                   wsdata_t * tdata) {
     double timediff;

     double t1, t2;

     //convert integer times to decimal seconds 
     t1 =  (double)proc->real_start_time.tv_usec/1000000. +
           (double)proc->real_start_time.tv_sec;
     t2 =  (double)proc->real_end_time.tv_usec/1000000. +
           (double)proc->real_end_time.tv_sec;

     timediff = t2 - t1;

     tuple_member_create_uint64(tdata, proc->event_cnt, proc->label_event_cnt);
     if (timediff) {
          tuple_member_create_double(tdata, 
                                     (double)proc->event_cnt / timediff,
                                     proc->label_event_rate);
     }

}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (!proc->event_cnt) {
          return 0;
     }
    
     if(0 == ws_check_subscribers(proc->outtype_tuple)) {
          dprint("no subscriber found");
          // default to the detailed but rigid-way of bw summmary.
          // On the other hand, if we have a 'print' kid (a valid subscriber), then we MAY
          // want to handle output in a datatype-specific format
          print_stats(proc);
          proc_reset_stats(proc);
          return 0;
     }

     wsdata_t * tdata = ws_get_outdata(proc->outtype_tuple); 
     if(!tdata) {
          error_print("unable to allocate memory .. see '%s:%d'", __FILE__, __LINE__);
          return 0;
     }
     if(proc->do_timestamp) {
         time_t sec = time(NULL);
         tuple_member_create_sec(tdata, sec, proc->label_datetime); 
     }
     if (proc->event_cnt) {
          print_item_stats_tuple(proc, tdata);
     }
     ws_set_outdata(tdata, proc->outtype_tuple, dout);
     proc->outcnt++;
     proc_reset_stats(proc);
     return 0;
}

// this function is polled periodically by a monitor kid to get periodic health and
// status 
static int proc_monitor(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     //you need to get a tuple from the monitor to write your output
     wsdata_t * mtdata = wsdt_monitor_get_tuple(input_data);

     //you can allocate a subtuple for organizing a kid's specific output
     wsdata_t * tdata = tuple_member_create_wsdata(mtdata, dtype_tuple,
                                                   proc->label_bandwidth);

     if (tdata) {
          
          if (proc->event_cnt) {
               print_item_stats_tuple(proc, tdata);
          }
     }

     //the following function must be called before exiting this callback
     // this function signals that your kid has been visited and data has been
     // appended
     wsdt_monitor_set_visit(input_data);
     return 0;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("output cnt %" PRIu64, proc->outcnt);

     print_stats(proc);

     //free dynamic allocations
     free(proc);

     return 1;
}

