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
#define PROC_NAME "csv_in"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <poll.h>
#include <zlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "sysutil.h"
#include "timeparse.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_uint.h"
#include "datatypes/wsdt_uint64.h"
#include "datatypes/wsdt_double.h"
#include "mimo.h"

char proc_name[]	= PROC_NAME;
char *proc_tags[]	= { "input", NULL };
char proc_purpose[]	= "Data source processor for event tuples based on character-"
     "delimited data.";
char *proc_synopsis[]	= {"csv_in <LABEL> ... [-i][-F <file>][-P <file> -t "
     "<duration>][-U <port> -w][-l | -I][-d | -s '<delimiters>']", NULL};
char proc_description[]	= "Reads in data in comma separated value (CSV) format. "
     "(Default delimiter is a comma.)  The data is bundled into a tuple for output. "
	"Labels for each field can be specified as command line arguments (maximum "
     "128 labels) or with the input data (using options). Any labels exceeding "
     "the number of fields parsed are ignored. If labels are not given, they will "
     "be left empty. The -l option is used to indicate that the first element in "
     "each event is a label for the tuple container. In contrast, the -I option "
     "indicates that the input data includes (unlimited) member labels inline; "
     "labels must be enclosed in square brackets (i.e., [LABEL]value), and default "
     "behavior expects tab ('\\t') delimiters. (NOTE: If the first field in the "
     "event does not have a label, it will be treated as a tuple container label "
     "UNLESS it is the first event in the data set when it will be ignored.) " 
     "Delimiter characters (one or more) can be set using the -d or -s option "
     "(identical behavior); delimiters are additive, so both options can be used "
     "in the same command. Default behavior expects delimited events via a pipe. "
     "Events can be parsed from stdin when the -i option is used. The -F option "
     "is used to provide a file with a list of CSV input files. Additionally, the "
     "-P option is used to provide a file that will be regularly polled for events. "
     "The default polling interval (60 seconds) is changed using the -t option. ";
proc_example_t proc_examples[]	= {
     {"find /data/*.csv | waterslide \"csv_in FIRST SECOND THIRD | ... ", 
          "Read in all event tuples in a list of CSV files and label the fields "
               "'FIRST', 'SECOND', and 'THIRD'."},
     {"cat /data/*.csv | waterslide \"csv_in -il DATE TIME | ... ", "Read from "
          "stdin all event tuples in a list of CSV files, use first element in "
               "each event as a tuple container label, and label tuple members "
               "'DATE' and 'TIME'."},
     {"... | csv_in -F filelist.txt -Id ':*' | ...", "Read in event tuples from "
          "files listed in 'filelist.txt'; tuplize data using inline labels and "
               "delimiters ':' and '*'."},
	{"... | csv_in -P events.csv -t 15 -s '\\t' DATE TIME MSG | ... ", "Poll "
          "event tuples from 'events.csv' every 15 seconds; tuplize data using "
               "tab delimiters; label tuple members 'DATE', 'TIME', and 'MSG'"}, 
     {NULL, NULL}
};
char *proc_alias[]  = {NULL};
char proc_version[] = "1.5";
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
     {'d',"","delimiters",
     "use characters as delimiters (CRNL = no delimiter)",0,0},
     {'F',"","file",
     "use file as a list of csv input files",0,0},
     {'r',"","file",
     "Read in a single CSV file", 0,0},
     {'i',"","",
     "use stdin for data",0,0},
     {'I',"","",
     "use inline labels (NOTE: default delimiter is '\\t')",0,0},
     {'l',"","",
     "use first element in each event as label for tuple container",0,0},
     {'P',"","file",
     "poll this file for events",0,0},
     {'p',"","",
     "Pre-load data from CSV files.  Emit all data at the beginning.", 0, 0},
     {'s',"","delimiters",
     "use characters as delimiters",0,0},
     {'t',"","duration",
     "poll a file on this period",0,0},
     {'N',"","",
     "disable auto-datatyping of members (e.g., as UINT, etc)...all members are marked as string types",0,0},
     {'X',"","",
     "read entire line as record",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};
char proc_nonswitch_opts[]	= "LABEL to apply to parsed field";

#define CSV_FILENAME_MAX 1000
#define MAX_INLINE_LABELS 16 // limit on # of inline labels each member can have
#define LABEL_DELIM ":"
#define LABEL_BEGIN_CHAR '['
#define LABEL_END_CHAR ']'
#define MAXUDPBUF 9000
#define READ_BUFSIZE 65536

typedef struct _listen_sock_t {
     int s; //the file descriptor
     uint16_t port;
     struct sockaddr_in sock_server;
     struct sockaddr_in6 sock_server6;
     socklen_t socklen;

     char buf[MAXUDPBUF +1];
} listen_sock_t;

// processing module instance data structure
typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t badline_cnt;
     uint64_t outcnt;

     ws_outtype_t * outtype_tuple;
     char * filename; // name of file (e.g., from list of csv files) to be parsed
     // list of labels for members parsed from input events
     wslabel_set_t lset;	// limited to 128 by WSMAX_LABEL_SET in waterslide.h
     gzFile fp; // file descriptor that supports read/write of gzip files
     FILE * in; // event input source (e.g., stdin, list of files)
     char * delim; // delimiter used in file between members of tuple
     int straight_data; //used stdin for data
     int do_poll; // poll a file for events
     char * poll_filename; // file that will be polled for events
     time_t poll_duration; // poll file on this period 
     time_t next_time; // local time + poll_duration (time for next poll of file)
     int inline_labels; // use inline labels
     // list of inline labels for a specific member 
     wslabel_t * label_inline[MAX_INLINE_LABELS]; // limited to 16
     int label_inline_cnt;
     void * type_table; // for inline label lookup
     int first_el_label; // use first element in event as container label
     int done;
     int preload;
     int no_autodatatyping; // auto-datatyping does a good effort of automatically marking datatypes on members
} proc_instance_t;

//function prototypes for local functions
static int proc_cmd_options(int, char **, proc_instance_t *, void *);
// file
// stdin and list of files
static int local_scour_stdin(proc_instance_t *);
static int data_source(void *, wsdata_t*, ws_doutput_t*, int);
static int data_source_preload(void *, wsdata_t*, ws_doutput_t*, int);
static inline int read_csv_file(proc_instance_t *, wsdata_t *);
// poll file
static inline time_t local_get_time(void);
static int data_source_filepoll(void *, wsdata_t*, ws_doutput_t*, int);
static inline int poll_file(proc_instance_t *, wsdata_t *);
static inline int read_csv_buffer(proc_instance_t *, wsdata_t *, char *, int);
static inline void add_tup_labels(proc_instance_t *, wsdata_t *, char *, int);
static inline void get_inline_labels(proc_instance_t *, char **, int *, int);
static inline void set_default_label(proc_instance_t *, int);
static inline void add_timestamp_dt(proc_instance_t *, wsdata_t *, char *, 
                                    char *, int, int);
static inline int detect_strtype (proc_instance_t *, wsdata_t *, char *, int);


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

     // initialize flags and variables
     proc->delim = strdup(","); // default delimiter
     proc->in = stdin; // default input source
     proc->type_table = type_table; // for inline label lookup
     proc->do_poll = 0; // false
     proc->poll_duration = 60; // default duration for polling a file

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     // TODO: If no input stream is given, we should gracefully exit ...
     
     // register sources, set output types
     if (proc->do_poll) { // file will be polled for events
          proc->outtype_tuple =
               ws_register_source_byname(type_table, "TUPLE_TYPE",
                                         data_source_filepoll, sv);
          proc->next_time = local_get_time() + proc->poll_duration;
     }
     else { // not polling file
          if (proc->straight_data) { // getting events from stdin
               int fd = fileno(proc->in); // int descriptor of stdin stream
               proc->fp = gzdopen(fd, "r"); // associates stdin with gzFile object
          }
          else { // reading from list of file names
               if (!local_scour_stdin(proc)) {
                    error_print("no input file to start with");
                    return 0;
               }
          } // TODO: Include ability to read directly from a single csv file

          if ( proc->preload ) {
               proc->outtype_tuple =
                    ws_register_source_byname(type_table, "TUPLE_TYPE", data_source_preload, sv);
          } else {
               proc->outtype_tuple = 
                    ws_register_source_byname(type_table, "TUPLE_TYPE", data_source, sv);
          }
     }

     if (!proc->outtype_tuple) {
          fprintf(stderr, "registration failed\n");
          return 0;
     }

     return 1; 
}

// Process options from command line
static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc, void * type_table) {

     int op;
     // qty of labels provided on cmd line for members parsed from input events
     int label_cnt = 0; // limited to 128

     while ((op = getopt(argc, argv, "lIP:pt:F:r:is:d:NX")) != EOF) {
          switch (op) {
          case 'l': // use first element in event as container label
               proc->first_el_label = 1;
               break;
          case 'I': // use inline labels
               proc->inline_labels = 1;
               free(proc->delim);
               proc->delim = strdup("\t"); // TODO: why default to tab here?
               break;
          case 'P': // poll file for events
               proc->poll_filename = strdup(optarg);
               proc->do_poll = 1;
               break;
          case 'p':
               proc->preload = 1;
               break;
          case 't': // poll file on this period
               proc->poll_duration = atoi(optarg);
               proc->do_poll = 1; // TODO: what happens if you give t but not P?
               break;
          case 'F': // use file as list of csv input files
               proc->in = sysutil_config_fopen(optarg,"r");
               if (!proc->in) { // TODO: test this; proc->in initialized to stdin
                    tool_print("unable to open file for reading %s", optarg);
                    return 0;
               }
               else {
                    tool_print("opened file for reading list of csv files %s", 
                               optarg);
               }
               break;
          case 'r':
               proc->in = sysutil_config_fopen(optarg, "r");
               proc->straight_data = 1;
               if ( !proc->in ) {
                    error_print("Unable to open file '%s' for input", optarg);
                    return 0;
               }
               break;
          case 'i': // use stdin for data
               proc->straight_data = 1;
               break;
          case 's':
          case 'd': // use characters as delimiters
               free(proc->delim);
               if ((strcmp(optarg, "CRNL") == 0) ||
                   (strcasecmp(optarg, "NONE") == 0)) {
                    proc->delim = strdup("\r\n");
                    tool_print("using [newline] and [return] as input delimiter");
               }
               else if (strcmp(optarg, "\\t") == 0) {
                    proc->delim = strdup("\t");
                    tool_print("using [tab] as input delimiter");
               }
               else {
                    proc->delim = strdup(optarg);
                    tool_print("using [%s] as input delimiter", proc->delim);
               }
               break;
          case 'N': // use stdin for data
               proc->no_autodatatyping = 1;
               break;
          case 'X':
               tool_print("reading lines as records");
               proc->delim = strdup("\r\n");
               wslabel_set_add_noindex(type_table, &proc->lset, "LINE");
               label_cnt++;
               proc->no_autodatatyping = 1;
               break;
          default:
               return 0;
          }
     }
     // store all remaining command line tokens as labels for members parsed
     // from the events received as input
     while (optind < argc) {
          // proc->lset is limited to 128 by WSMAX_LABEL_SET defined in waterslide.h
          wslabel_set_add_noindex(type_table, &proc->lset, argv[optind]);
          if ((WSMAX_LABEL_SET - label_cnt) > 0) {
               tool_print("searching for string with label %s", argv[optind]);
          } else {
               tool_print("too many labels: %s", argv[optind]);
          }
          optind++;
          label_cnt++;
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

/*
 * READ FROM FILE
 */

/*
 * READ FROM STDIN
 * READ FROM LIST OF FILES
 */

static int local_scour_stdin(proc_instance_t * proc) {
     int len;
     // proc->filename doesn't get set before we start reading from the list
     //char * buf = proc->filename;  //use pre-allocated buffer
     char buf[CSV_FILENAME_MAX + 1];

     if (proc->done) {
          return 0;
     }

     //close old capture if needed
     if (proc->fp) {
          gzclose(proc->fp);
          proc->fp = NULL;
     }

     //read from stdin.. list of files..
     while (fgets(buf, CSV_FILENAME_MAX, proc->in)) {
          dprint("buf: %s", buf);
          //strip return
          len = strlen(buf);
          if (buf[len - 1] == '\n') {
               buf[len - 1] = '\0';
               len--;
          }
          proc->filename = buf; // includes null terminator
          dprint("proc->filename: %s", proc->filename);
          proc->fp = gzopen(proc->filename, "r"); 
          if (proc->fp) {
               tool_print("opened csv file %s", buf);
               return 1;
          }

     }
     proc->done = 1;
     return 0;
}


static int32_t get_current_index_size(void * type_table) {
       mimo_datalists_t * mdl = (mimo_datalists_t *)type_table;
       return mdl->index_len;
}

// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
//
static int data_source(void * vinstance, wsdata_t* source_data,
                       ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;


     uint32_t start_index_size = get_current_index_size(proc->type_table);


     if (!proc->done && read_csv_file(proc, source_data)) {
          wsdt_tuple_t * tuple = (wsdt_tuple_t*)source_data->data;
          if (tuple->len) {
               uint32_t end_index_size = get_current_index_size(proc->type_table);
               if(start_index_size == end_index_size) {
                    // the index size didn't change, write the data out
                    ws_set_outdata(source_data, proc->outtype_tuple, dout);
               }
               else {
                    // the index size did change, duplicate the tuple to update
                    // the index size.  Needed for searching
                   wsdata_t * newtup = wsdata_alloc(dtype_tuple);
                   if (!newtup) {
                         ws_set_outdata(source_data, proc->outtype_tuple, dout);
                   }
                   else { 
                         int i;
                         for (i = 0; i < tuple->len; i++) {
                              add_tuple_member(newtup, tuple->member[i]);
                         }
                         ws_set_outdata(newtup, proc->outtype_tuple, dout);
                   }
              }
              proc->meta_process_cnt++;
          }    
          return 1;
     }
     else {
          return 0;
     }
}

static int data_source_preload(void *vinstance, wsdata_t* source_data,
                               ws_doutput_t *dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     int emitted = 0;
     int ret = 1;
     while (!proc->done && ret == 1) {
          
          wsdata_t *tuple = NULL;
          if ( source_data ) {
               tuple = source_data;
               source_data = NULL;
          } else {
               tuple = wsdata_alloc(dtype_tuple);
          }
          if ( !tuple ) { ret = 0; break; }

          ret = data_source(vinstance, tuple, dout, type_index);
          if ( !ret ) wsdata_delete(tuple);
          if ( ret ) emitted++;
     }
     proc->done = 1;
     return emitted;
}

static inline int read_csv_file(proc_instance_t * proc, wsdata_t * tdata) {
     int hasdata = 0;
     char buf[READ_BUFSIZE];
     //read from stdin.. list of files..
     if (proc->fp && gzgets(proc->fp, buf, READ_BUFSIZE)) {
          hasdata = 1;
     }
     else if (!proc->straight_data && local_scour_stdin(proc)) {
          if (proc->fp && gzgets(proc->fp, buf, READ_BUFSIZE)) {
               hasdata = 1;
          }
     }
     if (!hasdata) {
          //proc->badline_cnt++;
          return 0;
     }

     int len = strlen(buf);
     if (len <= 1) {
          dprint("line too short, ignoring");
          proc->badline_cnt++;
          return 1;
     }

     // ignore comments
     if (buf[0] == '#') {
          dprint("line is comment, ignoring");
          return 1;
     }

     return read_csv_buffer(proc, tdata, buf, len);
}

/*
 * POLL A FILE
 */

static inline time_t local_get_time(void) {
     struct timeval tv;
     gettimeofday(&tv, NULL);
     return tv.tv_sec;
}

static int data_source_filepoll(void * vinstance, wsdata_t* source_data,
                                ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     
     if (!proc->fp) {
          time_t tnext = local_get_time();
          if (tnext > proc->next_time) {
               dprint("time %d %d %d", (int)tnext, (int)proc->next_time, (int)proc->poll_duration);
               proc->fp = gzopen(proc->poll_filename, "r");
               proc->next_time = tnext + proc->poll_duration;
          }
     }

     if (proc->fp) {
          if (poll_file(proc, source_data)) {
               wsdt_tuple_t * tuple = (wsdt_tuple_t*)source_data->data;
               if (tuple->len) {
                    ws_set_outdata(source_data, proc->outtype_tuple, dout);
               }
          }
     }

     return 1;
}

static inline int poll_file(proc_instance_t * proc, wsdata_t * tdata) {
     char buf[READ_BUFSIZE];

     if (!gzgets(proc->fp, buf, READ_BUFSIZE)) {
          gzclose(proc->fp);
          proc->fp = NULL;
          dprint("close");
          return 0;
     }
     int len = strlen(buf);
     if (len <= 1) {
          dprint("line too short, ignoring");
          proc->badline_cnt++;
          return 0;
     }
     return read_csv_buffer(proc, tdata, buf, len);
}

static inline int read_csv_buffer(proc_instance_t * proc, wsdata_t * tdata, 
                                  char * buf, int len) {
     // strip return
     if (buf[len - 1] == '\n') {
          if (buf[len - 2] == '\r') {
               buf[len - 2] = '\0';
               len -= 2;
          }
          else {
               buf[len - 1] = '\0';
               len--;
          }
     }

     //char * ptok = NULL;
     //char * rtok = strtok_r(buf, proc->delim, &ptok);
     
     char * rtok = strsep(&buf, proc->delim);
     int rec = 0;
     int tuplabels = 0;
     while (rtok) {
          dprint("[rec: %d, tuplables: %d]  rtok %s", rec, tuplabels, rtok);
          int rtok_len = strlen(rtok);

          if (proc->inline_labels) {
               if ((rec == 0) && !tuplabels &&
                   rtok_len && (rtok[0] != LABEL_BEGIN_CHAR)) {
                    add_tup_labels(proc, tdata, rtok, rtok_len);
                    tuplabels = 1;
                    rtok = strsep(&buf, proc->delim);
                    continue;
               }
               else { 
                    get_inline_labels(proc, &rtok, &rtok_len, rec);
               }
          }
          else if (proc->first_el_label && !tuplabels &&
                   (rec == 0) && rtok_len) {
                    add_tup_labels(proc, tdata, rtok, rtok_len);
                    tuplabels = 1;
                    rtok = strsep(&buf, proc->delim);
                    continue;
          }
          dprint("rtok %s", rtok);
               
          if (timeparse_detect_date(rtok, rtok_len) == 1) {
               dprint("date format 1");
               
               //char * timestr = strtok_r(NULL, proc->delim, &ptok);
               char * timestr = strsep(&buf, proc->delim);
               
               int timestr_len = 0;
               if (timestr) {
                    timestr_len = strlen(timestr);
               }
               if (timeparse_detect_time(timestr, timestr_len)) {
                    add_timestamp_dt(proc, tdata, rtok,
                                     timestr, timestr_len, rec);
               }
               else {
                    //treat data as separate records
                    detect_strtype(proc, tdata, rtok, rec);
                    if (timestr) {
                         rec++;
                         detect_strtype(proc, tdata, timestr, rec);
                    }
                    else {
                         return 1;
                    }
               }
          }
          else {
               detect_strtype(proc, tdata, rtok, rec);
          }
          rec++;
          //rtok = strtok_r(NULL, proc->delim, &ptok);

          rtok = strsep(&buf, proc->delim);
     }
     return 1;
}

static inline void add_tup_labels(proc_instance_t * proc, wsdata_t * tdata,
                                  char * buf, int len) {
     char * rtok = strsep(&buf, LABEL_DELIM);
     while (rtok) {
          int lbl_len = strlen(rtok);
          if (lbl_len) {
               wslabel_t * lbl = wsregister_label(proc->type_table, rtok);
               if (lbl) {
                    wsdata_add_label(tdata, lbl);
               }
          }

          rtok = strsep(&buf, LABEL_DELIM);
     }
}

static inline void get_inline_labels(proc_instance_t * proc, char ** vbuf,
                                     int * vlen, int rec) {
     char * buf = *vbuf;
     int len = *vlen;

     dprint("get inline labels");

     proc->label_inline_cnt = 0;

     if ((len < 3)  || (buf[0] != LABEL_BEGIN_CHAR)) {
          set_default_label(proc, rec);
          return;
     }
     buf++;
     len--;

     //look for end section
     char * endlbl = strchr(buf, LABEL_END_CHAR);
     if (!endlbl) {
          set_default_label(proc, rec);
          return;
     }
     else {
          *vbuf = endlbl + 1;
          int diff = endlbl - buf + 1;
          *vlen = len - diff;
          endlbl[0] = '\0';
     }


     // This appears to be where we process the (possibly, multiple) labels for
     // each tuple member. 
     // now search in .. tokenize
     
     //char * ptok = NULL;

     //char * rtok = strtok_r(buf, LABEL_DELIM, &ptok);
     char * rtok = strsep(&buf, LABEL_DELIM);
     while (rtok) {
          int lbl_len = strlen(rtok);
          if (lbl_len) {
               if (proc->label_inline_cnt >= MAX_INLINE_LABELS) {
                    return;
               }
               dprint("Registering label %s", rtok);
               wslabel_t * lbl = wsregister_label(proc->type_table, rtok);
               if (lbl) {
                    proc->label_inline[proc->label_inline_cnt] = lbl;
                    proc->label_inline_cnt++;
               }
          }

          //rtok = strtok_r(NULL, LABEL_DELIM, &ptok);
          rtok = strsep(&buf, LABEL_DELIM);
     }
}

static inline void set_default_label(proc_instance_t * proc, int rec) {
     if (rec < proc->lset.len) {
          proc->label_inline[proc->label_inline_cnt] = proc->lset.labels[rec];
          proc->label_inline_cnt++;
     }
}

static inline void add_timestamp_dt(proc_instance_t * proc, wsdata_t * tdata,
                                    char * str, char * str2, int str2len,
                                    int rec) {
     char buffer[30];
     memcpy(buffer, str, 10);
     buffer[10] = ' ';
     memcpy(buffer + 11, str2, str2len);
     buffer[11+str2len] = '\0';
     wsdata_t * wsd = dtype_str2ts(buffer, 11 + str2len);
     if (!wsd) {
          return;
     }
     if (proc->inline_labels) {
          int i;
          for (i = 0; i < proc->label_inline_cnt; i++) {
               wsdata_add_label(wsd, proc->label_inline[i]);
          }
     }
     else if (rec < proc->lset.len) {
          wsdata_add_label(wsd, proc->lset.labels[rec]);
     }
     add_tuple_member(tdata, wsd);
}

static inline int detect_strtype (proc_instance_t * proc, wsdata_t * tdata,
                                  char * str, int rec) {

     int len = strlen(str);
     if (len <= 0) {
          return 0;
     }
     wsdata_t * wsd = NULL;
     if(proc->no_autodatatyping) {
          // all data members are interpreted as string types
          wsd = wsdata_create_string(str, len);
          if (!wsd) {
               return 0;
          }
     }
     else {
          wsd = dtype_detect_strtype(str, len);
          if (!wsd) {
               wsd = wsdata_create_string(str, len);
               if (!wsd) {
                    return 0;
               }
          }
     }

     if (proc->inline_labels) {
          int i;
          for (i = 0; i < proc->label_inline_cnt; i++) {
               wsdata_add_label(wsd, proc->label_inline[i]);
          }
     }
     else if (rec < proc->lset.len) {
          wsdata_add_label(wsd, proc->lset.labels[rec]);
     }
     if (!add_tuple_member(tdata, wsd)) {
          dprint("overalloc");
          wsdata_delete(wsd);
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("badline cnt %" PRIu64, proc->badline_cnt);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     if (!proc->straight_data && proc->fp) {
          gzclose(proc->fp);
     }
     if (proc->in && proc->in != stdin) {
          sysutil_config_fclose(proc->in);
     }

     //free dynamic allocations
     free(proc->poll_filename);
     free(proc->delim);
     free(proc);

     return 1;
}

