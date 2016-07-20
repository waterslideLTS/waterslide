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
#define PROC_NAME "metadice"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <netinet/in.h>
#include <time.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "procloader.h"
#include "sysutil.h"
#include "listhash.h"

char proc_name[]       	= PROC_NAME;
char *proc_tags[]	= { "output", NULL };
char proc_purpose[]     = "Print out content buffers from metadata streams.";
char *proc_synopsis[]	= { "metadice [-s <separator>] [-m <count>] [-t <period>] [ -v <moveprefix>] [-V] [-w <prefix>] [-E <ENV>]", NULL };
char proc_description[]	= "Print out content from metadata streams in text format. The separator between values, the number of records per file, the time period to create new files, and the destination where files are written can all be specified by the user.";
proc_example_t proc_examples[]  = {
	{"... | metadice -w 'myoutput.' -v '/data/myoutput.' -V -t 10m'", "write out a new file every 10 minutes with a file name prefix of 'myoutput'. When finished the file is moved to /data using the same file prefix. The tuple labels are also included with the -V flag."},
	{"... | metadice -m 100 -w 'myoutput.' -v '/data/myoutput.' -V -E MYENV'", "write a new file every 100 records with a file name prefix of 'myoutput.'. When the file reaches 100 records in length move it to /data using the same file prefix. Include tuple lables in the output with the -V flag and append the 'MYENV' environment variable to the filename."},
	{NULL, ""}
};  
char *proc_alias[]      	= { "metaout","metaprint", "printmeta", NULL };
char proc_version[]     	= "1.5";
char proc_requires[]		= "";
char *proc_input_types[]    	= {"tuple", NULL};
char *proc_output_types[]	= {"tuple", NULL};
char *proc_menus[]      	= { NULL };
proc_port_t proc_input_ports[]  = {{NULL, NULL}};
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[]        = {NULL};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'s',"","separator",
      "set separator between fields",0,0},
     {'m',"","count",
      "max records per file",0,0},
     {'t',"","period",
     "period to create new file (default unit is sec.; use m for minutes, h for hours)",0,0},
     {'v',"","moveprefix",
     "destination to move file to after write completes ",0,0},
     {'V',"","",
      "output labels attached to tuple data",0,0},
     {'w',"","prefix",
     "output file prefix to write metadata ",0,0},
     {'E',"","ENVIRONMENT",
     "environment variable to append to filename",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]	= "";

#define MAX_SPLITFILENAME 256

//function prototypes for local functions
static int process_tuple(void *, wsdata_t*, ws_doutput_t*, int);

typedef struct _label_write_t {
     char * prefix;
     char * moveprefix;
     FILE * fp;
     time_t current_boundary;
     uint64_t records;
     char splitfile[MAX_SPLITFILENAME];
     char movefile[MAX_SPLITFILENAME];
} label_write_t;

typedef struct _proc_instance_t {
     uint64_t tuple_cnt;
     char * writefile;
     char * movefile;
     char * extension;
     time_boundary_t splittime;
     int timestamp_output;
     listhash_t * file_table;
     char sep;
     int max_records;
     int verbose;  /* >=1 if labels should be printed */
     char * env;
} proc_instance_t;

static int proc_cmd_options(int argc, char ** argv, 
                             proc_instance_t * proc) {
     int op;

     while ((op = getopt(argc, argv, "E:Vm:s:t:v:w:")) != EOF) {
          switch (op) {
          case 'V':
               proc->verbose++;
               break;
          case 's':
               if (strcmp(optarg, "\\n") == 0) {
                    proc->sep = '\n';
                    tool_print("using [newline] as record separator");
               }
               else if (strcmp(optarg, "\\t") == 0) {
                    proc->sep = '\t';
                    tool_print("using [tab] as record separator");
               }
               else {
                    proc->sep = optarg[0];
                    tool_print("using '%c' as record separator", proc->sep);
               }
               break;
          case 'm':
               proc->max_records = atoi(optarg);
               tool_print("using %d as max record per file", proc->max_records);
               break;
          case 't':
               proc->timestamp_output = 1;
               proc->splittime.increment_ts = sysutil_get_duration_ts(optarg);
               if (proc->splittime.increment_ts) {
                    fprintf(stderr,"%s new file every ", PROC_NAME);
                    sysutil_print_time_interval(stderr,
                                                proc->splittime.increment_ts);
                    fprintf(stderr,"\n");
               }
               else {
                    tool_print("time must be divisible by the hour %d",
                               (int)proc->splittime.increment_ts);
                    return 0;
               }
               break;
          case 'v':
               proc->movefile = strdup(optarg);
               tool_print("moving files to prefix %s after closing", optarg);
               break;
          case 'w':
               proc->writefile = strdup(optarg);
               tool_print("writing metadata to prefix %s", optarg);
               break;
          case 'E':
               proc->env = getenv(optarg);
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

     proc->sep = '\t';
     proc->file_table = listhash_create(256, sizeof(label_write_t));
     proc->extension = ".meta";

     //read in command options
     if (!proc_cmd_options(argc, argv, proc)) {
          return 0;
     }

     if (!proc->timestamp_output) {
          proc->splittime.increment_ts = sysutil_get_duration_ts("1h");
     }

     return 1; 
}

// this function needs to decide on processing function based on datatype
// given.. also set output types as needed (unless a sink)
//return 1 if ok
// return 0 if problem
proc_process_t proc_input_set(void * vinstance, wsdatatype_t * meta_type,
                              wslabel_t * port,
                              ws_outlist_t* olist, int type_index,
                              void * type_table) {
     //proc_instance_t * proc = (proc_instance_t*)vinstance;

     //check if datatype has print function
     if (!meta_type->print_func){
          return NULL;
     }

     //proc_instance_t * proc = (proc_instance_t *)vinstance;
     if (meta_type != dtype_tuple) {
          return NULL;
     }

     return process_tuple; // a function pointer
}

/*-----------------------------------------------------------------------------
  * taken from proc_print.c for printing labels
  *---------------------------------------------------------------------------*/
   static inline void print_label_v1(proc_instance_t * proc, 
                       wsdata_t * input_data, FILE *fp) {
      fprintf(fp, "[");
      int i; 
      for (i = 0; i < input_data->label_len; i++) {
           if (i > 0) {
                fprintf(fp, ":");
           }
           fprintf(fp, "%s", input_data->labels[i]->name);
      }
      fprintf(fp, "]");
      
      //if (proc->verbose > 1) {
      //     fprintf(proc->outfp,"{%s}", input_data->dtype->name);
      //}
 }

static inline char * create_prefix(char * fname, wslabel_t * label, char * env) {
     char * lname = NULL;
     char * prefix = NULL;
     char * pnext = NULL;
     int lname_len = 0;
     int elen = 0;
     int len = 1;
     if (label) {
          lname = label->name;
          lname_len = strlen(lname);
          len = lname_len + 2;
     }
     if (env) {
          elen = strlen(env);
          len += elen + 2;
     }
     if (fname) {
          int wlen = strlen(fname);
          len += wlen;
          prefix = (char *)calloc(1, len);
          if (!prefix) {
               error_print("failed calloc of prefix");
               return NULL;
          }
          memcpy(prefix, fname, wlen);
          pnext = prefix + wlen;
     }
     else {
          prefix = (char *)calloc(1, len);
          if (!prefix) {
               error_print("failed calloc of prefix");
               return NULL;
          }
          pnext = prefix;
          prefix[len-1] = '\0';
     }
     if (env) {
          memcpy(pnext, env, elen);
          pnext[elen] = '.';
          pnext[elen + 1] = '\0';
          pnext += elen + 1;
     }
     if (lname) {
          memcpy(pnext, lname, lname_len);
          pnext[lname_len] = '.';
          pnext[lname_len + 1] = '\0';
     }
     return prefix;
}

static inline FILE * get_fp(proc_instance_t * proc, time_t sec, wsdata_t * wsd) {
     if (!sec) {
          sec = time(NULL);
     }
     sysutil_test_time_boundary(&proc->splittime, sec);

     wslabel_t * label = NULL;
     if (wsd->label_len) {
          //get last label
          label = wsd->labels[wsd->label_len -1];
     }
     label_write_t * lw = listhash_find_attach(proc->file_table,
                                               (const char *)&label, sizeof(wslabel_t *));

     if (proc->max_records && (lw->records >= proc->max_records)) {
          lw->current_boundary = 0; //reset boundary - forces opening of new file
     }
     if (lw->current_boundary != proc->splittime.current_boundary) {
          lw->current_boundary = proc->splittime.current_boundary;
          if (lw->fp) {
               fclose(lw->fp);
               if (lw->splitfile) {
                    tool_print("finished writing to %s", lw->splitfile);

                    if (lw->moveprefix) {
                         sysutil_rename_file(lw->splitfile, lw->movefile);
                         tool_print("moved %s to %s", lw->splitfile, lw->movefile);
                    }
               }
          }
          if (!lw->prefix) {
               lw->prefix = create_prefix(proc->writefile, label, proc->env);
               if (proc->movefile) {
                    lw->moveprefix = create_prefix(proc->movefile, label,
                                                   proc->env);
               }
          }
          lw->fp = sysutil_open_timedfile(lw->prefix, proc->extension,
                                          proc->splittime.current_boundary,
                                          proc->splittime.increment_ts,
                                          lw->splitfile, MAX_SPLITFILENAME);
          if (lw->moveprefix) {
               sysutil_name_timedfile(lw->moveprefix, proc->extension,
                                 proc->splittime.current_boundary,
                                 proc->splittime.increment_ts,
                                 lw->movefile, MAX_SPLITFILENAME);
          }
          lw->records = 0;

     }
     lw->records++;

     return lw->fp;
}

static inline void local_print_subtuple(proc_instance_t * proc,
                                        wsdata_t * tdata, FILE * fp) {
     wsdt_tuple_t * tuple = (wsdt_tuple_t*)tdata->data;
     wsdata_t * member;
     int i;
     for (i = 0; i < tuple->len; i++) {
          if (i > 0) {
               fprintf(fp, "%c", proc->sep);
          }
          member = tuple->member[i];
         
          if (proc->verbose) {
               print_label_v1(proc, member, fp);
          }
          //if nested tuple
          if (member->dtype == dtype_tuple) {
               if (proc->verbose) {
                    fprintf(fp, "{tuplebegin}%c", proc->sep);
               }
               local_print_subtuple(proc, member, fp);
               if (proc->verbose) {
                    fprintf(fp, "%c{tupleend}", proc->sep);
               }
          }
          else if (member->dtype->print_func) {
               member->dtype->print_func(fp, member,
                                         WS_PRINTTYPE_TEXT);
          }
     }
}

static inline void local_print_tuple(proc_instance_t * proc,
                                     wsdata_t * tdata,
                                     FILE * fp) {
     int i;
     if (tdata->label_len) {
          for (i = 0; i < tdata->label_len; i++) {
               if (i > 0) {
                    fprintf(fp, ":");
               }
               fprintf(fp, "%s", tdata->labels[i]->name);
          }
          fprintf(fp, "%c", proc->sep);
     }

     //handle tuple in a special way to allow for user-defined
     // field separator
     if (tdata->dtype == dtype_tuple) {
          local_print_subtuple(proc, tdata, fp);
     }
     fprintf(fp, "\n");
     fflush(fp);

}

static int process_tuple(void * vinstance, wsdata_t* input_data,
                         ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->tuple_cnt++;

     wsdt_tuple_t * tuple = (wsdt_tuple_t *)input_data->data;
     int i;
     wsdt_ts_t * ts= NULL; 
     //search for timestamp
     for (i = 0; i < tuple->len; i++) {
          if (tuple->member[i]->dtype == dtype_ts) {
               ts = tuple->member[i]->data;
               break;
          }
     } 
     FILE * fp;
     if (!ts) {
          fp = get_fp(proc, 0, input_data);
     }
     else {
          fp = get_fp(proc, ts->sec, input_data);
     }

     if (!fp) {
          return 0;
     }
     local_print_tuple(proc, input_data, fp);

     return 0;
}

static void close_files(void * vlw, void * vdata) {
     label_write_t * lw = (label_write_t *)vlw;
     if (lw->fp) {
          fclose(lw->fp);
          if (lw->splitfile) {
               tool_print("finished writing to %s", lw->splitfile);
                    if (lw->moveprefix) {
                         sysutil_rename_file(lw->splitfile, lw->movefile);
                         tool_print("moved %s to %s", lw->splitfile, lw->movefile);
                    }
          }
          lw->fp = NULL;
          if (lw->moveprefix) {
               free(lw->moveprefix);
          }
          if (lw->prefix) {
               free(lw->prefix);
          }
     }
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("input tuple cnt  %" PRIu64, proc->tuple_cnt);

     //walk list of items close files..
     listhash_scour(proc->file_table, close_files, NULL);
     listhash_destroy(proc->file_table);

     //free dynamic allocations
     free(proc->movefile);
     free(proc->writefile);
     free(proc);

     return 1;
}

