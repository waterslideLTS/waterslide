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
#define PROC_NAME "dpu"
//#define DEBUG 1

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include "waterslide.h"
#include "waterslidedata.h"
#include "datatypes/wsdt_tuple.h"
#include "sysutil.h"  //sysutil_config_fopen

#include "funny-car/funny-car-c.h" // DPU driver

#define PATTERN_ID_NOT_SET    (-1) // this must be negative
#define LOCAL_MAX_TERMS_SIZE (2000)
char proc_name[]       = PROC_NAME;
char proc_version[]     = "1.5";
char *proc_alias[]     = { "tdpu", "tpu", NULL };
char *proc_menus[]     = { "Filters", NULL };
char *proc_tags[]     = { "RegEx", "Match Annotation", NULL };
char proc_purpose[]    = "Accelerate regexp on a Lewis Rhodes Labs DPU/TPU hardware device.";
char *proc_synopsis[] = {"dpu [-L <label>] [-D <directory>] -F <file> [-B] [-P] <label of binary member to match>", NULL};
char proc_description[] = "The dpu kid compiles the regex given via a text file description."
     "The regex is evaluated by the Lewis Rhodes Labs DPU hardware device, which provides "
     "highly efficient evaluation of the queries on the input tuple in a very deterministic "
     "manner.  Only tuples with binary data are searched for regex. By default, only input "
     "data (tuples) with matches are emitted; use -P to emit ALL data, where those data "
     "containing matches are further labeled as specified by the user.";
proc_example_t proc_examples[] = {
     {"...| dpu RESPONSE_CONTENT -L RESULT -F 'queries.re' | print -TV\n", "use regular expressions in the input file 'queries.re' and perform regex on binary data in the RESPONSE_CONTENT tuple, then append the label RESULT to tuples with matches and emit them.\n"},
     {"...| dpu RESPONSE_CONTENT -L RESULT -F 'queries.re' -P | print -TV\n", "emit all data, both those that contain matches and those that do not.\n"},
     {NULL, ""}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'L',"","label",
     "Label to add to matching results of regexp search (default is DPU_RESULT).",0,0},
     {'F',"","/path/to/file.re",
     "Path to file containing list of regular expressions.",0,0},
     {'B',"","",
     "regex file is bare (not in the standard form with labels).",0,0},
     {'P',"","",
     "pass/send all data through (default is to ONLY pass data with match).",0,0},
     {'D',"","/path/to/device0",
     "Path to the DPU device (default is /dev/lru_npu0).",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_requires[] = "Regex Specification File";
char proc_nonswitch_opts[]    = "LABEL(s) of binary member to match";
char *proc_input_types[]    = {"tuple", "flush", NULL};
char *proc_output_types[]    = {"tuple", NULL};
proc_port_t proc_input_ports[] = {
     {"none","perform regular expression matching on content in label"},
     {NULL, NULL}
};

char *proc_tuple_container_labels[] = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};
char *proc_tuple_member_labels[] = {"FIRST_MATCHING_PATTERN_ID", "FIRST_MATCHING_PATTERN_STRING", NULL};

//function prototypes for local functions
static int proc_process_tuple(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_flush(void *, wsdata_t*, ws_doutput_t*, int);

// struct that contains a pointer to the input_data that we enqueue,
// whether or not there is a regex match found
typedef struct _queue_data_t {
     uint32_t has_match; // used to know which data will be labeled as matching
     uint32_t processed_all_members; // 0 if some members still need to be processed; otherwise, last member processed
     wsdata_t *input_data;
     wsdata_t *member;
     int pattern_id;
} queue_data_t;
/*
 * Define the callback structure used for accounting and aiding
 * this kid in subsequent forwarding of result from DPU hardware.
 * This is the context struct that is handed to the result callback
 * functions.
 */
typedef struct _dpu_cb_struct_t_ {
     uint8_t regex_matched; // number of regex matches found
     uint8_t is_last_member; // 0 if not last member; otherwise, 'member' is last in input_data
     nhqueue_t *q;
     pthread_spinlock_t *mylock;
     queue_data_t *qd;
     uint64_t *nregex_matches_ptr; // pointer to global number of regex matches
     uint32_t *found_match_ptr;
} dpu_cb_struct_t;

/*
 * Callback function for mid-packet results; in general, the buffer that
 * we submit to the DPU hardware is internally broken into smaller blocks,
 * which are then regex searched. For all blocks with a match, the partial
 * function below is called.
 */
void partial_cb_match(void *ep, NPUResult res) {
     dpu_cb_struct_t *priv = (dpu_cb_struct_t *)ep;
     if(res.block_match) { // we have a match in a block
          dprint("found a partial match");
          ++priv->regex_matched;
          if(priv->qd->pattern_id == -1) {
               // will only set for the first match in dpu packet
               priv->qd->pattern_id = res.pattern;
          }
     }
}

/*
 * Callback function to handle packet completion.  Deals with outputting
 * any remaining buffered results, and also cleans up the context object.
 */
void total_cb_match(void *ep, NPUResult res) {
     // accumulate results from the last block of data
     partial_cb_match(ep, res);
     dpu_cb_struct_t *priv = (dpu_cb_struct_t *)ep;

     if(priv->regex_matched) {
          dprint("num regex_matched = %u", priv->regex_matched);
          *priv->found_match_ptr = 1;
          priv->qd->has_match = 1;
     } else {
          priv->qd->has_match = 0;
     }
     priv->qd->processed_all_members = priv->is_last_member;

     // all DPU processed data are added to the queue for the QK kid thread to post-process
     pthread_spin_lock(priv->mylock);
     queue_add(priv->q, priv->qd);
     pthread_spin_unlock(priv->mylock);

     // possibly increment number of match count, if we just handled the last member
     if(priv->is_last_member) {
          if(*priv->found_match_ptr) {
               *priv->nregex_matches_ptr += 1;
               *priv->found_match_ptr = 0; // reset for next use
          }
     }

     // free back callback structure to our user-managed memory
     free(priv);
}

// a struct object for each pattern to match
typedef struct _dpu_term_t_ {
     wslabel_t *label;
     char *pattern;
} dpu_term_t;

// kid-specific structure
typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t outcnt;
     uint64_t nfail;
     uint64_t no_submit_to_dpu;
     uint64_t nregex_matches;

     dpu_term_t term_dpu[LOCAL_MAX_TERMS_SIZE];

     uint32_t term_dpu_len;
     int next_1st_pattern_id;
     uint32_t dpuBlockSz;
     uint32_t found_match; // XXX: this is intended only for dedicated use by the npu thread
     char * reFilePath;
     char * dpuDevName;
     NPUDriver * npuDriver;
     NPUClient * npuClient;

     wslabel_t * label_result;
     wslabel_t * label_first_pattern;
     wslabel_t * label_first_pattern_str;

     ws_outtype_t * outtype_tuple;

     wslabel_set_t lset;

     // Define user-managed memory that will be used by two threads - the QK main
     // thread and the thread delegated for handling callbacks from the DPU hardware.
     // The nhqueue_t type is never thread-safe by itself, so we define a lock to help
     // keep the important list of user-managed memory as well as the array of
     // regex'ed data.
     nhqueue_t * cbqueue;
     pthread_spinlock_t lock;
     int is_bare_regex;
     int pass_all;
     int dpu_thread_stopped;
} proc_instance_t;


// returns 1 on success; 0 on failure
static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;
     int num_match_labels = 0;

     while ((op = getopt(argc, argv, "L:F:D:BP")) != EOF) {
          switch (op) {
          case 'L':
               proc->label_result = wsregister_label(type_table, optarg);
               tool_print("using output label %s", optarg);
               break;
          case 'F':
               free(proc->reFilePath); // free any default value
               proc->reFilePath = strdup(optarg);
               break;
          case 'D':
               free(proc->dpuDevName);
               proc->dpuDevName = strdup(optarg);
               break;
          case 'B':
               proc->is_bare_regex = 1;
               break;
          case 'P':
               proc->pass_all = 1;
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_set_add(type_table, &proc->lset, argv[optind]);
          tool_print("filtering on %s", argv[optind]);
          optind++;
          num_match_labels++;
     }

     if(!num_match_labels) {
          error_print("must specify at least one label to match - %s", PROC_NAME);
          return 0;
     }

     dprint("%d labels to match", num_match_labels);
     return 1;
}


// XXX:  Functions in note below were lifted from proc_vectormatchre2.cc
// NOTE: dpumatch_add_element, process_hex_string, dpumatch_loadfile were copied
//       almost verbatim from the vectormatchre2 kid.  It is better to merge
//       these functions with their corresponding functions in vectormatchre2 
//       (vectormatch_add_element, process_hex_string, and vectormatch_loadfile)
//       for ease of development and maintenance.

/*-----------------------------------------------------------------------------
 * dpumatch_add_element
 *   adds an element to the dpu table
 * 
 * [in] vinstance  - the current instance
 * [in] type_table - needed to create new labels
 * [in] restr      - the input regex pattern/string
 * [in] matchlen   - the input string length
 * [in] labelstr   - string containing the label to be made
 *
 * [out] rval      - 1 if okay, 0 if error.
 *---------------------------------------------------------------------------*/
static int dpumatch_add_element(void *vinstance, void * type_table, 
				   char *restr, unsigned int matchlen,
				   char *labelstr)
{
   proc_instance_t *proc = (proc_instance_t*)vinstance;
   wslabel_t *newlab;
   
   /* push everything into a dpu element */
   if (proc->term_dpu_len+1 < LOCAL_MAX_TERMS_SIZE) {
      newlab = wsregister_label(type_table, labelstr);
      uint32_t match_id = proc->term_dpu_len;
      proc->term_dpu[match_id].pattern = strdup(restr);
      proc->term_dpu[match_id].label = newlab;
      proc->term_dpu_len++;
   } else {
      error_print("%s: regex pattern table full.", __FILE__);
      return 0;
   }
   
   return 1;
}

/*-----------------------------------------------------------------------------
 * process_hex_string
 *   Lifted straight out of label_match.c
 *
 *---------------------------------------------------------------------------*/
static int process_hex_string(char * matchstr, int matchlen) {
     int i;
     char matchscratchpad[1500];
     int soffset = 0;
     
     for (i = 0; i < matchlen; i++) {
          if (isxdigit(matchstr[i])) {
               if (isxdigit(matchstr[i + 1])) {
                    matchscratchpad[soffset] = (char)strtol(matchstr + i, NULL, 16);
                    soffset++;
                    i++;
               }
          }
     }
     
     if (soffset) {
          //overwrite hex-decoded string on top of original string..
          memcpy(matchstr, matchscratchpad, soffset);
     }
     
     return soffset;
}

/*-----------------------------------------------------------------------------
 * dpumatch_loadfile
 *   read input match strings from input file.  This function is taken
 *   from label_match.c/label_match_loadfile.
 *---------------------------------------------------------------------------*/
static int dpumatch_loadfile(void* vinstance, void* type_table,
				char * thefile) 
{
     FILE * fp;
     char line [2001];
     int linelen;
     char * linep;
     char * matchstr;
     int matchlen;
     char * endofstring;
     char * match_label;

     proc_instance_t *proc = (proc_instance_t*)vinstance;

     if ((fp = sysutil_config_fopen(thefile,"r")) == NULL) {
          error_print("Could not open dpumatches file %s:\n  %s", thefile, strerror(errno));
          return 0;
     } 

     while (fgets(line, 2000, fp)) {
          //strip return
          linelen = strlen(line);
          if (line[linelen - 1] == '\n') {
               line[linelen - 1] = '\0';
               linelen--;
          }

          if ((linelen <= 0) || (line[0] == '#')) {
               continue;
          }

          linep = line;
          matchstr = NULL;
          match_label = NULL;

          // read line - exact seq
          if (linep[0] == '"') {
               linep++;
               endofstring = (char *)rindex(linep, '"');
               if (endofstring == NULL) {
                    continue;
               }

               endofstring[0] = '\0';
               matchstr = linep;
               matchlen = strlen(matchstr);
               //sysutil_decode_hex_escapes(matchstr, &matchlen);
               linep = endofstring + 1;
          } else {
               if (linep[0] == '{') {
                    linep++;
                    endofstring = (char *)rindex(linep, '}');
                    if (endofstring == NULL) {
                         continue;
                    }
                    endofstring[0] = '\0';
                    matchstr = linep;

                    matchlen = process_hex_string(matchstr, strlen(matchstr));
                    if (!matchlen) {
                         continue;
                    }

                    linep = endofstring + 1;
               } else {
                    continue;
               }
          }

          // Get the corresonding label 
          if (matchstr) 
          { 
               //find (PROTO)
               match_label = (char *) index(linep,'(');
               endofstring = (char *) rindex(linep,')');

               if (match_label && endofstring && (match_label < endofstring)) {
                    match_label++;
                    endofstring[0] = '\0';
               } else {
                    error_print("no label corresponding to match string '%s'\n", matchstr);
                    sysutil_config_fclose(fp);
                    return 0;
               }

               linep = endofstring + 1;
          }

          if (!dpumatch_add_element(proc, type_table, matchstr, matchlen, 
                         match_label)) {
               sysutil_config_fclose(fp);
               return 0;
          }

          tool_print("Adding entry for string '%s' label '%s' ",
                    matchstr, match_label);
     }
     sysutil_config_fclose(fp);

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


     proc->next_1st_pattern_id = PATTERN_ID_NOT_SET;
     proc->dpuBlockSz = 1024;
     proc->dpuDevName = strdup("/dev/lrl_npu0");
     // default labels...
     proc->label_result = wsregister_label(type_table, "DPU_RESULT");
     proc->label_first_pattern = wsregister_label(type_table, "FIRST_MATCHING_PATTERN_ID");
     proc->label_first_pattern_str = wsregister_label(type_table, "FIRST_MATCHING_PATTERN_STRING");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if(!proc->reFilePath) {
          error_print("must specify file with regex terms");
          return 0;
     }

     npu_set_log_level(NPU_INFO);
     proc->npuDriver = npu_open(proc->dpuDevName, proc->dpuBlockSz,16);
     if(!proc->npuDriver) {
          error_print("failed to open DPU hardware device");
          return 0;
     }

#if 0
     //TODO: it may be more efficient to load pattern directly from file, but this
     //      appears to be broken as of version 0.5 release of the dpu driver:
     //      it fails to yield the correct pattern counts or the NPUResult.pattern
     if(npu_load_pcre_file(proc->npuDriver, proc->reFilePath, false) < 0) {
          error_print("could not load regex file: '%s'", proc->reFilePath);
          return 0;
     }
#else
     if(proc->is_bare_regex) {
          // plain regexes expected in file, with no labels (and weights)
          FILE *fp;
          size_t guessmaxlen = 1024, read = 0;
          char * regex_line = (char *)malloc(guessmaxlen);
          if ((fp = fopen(proc->reFilePath, "r")) == NULL)
          {
               error_print("could not open file '%s'", proc->reFilePath);
               return 0;
          }

          // read each line of the regex file, stripping out end-of-line character if applicable, and
          // load pattern into npu driver
          while ((read = getline(&regex_line, &guessmaxlen, fp)) != EOF) {
               dprint("regex line read: '%s'\n", regex_line);
               if(read != 0 && regex_line[0] != '\n') {
                    npu_pattern_insert(proc->npuDriver, regex_line,
                              (regex_line[read-1] == '\n') ? read - 1 : read, false); // 'read-1' --> do not include newline
               }
          }
          free(regex_line);
          fclose(fp);
          proc->term_dpu_len = npu_pattern_count(proc->npuDriver);
     } else {
          // standaard regex input for QK with labels
          if(!dpumatch_loadfile(proc, type_table, proc->reFilePath)) {
               error_print("could not fully load file '%s'", proc->reFilePath);
               return 0;
          }
          uint32_t i;
          for(i = 0; i < proc->term_dpu_len; ++i) {
               if(npu_pattern_insert(proc->npuDriver, proc->term_dpu[i].pattern, strlen(proc->term_dpu[i].pattern), false) < 0) {
                    error_print("could not insert pattern: '%s'", proc->term_dpu[i].pattern);
                    return 0;
               }
          }
     }

     npu_load_patterns(proc->npuDriver);
#endif

     status_print("number of loaded patterns: %d\n", (int)npu_pattern_count(proc->npuDriver));

     // initialize our local queue for alloc and dealloc of the callback structure objects

     // initialize our queue of metadata
     proc->cbqueue = queue_init();
     if(!proc->cbqueue) {
          error_print("could not init queue of metadata");
          return 0;
     }

     pthread_spin_init(&proc->lock, PTHREAD_PROCESS_PRIVATE);

     if(npu_start_thread(proc->npuDriver) < 0) {
          error_print("could not start DPU thread");
          return 0;
     }

     proc->npuClient = npu_get_client(proc->npuDriver, partial_cb_match, total_cb_match);
     if(!proc->npuClient) {
          error_print("could not crete a client for holding reference to npuDriver");
          return 0;
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
     proc_instance_t * proc = (proc_instance_t *)vinstance;

     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype_byname(type_table, olist, "TUPLE_TYPE", NULL);
     }

     // assign appropriate function pointers based on input type

     if (wsdatatype_match(type_table, meta_type, "FLUSH_TYPE")) {
          dprint("trying to register flusher type");
          return proc_flush;
     }

     if (wsdatatype_match(type_table, meta_type, "TUPLE_TYPE")) {
          return proc_process_tuple;
     }

     return NULL;
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_tuple(void * vinstance, wsdata_t* input_data,
                                 ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     int i, j;
     wsdata_t **members;
     int mlen;
     int input_submitted_to_dpu = 0;
     int nbinary; // number of binary members to be submitted to DPU
     int nsubmitted;

     ++proc->meta_process_cnt;
     if(proc->dpu_thread_stopped) {
          // this flag is set once we stop the dpu thread, at which point, we stop
          // submitting jobs to the dpu thread's queue
          ++proc->no_submit_to_dpu;
          return 1;
     }

     // STEP 1: add new entry(ies) to our running list for DPU's 'out-of-sync' regex processing;
     //   the out-of-sync regex processing simply means that the DPU hardware operates at its
     //   own pace independent of this main thread's processing speed.
     for (i = 0; i < proc->lset.len; ++i) {
          // process each serach label specified on this kid
          if (tuple_find_label(input_data, proc->lset.labels[i], &mlen, &members)) {
               // go through all binary members of the matching tuple
               nbinary = 0;
               nsubmitted = 0;
               // first, count the number of binary members to be submitted
               for (j = 0; j < mlen; j++) {
                    if (members[j]->dtype == dtype_binary || members[j]->dtype == dtype_string) {
                         ++nbinary;
                    }
               }

               if(0 == nbinary) continue; // no binary to process, go to next search label if one exists

               wsdata_add_reference(input_data);

               // then, submit all binary members but mark the last binary member separately
               for (j = 0; j < mlen; j++) {
                    if (members[j]->dtype == dtype_binary || members[j]->dtype == dtype_string) {
                         wsdata_t *member = members[j];

                         ++nsubmitted;

                         dpu_cb_struct_t *cbdata = (dpu_cb_struct_t *)calloc(1, sizeof(dpu_cb_struct_t));

                         if(!cbdata) {
                              error_print("Failed to allocate callback object!");
                              wsdata_delete(input_data);
                              return 0;
                         }
                         cbdata->qd = (queue_data_t *)calloc(1, sizeof(queue_data_t));
                         cbdata->qd->has_match             = 0;
                         cbdata->qd->processed_all_members = 0;
                         cbdata->qd->input_data            = input_data;
                         cbdata->qd->member                = member;
                         cbdata->qd->pattern_id            = -1;

                         cbdata->q              = proc->cbqueue;
                         cbdata->mylock         = &proc->lock;
                         cbdata->is_last_member = (nsubmitted == nbinary) ? 1 : 0;
                         cbdata->regex_matched  = 0;
                         cbdata->nregex_matches_ptr = &proc->nregex_matches;
                         cbdata->found_match_ptr    = &proc->found_match;

                         // Create a new packet to send to the DPU
                         if(npu_client_new_packet(proc->npuClient, cbdata) < 0) {
                              error_print("DPU failed to yield a new packet");
                              wsdata_delete(input_data);
                              ++proc->nfail;
                              return 0;
                         }

                         // extract pointers to the beinnning and end+1 of data buffer
                         char *data_begin_ptr = NULL;
                         char *data_end_ptr = NULL;
                         if(members[j]->dtype == dtype_binary) {
                              wsdt_binary_t *wsb = (wsdt_binary_t *)member->data;
                              data_begin_ptr = wsb->buf;
                              data_end_ptr = wsb->buf + wsb->len;
                         } else if(members[j]->dtype == dtype_string) {
                              wsdt_string_t *wss = (wsdt_string_t *)member->data;
                              data_begin_ptr = wss->buf;
                              data_end_ptr = wss->buf + wss->len;
                         }

                         // now, send data to DPU hardware
                         const char *last_write_ptr = npu_client_write_packet(proc->npuClient, data_begin_ptr, data_end_ptr);
                         if(last_write_ptr != data_end_ptr) {
                              error_print("failed to write all data to the DPU packet!");
                              wsdata_delete(input_data);
                              ++proc->nfail;
                              return 0;
                         }

                         // tell the client that we've reached the end of the packet, so it will enqueue the
                         // completion callback to tell us when the device is done with it.
                         if(npu_client_end_packet(proc->npuClient) < 0) {
                              error_print("npu_client_end_packet failed");
                              wsdata_delete(input_data);
                              ++proc->nfail;
                              return 0;
                         }

                         input_submitted_to_dpu = 1;
                    }
               }
          }
     }

     if(proc->pass_all && !input_submitted_to_dpu) {
          // we pass all data whether or not they contain match or whether or not they are even searched
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          ++proc->outcnt;
     }

     // STEP 2: check list for any completed jobs (from the DPU hardware), then send data along with memory cleanups
     uint32_t num_to_process = queue_size(proc->cbqueue); //XXX: retrieve size BEFORE looping for thread-safeness!!!
     for (i = 0; i < num_to_process; i++) {
          pthread_spin_lock(&proc->lock);
          queue_data_t * qd = (queue_data_t *)queue_remove(proc->cbqueue);
          pthread_spin_unlock(&proc->lock);

          if(qd->has_match) {
               // add label to tuple member if there is a match
               tuple_add_member_label(qd->input_data, qd->member, proc->label_result);

               // add first match member only
               if(proc->next_1st_pattern_id == PATTERN_ID_NOT_SET) {
                    if(qd->pattern_id >= 0 && qd->pattern_id < proc->term_dpu_len) { // ensure a valid indexable pattern id
                         if(!proc->is_bare_regex) {
                              tuple_add_member_label(qd->input_data, qd->member, proc->term_dpu[qd->pattern_id].label);
                         }
                         tuple_member_create_int(qd->input_data, qd->pattern_id, proc->label_first_pattern);
                         char * patternstr = (char *)npu_pattern_source(proc->npuDriver, qd->pattern_id);
                         tuple_dupe_string(qd->input_data, proc->label_first_pattern_str, patternstr, strlen(patternstr));
                         proc->next_1st_pattern_id = qd->pattern_id;
                    } else {
                         ++proc->nfail;
                    }
               }
          }

          if(qd->processed_all_members) {
               // if proc->next_1st_pattern_id is not PATTERN_ID_NOT_SET, then this metadata (qd->input_data) has a match
               if(proc->pass_all || proc->next_1st_pattern_id != PATTERN_ID_NOT_SET) {
                    // send out tuple
                    ws_set_outdata(qd->input_data, proc->outtype_tuple, dout);
                    ++proc->outcnt;
                    proc->next_1st_pattern_id = PATTERN_ID_NOT_SET; // reset for next use
               }

               // remove the reference we added before enqueue'ing for the DPU hardware
               wsdata_delete(qd->input_data);
               free(qd);
          }
     }

     return 1;
}

static int proc_flush(void * vinstance, wsdata_t* input_data,
                      ws_doutput_t * dout, int type_index) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;

     if (dtype_is_exit_flush(input_data)) {
          npu_client_flush(proc->npuClient);
          npu_put_client(&proc->npuClient);
          if(npu_stop_thread(proc->npuDriver) < 0) {
               error_print("failed to stop threads properly");
               return 0;
          }

          proc->dpu_thread_stopped = 1;

          // Gather any outstanding results and send out. Given that the auxillary thread should have completed by now,
          // we no longer need to lock while accessing queue data.
          queue_data_t * qd = NULL;
          while ( (qd = queue_remove(proc->cbqueue)) ) {

               if(qd->has_match) {
                    // add label to tuple member if there is a match
                    tuple_add_member_label(qd->input_data, qd->member, proc->label_result);

                    // add first match member only
                    if(proc->next_1st_pattern_id == PATTERN_ID_NOT_SET) {
                         if(qd->pattern_id >= 0 && qd->pattern_id < proc->term_dpu_len) { // ensure a valid indexable pattern id
                              if(!proc->is_bare_regex) {
                                   tuple_add_member_label(qd->input_data, qd->member, proc->term_dpu[qd->pattern_id].label);
                              }
                              tuple_member_create_int(qd->input_data, qd->pattern_id, proc->label_first_pattern);
                              char * patternstr = (char *)npu_pattern_source(proc->npuDriver, qd->pattern_id);
                              tuple_dupe_string(qd->input_data, proc->label_first_pattern_str, patternstr, strlen(patternstr));
                              proc->next_1st_pattern_id = qd->pattern_id;
                         } else {
                              ++proc->nfail;
                         }
                    }
               }

               if(qd->processed_all_members) {
                    // if proc->next_1st_pattern_id is not PATTERN_ID_NOT_SET, then this metadata (qd->input_data) has a match
                    if(proc->pass_all || proc->next_1st_pattern_id != PATTERN_ID_NOT_SET) {
                         // send out tuple
                         ws_set_outdata(qd->input_data, proc->outtype_tuple, dout);
                         ++proc->outcnt;
                         proc->next_1st_pattern_id = PATTERN_ID_NOT_SET; // reset for next use
                    }

                    // remove the reference we added before enqueue'ing for the DPU hardware
                    wsdata_delete(qd->input_data);
                    free(qd);
               }
          }
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     npu_put_client(&proc->npuClient);
     npu_put_driver(&proc->npuDriver);
     queue_exit(proc->cbqueue);
     pthread_spin_destroy(&proc->lock);

     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     if (proc->meta_process_cnt) {
          tool_print("output cnt %" PRIu64, proc->outcnt);
          tool_print("output percentage %.2f%%",
                     (double)proc->outcnt * 100 /
                     (double)proc->meta_process_cnt);
          tool_print("regex matches found %" PRIu64, proc->nregex_matches);
          if(proc->nfail) {
               tool_print("num failure detected %" PRIu64, proc->nfail);
          }
          if(proc->no_submit_to_dpu) {
               tool_print("outstanding metadata not submitted to DPU thread %" PRIu64, proc->no_submit_to_dpu);
          }
     }

     //free dynamic allocations
     free(proc->npuClient);
     free(proc->npuDriver);
     free(proc->reFilePath);
     free(proc->dpuDevName);
     free(proc);

     return 1;
}

