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

//Match strings in fixed positions
#define PROC_NAME "fixedmatch"
//#define DEBUG 1
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include "waterslide.h"
#include "datatypes/wsdt_fixedstring.h"
#include "datatypes/wsdt_tuple.h"
#include "datatypes/wsdt_string.h"
#include "datatypes/wsdt_binary.h"
#include "waterslidedata.h"
#include "procloader.h"
#include "ahocorasick.h"
#include "sysutil.h"
#include "label_match.h"

char proc_version[]     = "1.1";
char *proc_tags[]     = { "match", NULL };
char *proc_alias[]     = { "fmatch", "rightmatch", NULL };
char proc_name[]       = PROC_NAME;
char proc_purpose[]    = "match strings in fixed positions";
char *proc_synopsis[] = {
     "fixedmatch [-L <label>] [-F <file>] [-R <string>] <label of string member to match>",
     NULL };
char proc_description[] =
     "The strings to match come from a file or individually using -R option "
     "(the -R option can be used more than once during kid specification to "
     "match multiple strings.)"
     "\n"
     "When using the dictionary file option (-F), formatting for the file "
     "should have the match string in quotes, whitespace, and then a "
     "label to apply in parentheses.  When using hex, these characters should "
     "be placed between pipe characters. A sample file is as follows: \n"
     "  \"astring\"          (ASTRING_LABEL)\n"
     "  \"bstring\"          (BSTRING_LABEL)\n"
     "  \"|00 00 00 00 00|\" (HEXSTRING_LABEL)\n";

proc_example_t proc_examples[] = {
     {NULL, NULL}
};

proc_option_t proc_opts[] = {
     /*  'option character', "long option string", "option argument",
	 "option description", <allow multiple>, <required>*/
     {'L',"","label",
     "label to add to matched value",0,0},
     {'F',"","file",
     "file with items to search",0,0},
     {'R',"","string",
     "string to match",0,0},
     {'P',"","",
     "add label to parent tuple",0,0},
     //the following must be left as-is to signify the end of the array
     {' ',"","",
     "",0,0}
};

char proc_nonswitch_opts[]    = "LABEL of string member to match";
char *proc_input_types[]    = {"tuple", NULL};
char *proc_output_types[]    = {"tuple", NULL};
char *proc_tuple_member_labels[] = {"MATCH", NULL};
char proc_requires[]    = "";
char *proc_tuple_container_labels[]     = {NULL};
char *proc_tuple_conditional_container_labels[] = {NULL};

proc_port_t proc_input_ports[] = {
     {"none","pass if match"},
     {"TAG","pass all, tag tuple if match"},
     {"INVERSE","pass if no match"},
     {"NOT","pass if no match"},
     {NULL, NULL}
};

//function prototypes for local functions
static int proc_process_meta(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_meta_inverse(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_allstr(void *, wsdata_t*, ws_doutput_t*, int);
static int proc_process_allstr_inverse(void *, wsdata_t*, ws_doutput_t*, int);


#define FIXEDMATCH_LIST_SIZE (256)
#define LOCAL_MAX_TYPES (100)

//for containing list of matches
typedef struct _fixedmatchlist_t {
     int len;
     int offset;
     int atend;
     listhash_t * matches;
     struct _fixedmatchlist_t * next;
} fixedmatchlist_t;

typedef struct _proc_instance_t {
     uint64_t meta_process_cnt;
     uint64_t meta_flow_cnt;
     uint64_t hits;
     uint64_t outcnt;
     int do_tag[LOCAL_MAX_TYPES];
     ws_outtype_t * outtype_tuple;
     fixedmatchlist_t * matchlist; //ordered by longest first
     wslabel_t * label_match;
     wslabel_nested_set_t nest;
     int add_parent_label;
} proc_instance_t;

static int add_fixedmatch_string(proc_instance_t * proc, const char * str, int len,
                                 int atend, int offset, wslabel_t * label) {
     //walk existing matchlist to see if we already have a table
     fixedmatchlist_t * cursor;
     fixedmatchlist_t * prev = NULL;
     fixedmatchlist_t * inserttable = NULL;
     for (cursor = proc->matchlist; cursor; cursor = cursor->next) {
          if (cursor->len < len) {
               //we now have an insert point
               break;
          }
          else if ((cursor->len == len) && (cursor->atend == atend) &&
                   (cursor->offset == offset)) {
               inserttable = cursor;  //we have a match
               break;
          }
          prev = cursor;
     }
     if (!inserttable) {
          //we need to create new match table
          inserttable = calloc(1, sizeof(fixedmatchlist_t));
          if (!inserttable) {
               error_print("unable to allocate match table");
               return 0;  //unable to insert
          }
          inserttable->matches = listhash_create(FIXEDMATCH_LIST_SIZE, 0);
          if (!inserttable->matches) {
               error_print("unable to allocate match table hashtable");
               return 0;  //unable to insert
          }
          inserttable->len = len;
          inserttable->offset = offset;
          inserttable->atend = atend;

          //insert table into ordered list
          if (prev) {
               inserttable->next = prev->next;
               prev->next = inserttable;
          }
          else {
               inserttable->next = proc->matchlist;
               proc->matchlist = inserttable;
          }
     }

     //insert string
     listhash_find_attach_reference(inserttable->matches, str, len, label);
     return 1;
}

static int fixedmatch_loadfile(proc_instance_t * proc, void * type_table, char * thefile) {
     FILE * fp;
     char line [2001];
     int linelen;
     char * linep;
     char * matchstr;
     int matchlen;
     char * endofstring;
     char * labelstr;
	int offset;
	int atend;

     if ((fp = sysutil_config_fopen(thefile,"r")) == NULL) {
          error_print("fixedmatch_loadfile input file %s could not be located", thefile);
          error_print("fixedmatch Loadfile not found.");
          return 0;
     }
	tool_print("fixedmatch load file %s", thefile);

     while (fgets(line, 2000, fp)) {
		offset = 0;
		atend = 0;
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
          labelstr = NULL;

          // read line - exact seq
          if (linep[0] == '"') {
               linep++;
               endofstring = (char *)strrchr(linep, '"');
               if (endofstring == NULL) {
                    continue;
               }
               endofstring[0] = '\0';
               matchstr = linep;
               matchlen = strlen(matchstr);
			tool_print("matching \"%.*s\"", matchlen, matchstr);
               sysutil_decode_hex_escapes(matchstr, &matchlen);
			linep = endofstring + 1;
          }
          /*else if (linep[0] == '{') {
               linep++;
               endofstring = (char *)strrchr(linep, '}');
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
          }*/
          else {
               continue;
          }
		if (matchstr) {
			if (strstr(linep,"atend") != NULL) {
				atend = 1;
				tool_print("detect atend");
			}
			char * detectoffset = strstr(linep,"offset=");
			if (detectoffset) {
				offset = atoi(detectoffset + 7);
				tool_print("detect offset %d", offset);
			}
               //find (PROTO)
               labelstr = (char *) strchr(linep,'(');
               endofstring = (char *) strrchr(linep,')');

               if (labelstr && endofstring && (labelstr < endofstring)) {
                    labelstr++;
                    endofstring[0] = '\0';

                    //turn protostring into label
				wslabel_t * label = wsregister_label(type_table, labelstr);
				
                    add_fixedmatch_string(proc, matchstr, matchlen, atend, offset, label);
               }
               else  {
                    add_fixedmatch_string(proc, matchstr, matchlen, atend, offset, proc->label_match);
               }
          }
     }
     sysutil_config_fclose(fp);
	return 1;
}


static int proc_cmd_options(int argc, char ** argv, 
                            proc_instance_t * proc, void * type_table) {
     int op;
     int atend = 0;
     int offset = 0;

     while ((op = getopt(argc, argv, "PeEo:O:R:F:L:")) != EOF) {
          switch (op) {
          case 'P':
               proc->add_parent_label = 1;
               break;
          case 'F':
			fixedmatch_loadfile(proc, type_table, optarg);
               break;
          case 'e': //fallthrough
          case 'E': 
               atend = 1;
               tool_print("matching at end");
               break;
          case 'o': //fallthrough
          case 'O': 
               offset = atoi(optarg);
               tool_print("offset set to %d", offset);
               break;
          case 'L':
               proc->label_match = wsregister_label(type_table, optarg);
               tool_print("setting label to %s", optarg);
               break;
          case 'R':
               {
                    char *buf = strdup(optarg);
                    int len = strlen(buf);
                    sysutil_decode_hex_escapes(buf, &len);  //destructive 
                    add_fixedmatch_string(proc, buf, len, atend, offset,
                                          proc->label_match);
                    tool_print("added match string '%s'", optarg);
                    free(buf);
               }
               break;
          default:
               return 0;
          }
     }
     while (optind < argc) {
          wslabel_nested_search_build(type_table, &proc->nest, argv[optind]);
          tool_print("searching for string with label %s", argv[optind]);
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

     proc->label_match = wsregister_label(type_table, "FMATCH");

     //read in command options
     if (!proc_cmd_options(argc, argv, proc, type_table)) {
          return 0;
     }

     if (!proc->matchlist) {
          tool_print("no matched defined");
          return 0;
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

     if (type_index >= LOCAL_MAX_TYPES) {
          return NULL;
     }
     if (wslabel_match(type_table, port, "TAG")) {
          proc->do_tag[type_index] = 1;
     }

     if (!wsdatatype_match(type_table, input_type, "TUPLE_TYPE")) {
          return NULL;  // not matching expected type
     }
     if (!proc->outtype_tuple) {
          proc->outtype_tuple = ws_add_outtype(olist, input_type, NULL);
     }

     if (wslabel_match(type_table, port, "INVERSE") ||
         wslabel_match(type_table, port, "NOT")) {
          if (!proc->nest.cnt) {
               return proc_process_allstr_inverse;
          }
          else {
               return proc_process_meta_inverse; // a function pointer
          }
     }
     else {
          if (!proc->nest.cnt) {
               return proc_process_allstr;
          }
          else {
               return proc_process_meta; // a function pointer
          }
     }
}

static inline int find_fixedmatch(proc_instance_t * proc, wsdata_t * wsd, char * content,
                                  int len, wsdata_t * tdata, wsdata_t * tparent) {
     if (len <= 0) {
          return 0;
     }

     fixedmatchlist_t * cursor;
     for (cursor = proc->matchlist; cursor; cursor = cursor->next) {
          wslabel_t * mlabel = NULL;
          if ((cursor->offset < len) && (cursor->len <= (len - cursor->offset))) {
               if (cursor->atend) {
                    int loff = len - cursor->offset - cursor->len;
                    mlabel = listhash_find(cursor->matches,
                                           content + loff,
                                           cursor->len);

               }
               else {
                    mlabel = listhash_find(cursor->matches,
                                           content + cursor->offset,
                                           cursor->len);
               }
               if (mlabel) {
                    if (!wsdata_check_label(wsd, mlabel)) {
                         if (tdata) {
                              tuple_add_member_label(tdata, wsd, mlabel);
                         }
                         else {
                              wsdata_add_label(wsd, mlabel);
                         }
                    }

                    if (proc->add_parent_label && tdata && 
                        !wsdata_check_label(tdata, mlabel)) {
                         if (tparent) {
                              tuple_add_member_label(tparent, tdata, mlabel);
                         }
                         else {
                              wsdata_add_label(tdata, mlabel);
                         }
                    }
                    return 1;
               }
          }
     }
     return 0;
}

static inline int member_match(proc_instance_t *proc, wsdata_t *member,
                               wsdata_t * wsd_label, wsdata_t * tdata,
                               wsdata_t * tparent) {
     int found = 0;
     char * buf;
     int len;
     if (dtype_string_buffer(member, &buf, &len)) {
          found = find_fixedmatch(proc, wsd_label, buf, len, tdata, tparent);
     }

     if (found) {
          proc->hits++;
     }

     return found;
}

static int proc_nest_match_callback(void * vinstance, void * ignore,
                              wsdata_t * tdata, wsdata_t * member,
                              wsdata_t * tparent) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     return member_match(proc, member, member, tdata, tparent);
}

static int proc_process_meta(void * vinstance, wsdata_t* input_data,
                             ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int found = tuple_nested_search2(input_data, &proc->nest,
                                     proc_nest_match_callback,
                                     proc, NULL);

     if (found || proc->do_tag[type_index]) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}

static int proc_nest_notmatch_callback(void * vinstance, void * ignore,
                              wsdata_t * tdata, wsdata_t * member) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     return member_match(proc, member, NULL, NULL, NULL);
}

static int proc_process_meta_inverse(void * vinstance, wsdata_t* input_data,
                                     ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;

     proc->meta_process_cnt++;

     int found = tuple_nested_search(input_data, &proc->nest,
                                     proc_nest_notmatch_callback,
                                     proc, NULL);
     if (!found) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}

//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_allstr(void * vinstance, wsdata_t* input_data,
                        ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     wsdt_tuple_t * tuple = input_data->data;

     proc->meta_process_cnt++;

     int i;
     int tlen = tuple->len; //use this length because we are going to grow tuple
     wsdata_t * member;
     int found = 0;

     dprint("doing tuple search");
     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          found += member_match(proc, member, member, input_data, NULL);
     }

     if (found || proc->do_tag[type_index]) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}


//// proc processing function assigned to a specific data type in proc_io_init
//return 1 if output is available
// return 0 if not output
static int proc_process_allstr_inverse(void * vinstance, wsdata_t* input_data,
                                       ws_doutput_t * dout, int type_index) {

     proc_instance_t * proc = (proc_instance_t*)vinstance;
     wsdt_tuple_t * tuple = input_data->data;

     proc->meta_process_cnt++;

     int i;
     int tlen = tuple->len; //use this length because we are going to grow tuple
     wsdata_t * member;
     int found = 0;

     dprint("doing tuple search");
     for (i = 0; i < tlen; i++) {
          member = tuple->member[i];
          found += member_match(proc, member, NULL, NULL, NULL);
     }
     if (!found) {
          ws_set_outdata(input_data, proc->outtype_tuple, dout);
          proc->outcnt++;
     }

     return 1;
}

//return 1 if successful
//return 0 if no..
int proc_destroy(void * vinstance) {
     proc_instance_t * proc = (proc_instance_t*)vinstance;
     tool_print("meta_proc cnt %" PRIu64, proc->meta_process_cnt);
     tool_print("meta_flow cnt %" PRIu64, proc->meta_flow_cnt);
     tool_print("matched tuples cnt %" PRIu64, proc->hits);
     tool_print("output cnt %" PRIu64, proc->outcnt);

     //free up match tables
     fixedmatchlist_t * cursor = proc->matchlist;
     fixedmatchlist_t * next = NULL;
     while (cursor) {
          next = cursor->next;
          if (cursor->matches) {
               listhash_destroy(cursor->matches);
          }
          free(cursor);
          cursor = next;
     }

     //free dynamic allocations
     free(proc);

     return 1;
}

