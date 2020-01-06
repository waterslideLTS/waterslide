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
/* program to print out all the help options for each module*/
/* this program makes use of the aliases file for modules */

#include <getopt.h>
#include <dirent.h>
#include <dlfcn.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define _WSUTIL
#include "waterslide.h"
#include "waterslidedata.h"
#include "wstypes.h"
#include "wscalc.h"

#include "wsman_color.h"
#include "wsman_map.h"
#include "wsman_util.h"
#include "wsman_word_wrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif

double (*nameExists)(void *, void *);
uint32_t work_size = 0;

__thread int thread_rank = 0;

int keyword_search = 0;
int check_kid = 0;
int generate_rst = 0;
int tag_search = 0;
int input_type_search = 0;
int output_type_search = 0;
int verbose = 1;

FILE * outfp;
map_t * map = NULL;

char * last_kid = NULL;

int print_module_help(FILE *, const char *);
int print_module_help_plain(FILE *, const char *);
int print_module_help_rst(FILE *, const char *);


char * get_module_path(const char *name, const char * libpath) {
     int dlen = strlen(libpath);
     int modlen = strlen(name);
     int prefix = strlen(WS_PROC_MOD_PREFIX);
     int suffix = strlen(WS_PROC_MOD_SUFFIX);
     char * fullname = (char *)calloc(1,dlen+prefix+modlen+suffix+1);
     if (!fullname) {
          error_print("failed get_module_path calloc of fullname");
          return NULL;
     }

     memcpy(fullname, libpath, dlen);
     memcpy(fullname + dlen, WS_PROC_MOD_PREFIX, prefix);
     memcpy(fullname + dlen + prefix, name, modlen);
     memcpy(fullname + dlen + prefix + modlen,
            WS_PROC_MOD_SUFFIX, suffix);

     return fullname;
}

char * get_module_path_parallel(const char *name, const char * libpath) {
     int dlen = strlen(libpath);
     int modlen = strlen(name);
     int prefix = strlen(WS_PROC_MOD_PREFIX);
     int suffix = strlen(WS_PROC_MOD_PARALLEL_SUFFIX);
     char * fullname = (char *)calloc(1,dlen+prefix+modlen+suffix+1);
     if (!fullname) {
          error_print("failed get_module_path_parallel calloc of fullname");
          return NULL;
     }

     memcpy(fullname, libpath, dlen);
     memcpy(fullname + dlen, WS_PROC_MOD_PREFIX, prefix);
     memcpy(fullname + dlen + prefix, name, modlen);
     memcpy(fullname + dlen + prefix + modlen,
            WS_PROC_MOD_PARALLEL_SUFFIX, suffix);

     return fullname;
}

int search_for_keyword(const char *spath, char * keyword)  {
     void * sh_file_handle;
     if (spath == NULL) {
          return 0;
     }
     // try to locate the passed module name
     if ((sh_file_handle = dlopen(spath, RTLD_LAZY|RTLD_LOCAL))) {
          char ** proc_alias  = (char**)dlsym(sh_file_handle,"proc_alias");
          char * proc_name    = (char *)dlsym(sh_file_handle,"proc_name");
          char * proc_purpose = (char *)dlsym(sh_file_handle,"proc_purpose");
          char * proc_description = (char *)dlsym(sh_file_handle,"proc_description");
          proc_example_t * proc_example = (proc_example_t*)dlsym(sh_file_handle,"proc_examples"); 
          proc_option_t * opt = (proc_option_t*)dlsym(sh_file_handle,"proc_opts"); 
          char * ns_opt = (char*)dlsym(sh_file_handle,"proc_nonswitch_opts"); 

          // Search the name, aliases, purpose, description, example fields
          // for a keyword.
          int retval = 0;

          if (!retval && proc_name && _strcasestr(proc_name, keyword)) retval = 1;

          if (!retval && proc_purpose && _strcasestr(proc_purpose, keyword)) retval = 1;
          if (!retval && proc_alias) {
               int i;
               for (i = 0; proc_alias[i]; i++) {
                    if (_strcasestr(proc_alias[i], keyword)) retval = 1;
               }
          }
          if (!retval && proc_description && _strcasestr(proc_description, keyword)) retval = 1;
          if (!retval && proc_example) {
               int i;
               for (i = 0; proc_example[i].text; i++) {
                    if (_strcasestr((char *) proc_example[i].text, keyword)) retval = 1;
                    if (_strcasestr((char *) proc_example[i].description, keyword)) retval = 1;
               }
          }
          // TODO: this is ugly, needs cleaning 
          // for every option we need to compile the whole string together,
          // then search it 
          if (!retval && (opt || ns_opt)) {
               if (ns_opt && strlen(ns_opt) > 0) {
                    int len = strlen(ns_opt) + 2; 
                    char * ns = (char *) malloc(len+1);
                    if (!ns) {
                         error_print("failed search_for_keyword calloc of ns");
                         return 0;
                    }
                    snprintf(ns, len+1, "<%s>", ns_opt); 
                    if (_strcasestr(ns, keyword)) retval = 1;
               }
               if (opt) {
                    int i = 0;
                    while (opt[i].option != ' ') {
                         int sz = 1024;
                         char * buf = (char *) calloc(1,sz);
                         char * p = buf;
                         int read = 0;

                         if (opt[i].option != '\0') {
                              read = snprintf(p, 4, "[-%c", opt[i].option);  
                         }
		         else {
                              while ((read+3+strlen(opt[i].long_option)+1) > sz) {
                                   sz += 1024;
                                   buf = (char *) realloc(buf, sz);
                                   if (!buf) {
                                        error_print("failed opt.long_option realloc"); 
                                   }
                                   p = buf+read;
                              }
                              read = snprintf(p,3+strlen(opt[i].long_option)+1,
                                        "[--%s", opt[i].long_option);
                         }
                         p = p + read;
                         if (opt[i].argument && strlen(opt[i].argument)) {
                              while ((read+3+strlen(opt[i].argument)+1) > sz) {
                                   sz += 1024;
                                   buf = (char *) realloc(buf, sz);
                                   if (!buf) {
                                        error_print("failed opt.argument realloc"); 
                                   }
                                   p = buf+read;
                              }

                              read = snprintf(p,3+strlen(opt[i].argument)+1,
                                        " <%s>", opt[i].argument);
                              p = p + read;
                         }
                         while ((read+2+strlen(opt[i].description)+1) > sz) {
                             sz += 1024;
                             buf = (char *) realloc(buf, sz);
                             if (!buf) {
                                  error_print("failed opt.description realloc"); 
                             }
                             p = buf+read;
                         }

                         snprintf(p,2+strlen(opt[i].description)+1,
                              "] %s", opt[i].description);
                         if (_strcasestr(p, keyword)) retval = 1;
                         i++;
                    }
               }
          }

          dlclose(sh_file_handle);
          return retval;
     } else {
          fprintf(stderr,"%s\n", dlerror());
          fprintf(stderr,"unable to open module %s\n", spath);
          return 0;
     }
}

// return 1 if this module contains has the tag
int search_for_tag(const char *spath, char * tag)  {
     void * sh_file_handle;
     if (spath == NULL) {
          return 0;
     }
     // try to locate the passed module name
     if ((sh_file_handle = dlopen(spath, RTLD_LAZY|RTLD_LOCAL))) {
          char ** proc_tags  = (char**)dlsym(sh_file_handle,"proc_tags");
          if (proc_tags) {
               int i;
               for (i = 0; proc_tags[i]; i++) {
                    // found the tag
                    if (!strcasecmp(proc_tags[i], tag)) {
                         return 1;
                    }
               }
          }
          dlclose(sh_file_handle);
          return 0;
     } else {
          fprintf(stderr,"%s\n", dlerror());
          fprintf(stderr,"unable to open module %s\n", spath);
          return 0;
     }
}

// return 1 if this module contains has the input type
int search_for_input_type(const char *spath, char * tag)  {
     void * sh_file_handle;
     if (spath == NULL) {
          return 0;
     }
     // try to locate the passed module name
     if ((sh_file_handle = dlopen(spath, RTLD_LAZY|RTLD_LOCAL))) {

          char ** proc_input_types;
          // check for procbuffer kid
          int * is_procbuffer = (int *)dlsym(sh_file_handle,"is_procbuffer");
          if (is_procbuffer) {
               char * proc_itypes[] = {"tuple", "monitor", NULL};
               proc_input_types = (char**)proc_itypes;
          }
          else {
               proc_input_types = (char**)dlsym(sh_file_handle,"proc_input_types");
          }
          if (proc_input_types) {
               int i;
               for (i = 0; proc_input_types[i]; i++) {
                    // found the tag
                    if (!strcasecmp(proc_input_types[i], tag)) {
                         return 1;
                    }
               }
          }
          dlclose(sh_file_handle);
          return 0;
     } else {
          fprintf(stderr,"%s\n", dlerror());
          fprintf(stderr,"unable to open module %s\n", spath);
          return 0;
     }
}

// return 1 if this module contains has the output type
int search_for_output_type(const char *spath, char * tag)  {
     void * sh_file_handle;
     if (spath == NULL) {
          return 0;
     }
     // try to locate the passed module name
     if ((sh_file_handle = dlopen(spath, RTLD_LAZY|RTLD_LOCAL))) {

          char ** proc_output_types;
          // check for procbuffer kid
          int * is_procbuffer = (int *)dlsym(sh_file_handle,"is_procbuffer");
          if (is_procbuffer) {
               char * proc_otypes[] = {"tuple", NULL};
               proc_output_types = (char**)proc_otypes;
          }
          else {
               proc_output_types = (char**)dlsym(sh_file_handle,"proc_output_types");
          }
          if (proc_output_types) {
               int i;
               for (i = 0; proc_output_types[i]; i++) {
                    // found the tag
                    if (!strcasecmp(proc_output_types[i], tag)) {
                         return 1;
                    }
               }
          }
          dlclose(sh_file_handle);
          return 0;
     } else {
          fprintf(stderr,"%s\n", dlerror());
          fprintf(stderr,"unable to open module %s\n", spath);
          return 0;
     }
}

int check_kid_field_single_dereference(FILE *fp, char * field_name, char * field_data, uint32_t minimum_length) {
     if (!field_data) {
          fprintf(fp, "FAILURE: %s is null or undefined\n", field_name);
     }
     else if (strlen(field_data) < minimum_length) {
          fprintf(fp, "FAILURE: %s is shorter than %d\n", field_name, minimum_length);
     }
     else if (verbose) {
          fprintf(fp, "SUCCESS: %s is defined\n", field_name);
     }
     return 0;
}

int check_kid_field_double_dereference(FILE *fp, char * field_name, char ** field_data, uint32_t min_elements) {
     if (!field_data) {
          fprintf(fp, "FAILURE: %s is null or undefined\n", field_name);
     }
     else {
         if(verbose) {
               fprintf(fp, "SUCCESS: %s is defined\n", field_name);
          }
          int i = 0;
          while(field_data[i]) {
               if(strlen(field_data[i]) == 0) {
                    fprintf(fp, "FAILURE: %s[%d] has a zero-length\n",field_name,i);
               }
               else if (verbose) {
                    fprintf(fp, "SUCCESS: %s[%d]\n",field_name,i);
               }
               i++;
          }
          if(i < min_elements) {
               fprintf(fp, "FAILURE: %s had less than %d elements\n",field_name,min_elements);
          }
     }

     return 0;
}


/**
 * check the documentation variables of a kid
 * Show failures by default.  Use -v flag to show successes.
 */
int check_kid_documentation(FILE * fp, const char *spath, char *kid)  {
     void * sh_file_handle;
     //snprintf(spath,255,"%s/module_%s.so", g_pLibPath, modulename);
     if (spath == NULL) { return 0;}
     // try to locate the passed module name
     if ((sh_file_handle = dlopen(spath, RTLD_LAZY|RTLD_LOCAL))) {
          if (verbose) { 
               // test each kid field
               fprintf(fp, "SUCCESS: found %s\n", kid);
          }          
          
          // verify that this variable exists 
          char ** proc_alias  = (char**)dlsym(sh_file_handle,"proc_alias");
          check_kid_field_double_dereference(fp, "proc_alias", proc_alias, 0);

          // verify that this variable exists and has a positive length 
          char * proc_name    = (char *)dlsym(sh_file_handle,"proc_name");
          check_kid_field_single_dereference(fp, "proc_name", proc_name, 1);

          // verify that this variable exists and has a positive length 
          char * proc_purpose = (char *)dlsym(sh_file_handle,"proc_purpose");
          check_kid_field_single_dereference(fp, "proc_purpose", proc_purpose, 1);
          
          // verify that this variable exists and has at least one entry 
          char ** proc_synopsis  = (char**)dlsym(sh_file_handle,"proc_synopsis");
          check_kid_field_double_dereference(fp, "proc_synopsis", proc_synopsis, 1);

          // verify that this variable exists and has a positive length 
          char * proc_description = (char *)dlsym(sh_file_handle,"proc_description");
          check_kid_field_single_dereference(fp, "proc_description", proc_description, 1);

          // verify that this variable exists and has at least one entry 
          char ** proc_tags  = (char**)dlsym(sh_file_handle,"proc_tags");
          check_kid_field_double_dereference(fp, "proc_tags", proc_tags, 1);

          // verify that this variable exists and has at least one entry 
          proc_example_t * examples = (proc_example_t*)dlsym(sh_file_handle,"proc_examples"); 
          if (examples) {
               if(verbose) {
                    fprintf(fp, "SUCCESS: proc_examples is defined\n");
               }
               
               int i = 0;
               while (examples[i].text != NULL) {
                    if(strlen(examples[i].text) == 0) {
                         fprintf(fp, "FAILURE: proc_examples[%d].text has a zero-length\n",i);
                    }
                    else if (verbose) {
                         fprintf(fp, "SUCCESS: proc_examples[%d].text has a length\n",i);
                    }
                    if(examples[i].description == NULL || strlen(examples[i].description) == 0) {
                         fprintf(fp, "FAILURE: proc_examples[%d].description is null or zero-lengthed\n",i);
                    }
                    else if (verbose) {
                         fprintf(fp, "SUCCESS: proc_examples[%d].description has a length\n",i);
                    }
                    i++;
               }
               if(i < 1) {
                    fprintf(fp, "FAILURE: proc_examples didn't have any examples\n");
               }
          }
          else {
               fprintf(fp, "FAILURE: proc_examples is null or undefined\n"); 
          }

          // verify that this variable exists 
          char * proc_requires = (char *)dlsym(sh_file_handle,"proc_requires");
          check_kid_field_single_dereference(fp, "proc_requires", proc_requires, 0);

          // verify that this variable exists and has a positive length 
          char * proc_version = (char *)dlsym(sh_file_handle,"proc_version");
          check_kid_field_single_dereference(fp, "proc_version", proc_version, 1);

          // verify that this variable exists and that each entry has the appropriate lengths 
          proc_option_t * opt = (proc_option_t*)dlsym(sh_file_handle,"proc_opts"); 
          if (opt) {
               if(verbose) {
                    fprintf(fp, "SUCCESS: proc_opts is defined\n");
               }
               
                     // option, long_option, argument, description
               int i = 0;
               while (opt[i].option != ' ') {
                    if(verbose) {
                         fprintf(fp, "SUCCESS: proc_opts[%d].option\n",i);
                    }

                    if(opt[i].long_option == NULL) {
                         fprintf(fp, "FAILURE: proc_opts[%d].long_option shouldn't be null\n",i);
                    }
                    else if (verbose) {
                         fprintf(fp, "SUCCESS: proc_opts[%d].long_option isn't null\n",i);
                    }
                    if(opt[i].argument == NULL) {
                         fprintf(fp, "FAILURE: proc_opts[%d].argument shouldn't be null\n",i);
                    }
                    else if (verbose) {
                         fprintf(fp, "SUCCESS: proc_opts[%d].argument isn't null\n",i);
                    }
                    if(opt[i].description == NULL || strlen(opt[i].description) == 0) {
                         fprintf(fp, "FAILURE: proc_opts[%d].description is null or zero-lengthed\n",i);
                    }
                    else if (verbose) {
                         fprintf(fp, "SUCCESS: proc_opts[%d].description has a length\n",i);
                    }
                    if(opt[i].multiple != 0 && opt[i].multiple != 1) {
                         fprintf(fp, "FAILURE: proc_opts[%d].multiple should be 0 or 1\n",i);
                    }
                    else if (verbose) {
                         fprintf(fp, "SUCCESS: proc_opts[%d].multiple is 0 or 1\n",i);
                    }
                    if(opt[i].required != 0 && opt[i].required != 1) {
                         fprintf(fp, "FAILURE: proc_opts[%d].required should be 0 or 1\n",i);
                    }
                    else if (verbose) {
                         fprintf(fp, "SUCCESS: proc_opts[%d].required is 0 or 1\n",i);
                    }
                    i++;
               }
          }
          else {
               fprintf(fp, "FAILURE: proc_opts is null or undefined\n"); 
          }
          
          // verify that this variable exists 
          char * ns_opt = (char*)dlsym(sh_file_handle,"proc_nonswitch_opts"); 
          check_kid_field_single_dereference(fp, "proc_nonswitch_opts", ns_opt, 0);

          // verify that this variable exists 
          // check for procbuffer kid
          int * is_procbuffer = (int *)dlsym(sh_file_handle,"is_procbuffer");
	  char ** itypes;
          if (!is_procbuffer) {
               itypes = (char**)dlsym(sh_file_handle,"proc_input_types"); 
               check_kid_field_double_dereference(fp, "proc_input_types", itypes, 0);
          }

          // verify that this variable exists 
          // check for procbuffer kid
	  char ** otypes;
          if (!is_procbuffer) {
               otypes = (char**)dlsym(sh_file_handle,"proc_output_types");
               check_kid_field_double_dereference(fp, "proc_output_types", otypes, 0);
          }

          // verify that this variable exists 
          proc_port_t * ports = (proc_port_t*)dlsym(sh_file_handle,"proc_input_ports");
          if (ports) {
               if(verbose) {
                    fprintf(fp, "SUCCESS: proc_input_ports is defined\n");
               }
               
               int i = 0;
               while (ports[i].label != NULL) {
                    if(strlen(ports[i].label) == 0) {
                         fprintf(fp, "FAILURE: proc_input_ports[%d].label has a zero-length\n",i);
                    }
                    else if (verbose) {
                         fprintf(fp, "SUCCESS: proc_input_ports[%d].label has a length\n",i);
                    }
                    if(ports[i].description == NULL || strlen(ports[i].description) == 0) {
                         fprintf(fp, "FAILURE: proc_input_ports[%d].description is null or zero-lengthed\n",i);
                    }
                    else if (verbose) {
                         fprintf(fp, "SUCCESS: proc_input_ports[%d].description has a length\n",i);
                    }
                    i++;
               }
          }
          else {
               fprintf(fp, "FAILURE: proc_input_ports  is null or undefined\n"); 
          }

          // verify that this variable exists 
          char ** tml = (char **)dlsym(sh_file_handle,"proc_tuple_member_labels");
          check_kid_field_double_dereference(fp, "proc_tuple_member_labels", tml, 0);

          // verify that this variable exists 
          char ** tcl = (char **)dlsym(sh_file_handle,"proc_tuple_container_labels");
          check_kid_field_double_dereference(fp, "proc_tuple_container_labels", tcl, 0);

          // verify that this variable exists 
          char ** tccl = (char **)dlsym(sh_file_handle,"proc_tuple_conditional_container_labels");
          check_kid_field_double_dereference(fp, "proc_tuple_conditional_container_labels", tccl, 0);


     }
     else {
          return 0;
     }
     return 1;
}

int print_module_help(FILE * fp, const char *spath) {
     if (generate_rst) {
          return print_module_help_rst(fp, spath);
     } else {
          return print_module_help_plain(fp, spath);
     }
}

int print_module_help_rst(FILE * fp, const char *spath) {
     void * sh_file_handle;
     if (spath == NULL) { return 0;}
     // try to locate the passed module name
     if ((sh_file_handle = dlopen(spath, RTLD_LAZY|RTLD_LOCAL))) {
          // check if module is deprecated
          int * is_deprecated = (int *)dlsym(sh_file_handle,"is_deprecated");
          char * proc_name = (char *)dlsym(sh_file_handle,"proc_name");
          char * proc_purpose = (char *)dlsym(sh_file_handle,"proc_purpose");
          char ** proc_synopsis = (char**)dlsym(sh_file_handle,"proc_synopsis");
          char * proc_description = (char *)dlsym(sh_file_handle,"proc_description");
          char ** proc_tags  = (char**)dlsym(sh_file_handle,"proc_tags");
          char ** proc_alias  = (char**)dlsym(sh_file_handle,"proc_alias");
          char * proc_requires = (char *)dlsym(sh_file_handle,"proc_requires");
          char * proc_version = (char *)dlsym(sh_file_handle,"proc_version");

          proc_example_t * examples = (proc_example_t*)dlsym(sh_file_handle,"proc_examples"); 

          proc_option_t * opt = (proc_option_t*)dlsym(sh_file_handle,"proc_opts"); 
          char * ns_opt = (char*)dlsym(sh_file_handle,"proc_nonswitch_opts"); 

          char ** itypes;
          char ** otypes;
          // check for procbuffer kid
          int * is_procbuffer = (int *)dlsym(sh_file_handle,"is_procbuffer");
	  char * proc_input_types[] = {"tuple", "monitor", NULL};
	  char * proc_output_types[] = {"tuple", NULL};
          if (is_procbuffer) {
               itypes = (char**)proc_input_types;
               otypes = (char**)proc_output_types;
          }
          else {
               itypes = (char**)dlsym(sh_file_handle,"proc_input_types"); 
               otypes = (char**)dlsym(sh_file_handle,"proc_output_types");
          }
          
          proc_port_t * ports = (proc_port_t*)dlsym(sh_file_handle,"proc_input_ports");

          char ** tml = (char **)dlsym(sh_file_handle,"proc_tuple_member_labels");
          char ** tcl = (char **)dlsym(sh_file_handle,"proc_tuple_container_labels");
          char ** tccl = (char **)dlsym(sh_file_handle,"proc_tuple_conditional_container_labels");

          char * header_text;
          if (proc_purpose) {
               if (is_deprecated){
                    _asprintf(&header_text, "%s - DEPRECATED: %s", proc_name, proc_purpose);
               }
               else {
                     _asprintf(&header_text, "%s - %s", proc_name, proc_purpose);
               } 
          } else {
               if (is_deprecated) {
                    _asprintf(&header_text, "%s - DEPRECATED", proc_name);
               }
               else {
                    _asprintf(&header_text, "%s", proc_name);
               } 
          }

          print_rst_header(fp, header_text);
          free(header_text);

          fprintf(fp, "\n");
          if (proc_synopsis) {
               fprintf(fp, "%s\n", proc_synopsis[0]);
          } else {
               fprintf(fp, "none\n");
          }

          if (proc_description) {
               print_rst_subheader(fp, "Description");
               int last_line_was_bullet = 0;
               char * paragraph = strtok (proc_description,"\n"); 
               // For RST, we need two newlines between paragraphs, but only
               // one newline between bullet points. So tokenize on newlines,
               // in the proc_description, and print accordingly.
               // Doc writers seem to be using \t as a way of enumerating 
               // items in the proc_description, so this seems like a natural
               // spot to use bullet points in the RST. 
               while (paragraph!= NULL) {
                    // if there's a \t in this token, we're going to assume
                    // it's an item in a bulleted list 
                    if (strchr(paragraph, '\t')) {
                         // escape some special RST characters
                         char * replaced1 = strreplace(paragraph, "*", "\\*"); 
                         char * replaced2 = strreplace(replaced1, "|", "\\ "); 
                         char * replaced3 = strreplace(replaced2, "\t", "* "); 
                         fprintf(fp, "%s\n", replaced3); 
                         free(replaced1);
                         free(replaced2);
                         free(replaced3);
                         last_line_was_bullet = 1;
                    } else {
                         // add an extra line if the last line printed was
                         // bullet item
                         if (last_line_was_bullet) {
                              last_line_was_bullet=0;
                              fprintf(fp, "\n"); 
                         }
                         fprintf(fp, "%s\n\n", paragraph); 
                    }
                    paragraph = strtok(NULL, "\n");
               }
          } else {
               fprintf(fp, "none\n");
          }

          if (examples) {
               print_rst_subheader(fp, "Examples");
               int i = 0;
               while (examples[i].text != NULL) {
                    if (examples[i].text[0] != '\0') {
                         fprintf(fp, "\n"); 
                         fprintf(fp,".. code-block:: bash\n");
                         fprintf(fp, "\n"); 
                         fprintf(fp, "     # %s\n", examples[i].description);
                         fprintf(fp, "     %s\n", examples[i].text);
                    }
                    i++;
               }
               fprintf(fp, "\n");
          }

          if (proc_tags) {
               print_rst_subheader(fp, "Tags");
               if (proc_tags[0]) {
                    print_rst_list(fp, proc_tags);
               } else {
                    fprintf(fp, "none\n");
               }
          }

          if (proc_alias) {
               print_rst_subheader(fp, "Aliases");
               if (proc_alias[0]) {
                    print_rst_list(fp, proc_alias);
               } else {
                    fprintf(fp, "none\n");
               }

          }

          if (proc_version) {
               print_rst_subheader(fp, "Version");
               fprintf(fp, "%s\n", proc_version);
          }

          if (proc_requires) {
               print_rst_subheader(fp, "Requires");
               if (strcmp(proc_requires, "") == 0) {
                    fprintf(fp, "none\n");
               } else {
                    fprintf(fp, "%s\n", proc_requires);
               }
          }

          if (itypes) {
               print_rst_subheader(fp, "Input Types");
               if (itypes[0]) {
                    print_rst_list(fp, itypes);
               } else {
                    fprintf(fp, "none\n");
               }
          }

          if (otypes) {
               print_rst_subheader(fp, "Output Types");
               if (otypes[0]) {
                    print_rst_list(fp, otypes);
               } else {
                    fprintf(fp, "none\n");
               }
          }

          if (ports) {
               print_rst_subheader(fp, "Input Ports");
               if (ports[0].label) {
                    print_rst_portlist(fp, ports);
               } else {
                    fprintf(fp, "none\n");
               }
          }

          if (tcl) {
               print_rst_subheader(fp, "Tuple Container Labels");
               if (tcl[0]) {
                    print_rst_list(fp, tcl);
               } else {
                    fprintf(fp, "none\n");
               }
          }

          if (tccl) {
               print_rst_subheader(fp, "Tuple Conditional Container Labels");
               if (tccl[0]) {
                    print_rst_list(fp, tccl);
               } else {
                    fprintf(fp, "none\n");
               }
          }

          if (tml) {
               print_rst_subheader(fp, "Appended Tuple Members");
               if (tml[0]) {
                    print_rst_list(fp, tml);
               } else {
                    fprintf(fp, "none\n");
               }
          }

          if (opt || ns_opt) {
               print_rst_subheader(fp, "Options");
               if (ns_opt && strlen(ns_opt) > 0) {
                    fprintf(fp, "default option: %s\n", ns_opt);
                    fprintf(fp,"\n");
               }
               if (opt) {
                    int i = 0;
                    while (opt[i].option != ' ') {
                         fprintf(fp, "-%c ", opt[i].option);
                         if (opt[i].argument && strlen(opt[i].argument) > 0) {
                              fprintf(fp, "<%s> ", opt[i].argument);
                         }
                         if (opt[i].description) {
                              fprintf(fp, " %s ", opt[i].description);
                         }
                         fprintf(fp,"\n");
                         i++;
                    }
               }
          }

          dlclose(sh_file_handle);

          return 1;
     }
     else {
          return 0;
     }
}

/**
 * by default this will use the new print out to outfp option
 * unless you want to print to xml
 */
int print_module_help_plain(FILE * fp, const char *spath) {
     void * sh_file_handle;
     //snprintf(spath,255,"%s/module_%s.so", g_pLibPath, modulename);
     if (spath == NULL) { return 0;}
     // try to locate the passed module name
     if ((sh_file_handle = dlopen(spath, RTLD_LAZY|RTLD_LOCAL))) {
          // find out if the module is deprecated
          int * is_deprecated = (int *) dlsym(sh_file_handle,"is_deprecated"); 
          // get the information from the module that we need to print help
          char ** proc_alias  = (char**)dlsym(sh_file_handle,"proc_alias");
          char * proc_name    = (char *)dlsym(sh_file_handle,"proc_name");
          char * proc_purpose = (char *)dlsym(sh_file_handle,"proc_purpose");
          char ** proc_synopsis  = (char**)dlsym(sh_file_handle,"proc_synopsis");
          char * proc_description = (char *)dlsym(sh_file_handle,"proc_description");
          char ** proc_tags  = (char**)dlsym(sh_file_handle,"proc_tags");
          proc_example_t * examples = (proc_example_t*)dlsym(sh_file_handle,"proc_examples"); 
          char * proc_requires = (char *)dlsym(sh_file_handle,"proc_requires");
          char * proc_version = (char *)dlsym(sh_file_handle,"proc_version");
          proc_option_t * opt = (proc_option_t*)dlsym(sh_file_handle,"proc_opts"); 
          char * ns_opt = (char*)dlsym(sh_file_handle,"proc_nonswitch_opts"); 
          
          char ** itypes;
          char ** otypes;     
          // check for procbuffer kid
          int * is_procbuffer = (int *) dlsym(sh_file_handle,"is_procbuffer");
	  char * proc_input_types[] = {"tuple", "monitor", NULL};
	  char * proc_output_types[] = {"tuple", NULL};
          if (is_procbuffer) {
               itypes = (char**)proc_input_types;

               otypes = (char**)proc_output_types;
          } 
          else {
               itypes = (char**)dlsym(sh_file_handle,"proc_input_types");
               otypes = (char**)dlsym(sh_file_handle,"proc_output_types");
          }

          proc_port_t * ports = (proc_port_t*)dlsym(sh_file_handle,"proc_input_ports");

          char ** tml = (char **)dlsym(sh_file_handle,"proc_tuple_member_labels");
          char ** tcl = (char **)dlsym(sh_file_handle,"proc_tuple_container_labels");
          char ** tccl = (char **)dlsym(sh_file_handle,"proc_tuple_conditional_container_labels");

          title(fp, "PROCESSOR NAME");
          if (proc_purpose) {
               // concat the name and purpose into one string for word wrapping
               int len = strlen(proc_name) + strlen(proc_purpose) + 3;
               if (is_deprecated) {
                    int dep_len = strlen("DEPRECATED: ");
                    len +=dep_len;
               }
               char * name_purpose = malloc(len+1);
               if (!name_purpose) {
                    error_print("failed print_module_help calloc of name_purpose");
                    return 0;
               }
               if (is_deprecated) {
                    snprintf(name_purpose, len+1, "%s - DEPRECATED: ", proc_name);
               }
               else {
                    snprintf(name_purpose, len+1, "%s - ", proc_name);
               }
               strncat(name_purpose, proc_purpose, strlen(proc_purpose)+1);
               print_wrap(fp, name_purpose, WRAP_WIDTH, 4);
               free(name_purpose);
               fprintf(fp, "\n");
          } else {
               if (is_deprecated) {
                   fprintf(fp, "\t%s - DEPRECATED", proc_name);
               }
               else { 
                    fprintf(fp, "\t%s - ", proc_name);
               }
               fprintf(fp, "\n");
          }
          if (proc_synopsis) {
               title(fp, "SYNOPSIS");
//               print_list(fp, proc_synopsis);
               int i = 0;
               while (proc_synopsis[i] != NULL){
                    print_wrap(fp, proc_synopsis[i], WRAP_WIDTH, 4);
                    i++;
               }
               fprintf(fp, "\n");
          }
          if (proc_description && verbose) {
               title(fp, "DESCRIPTION");
               print_wrap(fp, proc_description, WRAP_WIDTH, 4);
               fprintf(fp, "\n");
          }
          if (proc_tags && proc_tags[0] && verbose) {
               title(fp, "TAGS");
               print_list(fp, proc_tags);
               fprintf(fp, "\n");
          }
          if (examples && verbose) {
               title(fp, "EXAMPLES");
               int i = 0;
               while (examples[i].text != NULL) {
                    if (examples[i].text[0] != '\0') {
                         print_wrap(fp, examples[i].text, WRAP_WIDTH, 4);
                         print_wrap(fp, examples[i].description, WRAP_WIDTH, 8);
                    }
                    i++;
               }
               fprintf(fp, "\n");
          }
          if (proc_alias && proc_alias[0] && verbose) {
               title(fp, "ALIAS");
               print_list(fp, proc_alias);
               fprintf(fp, "\n");
          }
          if (verbose) {
               title(fp, "VERSION");
               fprintf(fp, "    %s\n", proc_version);
               fprintf(fp, "\n");
          }
          if (proc_requires && verbose) {
               title(fp, "REQUIRES");
               print_wrap(fp, proc_requires, WRAP_WIDTH, 4);
               fprintf(fp, "\n");
          }
          if (itypes && verbose) {
               title(fp, "INPUT TYPES");
               print_list(fp, itypes);
               fprintf(fp, "\n");
          }
          if (otypes && verbose) {
               title(fp, "OUTPUT TYPES");
               print_list(fp, otypes);
               fprintf(fp, "\n");
          }
          if (ports) {
               title(fp, "INPUT PORTS");
               print_portlist(fp, ports);
               fprintf(fp, "\n");
          }
          if (tcl && verbose) {
               title(fp, "TUPLE CONTAINER LABELS");
               print_list(fp, tcl);
               fprintf(fp, "\n");
          }
          if (tccl && verbose) {
               title(fp, "TUPLE CONDITIONAL CONTAINER LABELS");
               print_list(fp, tccl);
               fprintf(fp, "\n");
          }
          if (tml && verbose) {
               title(fp, "APPENDED TUPLE MEMBERS");
               print_list(fp, tml);
               fprintf(fp, "\n");
          }
          if (opt || ns_opt) {
               title(fp, "OPTIONS");
               if (ns_opt && strlen(ns_opt) > 0) {
                    int len = strlen(ns_opt) + 2; 
                    char * ns = (char *) malloc(len+1);
                    if (!ns) {
                         error_print("failed print_module_help calloc of ns");
                         return 0;
                    }
                    snprintf(ns, len+1, "<%s>", ns_opt); 
                    print_wrap(fp, ns, WRAP_WIDTH, 4);
                    free(ns);
               }
               if (opt) {
                    int i = 0;
                    while (opt[i].option != ' ') {
                         int sz = 1024;
                         char * buf = (char *) calloc(1,sz);
                         char * p = buf;
                         
                         int read = 0;
                         if (opt[i].option != '\0') {
                              read = snprintf(p, 4, "[-%c", opt[i].option);  
                         }
		               else {
                              while ((read+3+strlen(opt[i].long_option)+1) > sz) {
                                   sz += 1024;
                                   buf = (char *) realloc(buf, sz);
                                   if (!buf) {
                                        error_print("failed opt.long_option realloc"); 
                                   }
                                   p = buf+read;
                              }
                              read = snprintf(p,3+strlen(opt[i].long_option)+1,
                                   "[--%s", opt[i].long_option);
                         }
                         p = p + read;
                         if (opt[i].argument && strlen(opt[i].argument)) {
                              while ((read+3+strlen(opt[i].argument)+1) > sz) {
                                   sz += 1024;
                                   buf = (char *) realloc(buf, sz);
                                   if (!buf) {
                                        error_print("failed opt.argument realloc"); 
                                   }
                                   p = buf+read;
                              }
                              read = snprintf(p,3+strlen(opt[i].argument) + 1,
                                   " <%s>", opt[i].argument);
                              p = p + read;
                         }

                         while ((read+2+strlen(opt[i].description)+1) > sz) {
                             sz += 1024;
                             buf = (char *) realloc(buf, sz);
                             if (!buf) {
                                  error_print("failed opt.description realloc"); 
                             }
                             p = buf+read;
                         }
                         snprintf(p,2+strlen(opt[i].description)+1,
                              "] %s", opt[i].description);

                         print_wrap(fp, buf, WRAP_WIDTH, 4);
                         i++;
                         free(buf);
                    }
               }
          }
          // close this shared-module handle
          dlclose(sh_file_handle);
          return 1;
     }
     else {
          return 0;
     }
}

int print_module_help_search(FILE * fp, const char *spath, char * keyword)  {
     void * sh_file_handle;
     //snprintf(spath,255,"%s/module_%s.so", g_pLibPath, modulename);
     if (spath == NULL) { return 0;}

     // try to locate the passed module name
     if ((sh_file_handle = dlopen(spath, RTLD_LAZY|RTLD_LOCAL))) {
          // find out if the module is deprecated
          int * is_deprecated = (int *)dlsym(sh_file_handle,"is_deprecated");
          // get the information from the module that we need to print help
          char ** proc_alias  = (char**)dlsym(sh_file_handle,"proc_alias");
          char * proc_name    = (char *)dlsym(sh_file_handle,"proc_name");
          char * proc_purpose = (char *)dlsym(sh_file_handle,"proc_purpose");
          char ** proc_synopsis  = (char**)dlsym(sh_file_handle,"proc_synopsis");
          char * proc_description = (char *)dlsym(sh_file_handle,"proc_description");
          char ** proc_tags  = (char**)dlsym(sh_file_handle,"proc_tags");
          proc_example_t * examples = (proc_example_t*)dlsym(sh_file_handle,"proc_examples"); 
          char * proc_requires = (char *)dlsym(sh_file_handle,"proc_requires");
          char * proc_version = (char *)dlsym(sh_file_handle,"proc_version");
          proc_option_t * opt = (proc_option_t*)dlsym(sh_file_handle,"proc_opts"); 
          char * ns_opt = (char*)dlsym(sh_file_handle,"proc_nonswitch_opts"); 
          char ** itypes = (char**)dlsym(sh_file_handle,"proc_input_types"); 
          char ** otypes = (char**)dlsym(sh_file_handle,"proc_output_types");

          proc_port_t * ports = (proc_port_t*)dlsym(sh_file_handle,"proc_input_ports");

          char ** tml = (char **)dlsym(sh_file_handle,"proc_tuple_member_labels");
          char ** tcl = (char **)dlsym(sh_file_handle,"proc_tuple_container_labels");
          char ** tccl = (char **)dlsym(sh_file_handle,"proc_tuple_conditional_container_labels");

          title(fp, "PROCESSOR NAME");
          if (proc_purpose) {
               // concat the name and purpose into one string for word wrapping
               char * name_purpose, * p;
               if (is_deprecated) {
                    _asprintf(&name_purpose, "%s - DEPRECATED: %s", proc_name, proc_purpose);
               }
               else {
                    _asprintf(&name_purpose, "%s - %s", proc_name, proc_purpose); 
               }
               p = name_purpose;
               name_purpose = search_and_highlight(outfp,name_purpose, keyword);
               if (name_purpose) { free(p); } else { name_purpose = p;}
               print_wrap(fp, name_purpose, WRAP_WIDTH, 4);
               free(name_purpose);
          } else { 
               char * name_purpose, * p;
               if (is_deprecated) {
                    _asprintf(&name_purpose, "%s - DEPRECATED", proc_name);
               }
               else {
                    _asprintf(&name_purpose, "%s -", proc_name); 
               }
               p = name_purpose;
               name_purpose = search_and_highlight(outfp,name_purpose, keyword);
               if (name_purpose) { free(p); } else { name_purpose = p;}
               print_wrap(fp, name_purpose, WRAP_WIDTH, 4);
               free(name_purpose);
          }
          if (proc_synopsis) {
               title(fp, "SYNOPSIS");
               print_list_search(fp, proc_synopsis, keyword);
               fprintf(fp, "\n");
          }
          if (proc_description) {
               title(fp, "DESCRIPTION");
               char * p = proc_description;
               proc_description = search_and_highlight(outfp,proc_description, keyword);
               if (!proc_description) proc_description = p;
               print_wrap(fp, proc_description, WRAP_WIDTH, 4);
               fprintf(fp, "\n");
          }
          if (proc_tags && proc_tags[0]) {
               title(fp, "TAGS");
               print_list_search(fp, proc_tags, keyword);
               fprintf(fp, "\n");
          }
          if (examples) {
               title(fp, "EXAMPLES");
               int i = 0;
               while (examples[i].text != NULL) {
                    if (examples[i].text[0] != '\0') {
                         char * p;
                         char * q;
                         highlight(p, (char *) examples[i].text, keyword, fp, WRAP_WIDTH, 4);
                         highlight(q, (char *) examples[i].description, keyword, fp, WRAP_WIDTH, 8);
                    }
                    i++;
               }
               fprintf(fp, "\n");
          }
          if (proc_alias && proc_alias[0]) {
               title(fp, "ALIAS");
               print_list_search(fp, proc_alias, keyword);
               fprintf(fp, "\n");
          }
          title(fp, "VERSION");
          if (proc_version) {
               fprintf(fp, "    %s\n", proc_version);
               fprintf(fp, "\n");
          }
          if (proc_requires) {
               title(fp, "REQUIRES");
               char * p = proc_requires;
               proc_requires = search_and_highlight(outfp,proc_requires, keyword);
               if (!proc_requires) proc_requires = p;
               print_wrap(fp, proc_requires, WRAP_WIDTH, 4);
               fprintf(fp, "\n");
          }
          if (itypes) {
               title(fp, "INPUT TYPES");
               print_list_search(fp, itypes, keyword);
               fprintf(fp, "\n");
          }
          if (otypes) {
               title(fp, "OUTPUT TYPES");
               print_list_search(fp, otypes, keyword);
               fprintf(fp, "\n");
          }
          if (ports) {
               title(fp, "INPUT PORTS");
               print_portlist_search(fp, ports, keyword);
               fprintf(fp, "\n");
          }
          if (tcl) {
               title(fp, "TUPLE CONTAINER LABELS");
               print_list_search(fp, tcl, keyword);
               fprintf(fp, "\n");
          }
          if (tccl) {
               title(fp, "TUPLE CONDITIONAL CONTAINER LABELS");
               print_list_search(fp, tccl, keyword);
               fprintf(fp, "\n");
          }
          if (tml) {
               title(fp, "APPENDED TUPLE MEMBERS");
               print_list_search(fp, tml, keyword);
               fprintf(fp, "\n");
          }
          if (opt || ns_opt) {
               title(fp, "OPTIONS");
               if (ns_opt && strlen(ns_opt) > 0) {
                    int len = strlen(ns_opt) + 2; 
                    char * ns = (char *) malloc(len+1);
                    if (!ns) {
                         error_print("failed print_module_help_search calloc of ns");
                         return 0;
                    }
                    snprintf(ns, len+1, "<%s>", ns_opt); 
                    char *p;
                    highlight(p, ns, keyword, fp, WRAP_WIDTH, 4);
                    free(ns);
               }
               if (opt) {
                    int i = 0;
                    while (opt[i].option != ' ') {
                         int sz = 1024;
                         char * buf = (char *) calloc(1,sz);
                         char * p = buf;
                         int read = 0;
                         if (opt[i].option != '\0') {
                              read = snprintf(p, 4, "[-%c", opt[i].option);  
                         }
		         else {
                            while ((read+3+strlen(opt[i].long_option)+1) > sz) {
                                   sz += 1024;
                                   buf = (char *) realloc(buf, sz);
                                   if (!buf) {
                                        error_print("failed opt.long_option realloc"); 
                                   }
                                   p = buf+read;
                              }
                              read = snprintf(p,3+strlen(opt[i].long_option)+1,
                                   "[--%s", opt[i].long_option);
                         }
                         p = p + read;
                         if (opt[i].argument && strlen(opt[i].argument)) {
                              while ((read+3+strlen(opt[i].argument)+1) > sz) {
                                   sz += 1024;
                                   buf = (char *) realloc(buf, sz);
                                   if (!buf) {
                                        error_print("failed opt.argument realloc"); 
                                   }
                                   p = buf+read;
                              }
                              read = snprintf(p,3+strlen(opt[i].argument)+1,
                                   " <%s>", opt[i].argument);
                              p = p + read;
                         }
                         while ((read+2+strlen(opt[i].description)+1) > sz) {
                             sz += 1024;
                             buf = (char *) realloc(buf, sz);
                             if (!buf) {
                                  error_print("failed opt.description realloc"); 
                             }
                             p = buf+read;
                         }

                         snprintf(p,2+strlen(opt[i].description)+1,
                              "] %s", opt[i].description);
                         char * ptr;
                         highlight(ptr, buf, keyword, fp, WRAP_WIDTH, 4);
                         i++;
                    }
               }
          }

          // close this shared-module handle
          dlclose(sh_file_handle);
          return 1;
     }
     else {
          fprintf(stderr,"%s\n", dlerror());
          fprintf(stderr,"unable to open module %s\n", spath);
          return 0;
     }
}

int print_module_help_short(FILE * fp, const char *spath) {
     void * sh_file_handle;
     if (spath == NULL) {
          return 0;
     }
     // try to locate the passed module name
     if ((sh_file_handle = dlopen(spath, RTLD_LAZY|RTLD_LOCAL))) {
          // find out if module is deprecated
          int * is_deprecated = (int *)dlsym(sh_file_handle,"is_deprecated");
          char * proc_name    = (char *) dlsym(sh_file_handle,"proc_name");
          char * proc_purpose = (char *) dlsym(sh_file_handle,"proc_purpose");
          if (proc_purpose) {
               if (is_deprecated) {
                    fprintf(fp, "%-20s\t- DEPRECATED: %s", proc_name, proc_purpose);
               }
               else {
                    fprintf(fp, "%-20s\t- %s", proc_name, proc_purpose);
               }
               fprintf(fp, "\n");
          } else {
               if (is_deprecated) {
                    fprintf(fp, "%-20s\t- DEPRECATED", proc_name);
               }
               else {
                    fprintf(fp, "%-20s\t-", proc_name);
               }
               fprintf(fp, "\n");
          }

          dlclose(sh_file_handle);
          return 1;
     }
     else {
          fprintf(stderr,"%s\n", dlerror());
          fprintf(stderr,"unable to open module %s\n", spath);
          return 0;
     }
}
// find the right .so filename to load as a datatype
int datatype_file_filter(const struct dirent * entry) {
     int len;
     if (strncmp(entry->d_name, "proc_", 5) == 0) {
          len = strlen(entry->d_name);
          if ((strncmp(entry->d_name+(len-5),"ws_so", 5) == 0) ||
              (strncmp(entry->d_name+(len-6),"wsp_so",6) == 0)) {
               return 1;
          }
     }
     return 0;
}

int load_proc_dir_term(const char * dirname, char * term) {
     struct dirent **namelist;
     int rtn = 0;

     int n;
     char spath[5000];

     if (dirname == NULL) {
          fprintf(stderr,"no datatype path.. set environment");
          return 0;
     }
     int dirlen = strlen(dirname);

     n = scandir(dirname, &namelist, datatype_file_filter, alphasort);

     if (n < 0) {
          perror("scandir");
     } else {
          memcpy(spath, dirname, dirlen);
          spath[dirlen] = '/';
          int namelen;
          int i;
          int divider = 0;
          for (i = 0; i < n; i++) {
               int index = strchr(namelist[i]->d_name, '.') - namelist[i]->d_name;
               char * kid = strndup(namelist[i]->d_name, index);

               // Check if the last kid we found was the serial version of
               // the same kid. We don't want to process the same kid twice.
               if (last_kid) {
                    int same_kid = (strcmp(kid, last_kid) == 0);
                    free(last_kid);
                    last_kid = kid;
                    if (same_kid) continue;
               }
               last_kid = kid;

               namelen = strlen(namelist[i]->d_name);
               memcpy(spath + dirlen + 1, namelist[i]->d_name, namelen);
               spath[dirlen+namelen + 1] = '\0';

               if (tag_search && search_for_tag(spath, term)) {
                    if (verbose) {
                         if (divider) {
                              print_divider(outfp);
                              divider = 0;
                         }
                         print_module_help(outfp, spath);
                         divider = 1;
                         fprintf(outfp,"\n");
                    } else {
                         print_module_help_short(outfp, spath);
                    }
               }
               if (input_type_search && search_for_input_type(spath, term)) {
                    if (verbose) {
                         if (divider) {
                              print_divider(outfp);
                              divider = 0;
                         }
                         print_module_help(outfp, spath);
                         divider = 1;
                         fprintf(outfp,"\n");
                    } else {
                         print_module_help_short(outfp, spath);
                    }
               }
               if (output_type_search && search_for_output_type(spath, term)) {
                    if (verbose) {
                         if (divider) {
                              print_divider(outfp);
                              divider = 0;
                         }
                         print_module_help(outfp, spath);
                         divider = 1;
                         fprintf(outfp,"\n");
                    } else {
                         print_module_help_short(outfp, spath);
                    }
               }
               if (keyword_search && search_for_keyword(spath, term)) {
                    if (divider) {
                         print_divider(outfp);
                         divider = 0;
                    }
                    print_module_help_search(outfp, spath, term);
                    divider = 1;
                    fprintf(outfp,"\n");
               }
               free(namelist[i]);
          }
          free(namelist);
     }
     return rtn; //number of libraries loaded
}

int load_proc_dir(const char * dirname) {

     struct dirent **namelist;
     int rtn = 0;

     int n;
     char spath[5000];

     if (dirname == NULL) {
          fprintf(stderr,"no datatype path.. set environment");
          return 0;
     }
     int dirlen = strlen(dirname);

     n = scandir(dirname, &namelist, datatype_file_filter, alphasort);

     if (n < 0) {
          perror("scandir");
     } else {
          memcpy(spath, dirname, dirlen);
          spath[dirlen] = '/';
          int namelen;
          int i;
          for (i = 0; i < n; i++) {
               int index = strchr(namelist[i]->d_name, '.') - namelist[i]->d_name;
               char * kid = strndup(namelist[i]->d_name, index);
               // Check if the last kid we found was the serial version of
               // the same kid. We don't want to process the same kid twice.
               if (last_kid) {
                    int same_kid = (strcmp(kid, last_kid) == 0);
                    free(last_kid);
                    last_kid = kid;
                    if (same_kid) continue;
               }
               last_kid = kid;

               namelen = strlen(namelist[i]->d_name);
               memcpy(spath + dirlen + 1, namelist[i]->d_name, namelen);
               spath[dirlen+namelen + 1] = '\0';
               
               //print everything
               if (verbose) {
                    print_module_help(outfp, spath);
                    if ((i+1) < n) print_divider(outfp);
               // man -k style
               } else {
                    print_module_help_short(outfp, spath);
               }

               free(namelist[i]);
          }
          free(namelist);
     }
     return rtn; //number of libraries loaded
}

#define DIRSEP_STR ":"
void load_multiple_dirs(char * libpath) {
     char * buf = libpath;
     char * dir = strsep(&buf, DIRSEP_STR);

     while (dir) {
          load_proc_dir(dir);
          dir = strsep(&buf, DIRSEP_STR);
     }
}

void load_multiple_dirs_term(char * libpath, char * term) {
     char * buf = libpath;
     char * dir = strsep(&buf, DIRSEP_STR);

     while (dir) {
          load_proc_dir_term(dir, term);
          dir = strsep(&buf, DIRSEP_STR);
     }
}


void find_specific_module(char * kid, char * libpath) {
     char * buf = libpath;
     char * dir = strsep(&buf, DIRSEP_STR);

     while (dir) {
          char * module_path_serial   = get_module_path(kid, dir);
          char * module_path_parallel = get_module_path_parallel(kid, dir);
          if (check_kid) {
               if (check_kid_documentation(outfp, module_path_serial, kid)) {
                    if (module_path_serial) {
                         free(module_path_serial);
                    }
                    if (module_path_parallel) {
                         free(module_path_parallel);
                    }
                    return;
               }
               if (check_kid_documentation(outfp, module_path_parallel, kid)) {
                    if (module_path_serial) {
                         free(module_path_serial);
                    }
                    if (module_path_parallel) {
                         free(module_path_parallel);
                    }
                    return;
               }

               // our previous lookups failed: let's see if this is an alias
               // for another kid
               char * aliased_kid = map_find(map, kid);

               if (aliased_kid) {
                    char * module_path_serial_alias =
                         get_module_path(aliased_kid, dir);
                    char * module_path_parallel_alias =
                        get_module_path_parallel(aliased_kid, dir);

                    if (check_kid_documentation(outfp, module_path_serial_alias, kid)) {
                         if (module_path_serial) {
                              free(module_path_serial);
                         }
                         if (module_path_parallel) {
                              free(module_path_parallel);
                         }
                         if (module_path_serial_alias) {
                              free(module_path_serial_alias);
                         }
                         if (module_path_parallel_alias) {
                              free(module_path_parallel_alias);
                         }
                         return;
                    }

                    if (check_kid_documentation(outfp, module_path_parallel_alias, kid)) {
                         if (module_path_serial) {
                              free(module_path_serial);
                         }
                         if (module_path_parallel) {
                              free(module_path_parallel);
                         }
                         if (module_path_serial_alias) {
                              free(module_path_serial_alias);
                         }
                         if (module_path_parallel_alias) {
                              free(module_path_parallel_alias);
                         }
 
                         return;
                    }
               }

               fprintf(stderr, "FAILURE: could not find %s\n", kid);
          } else {
               if (print_module_help(outfp, module_path_serial)) {
                    if (module_path_serial) {
                         free(module_path_serial);
                    }
                    if (module_path_parallel) {
                         free(module_path_parallel);
                    }
                    return;
               }
               if (print_module_help(outfp, module_path_parallel)) {
                    if (module_path_serial) {
                         free(module_path_serial);
                    }
                    if (module_path_parallel) {
                         free(module_path_parallel);
                    }
                    return;
               }

               // couldn't find kid so far - is this an alias?
               char * aliased_kid = map_find(map, kid);

               if (aliased_kid) {
                    // Yep, this is an alias
                    char * module_path_serial_alias =
                         get_module_path(aliased_kid, dir);
                    char * module_path_parallel_alias =
                        get_module_path_parallel(aliased_kid, dir);

                    if (print_module_help(outfp, module_path_serial_alias)) {
                         if (module_path_serial) {
                              free(module_path_serial);
                         }
                         if (module_path_parallel) {
                              free(module_path_parallel);
                         }
                         if (module_path_serial_alias) {
                              free(module_path_serial_alias);
                         }
                         if (module_path_parallel_alias) {
                              free(module_path_parallel_alias);
                         }
                         return;
                    }
                    if (print_module_help(outfp, module_path_parallel_alias)) {
                         if (module_path_serial) {
                              free(module_path_serial);
                         }
                         if (module_path_parallel) {
                              free(module_path_parallel);
                         }
                         if (module_path_serial_alias) {
                              free(module_path_serial_alias);
                         }
                         if (module_path_parallel_alias) {
                              free(module_path_parallel_alias);
                         }
                         return;
                    }
               }

               // we've done our best, but we can't find this kid.
               fprintf(stderr,"%s\n", dlerror());
               fprintf(stderr,"unable to open module %s\n", kid);
          }
         
          if (module_path_serial) {
               free(module_path_serial);
          }
          if (module_path_parallel) {
               free(module_path_parallel);
          }
          dir = strsep(&buf, DIRSEP_STR);
     }
}

int build_alias_map(char * alias_path) {
     char buf[1000];

     map = map_create();
     if (!map) return 1;

     FILE * fp = fopen(alias_path, "r");
     if (!fp) return 1;

     while (fgets(buf, 1000, fp)) {
          char * line = trim(buf);
          char * kid_name = line;
          char * aliases = strstr(line, ":");
          // replace the ":" with a null-terminator to split the string
          *aliases= 0;
          aliases+= 2;
          char * token = strtok (aliases, " ,");
          while (token != NULL) {
               map_insert(map, token, kid_name);
               token = strtok (NULL, " ,");
          }
     }

     fclose(fp);

     return 0;
}


// entry point (possibly receive a quoted command line
int main(int argc, char *argv[]) {

     char * alias_path;
     char * lib_path;

     char * pager= getenv("PAGER");
     if (pager && strlen(pager) > 0) {
         if(NULL == (outfp = popen(pager, "w"))) {
             fprintf(stderr, "pipe-open on pager failed...defaulting to stdout\n");
             outfp = stdout;
         }
     } else {
         outfp = stdout;
     }

     set_default_env();

     if ((lib_path = getenv(ENV_WS_PROC_PATH)) == NULL) {
          fprintf(stderr, "Please set the %s path variable... ", 
		  ENV_WS_PROC_PATH); 
          fprintf(stderr,"taking a guess\n\n"); 
          lib_path = "./procs";
     }

     if ((alias_path = getenv(ENV_WS_ALIAS_PATH)) == NULL) {
          fprintf(stderr, "Please set the %s path variable... ",
                 ENV_WS_ALIAS_PATH);
          fprintf(stderr,"taking a guess\n\n");
          alias_path = "./procs/.wsalias";
     }

     build_alias_map(alias_path);

     int op;
     int force_verbose = 0;
     while ((op = getopt(argc, argv, "rcsiotvVhmM?")) != EOF) {
          switch (op) {
               case 'c':
                    check_kid = 1;
                    break;
               case 's':
                    keyword_search = 1;
                    break;
               case 't':
                    tag_search = 1;
                    break;
               case 'i':
                    input_type_search = 1;
                    break;
               case 'o':
                    output_type_search = 1;
                    break;
               case 'r':
                    generate_rst = 1;
                    break;
               case 'v':
               case 'V':
                    verbose = 1;
                    force_verbose = 1;
                    break;
               case 'm':
               case 'M':
                    verbose = 0;
                    break;
               case 'h':
               case '?':
                    print_usage(outfp);
                    return 0;
                    break;
               default:
                    return 0;
               }
     }

     if (!verbose && !generate_rst) {
          print_helpful_message(stderr);
     }

     argc -= optind;
     argv += optind;

     if (!argc && !force_verbose) {
          verbose = 0;
     }

     if (argc > 0) {
          int x;
          for (x = 0; x < argc; x++) {
               if (x > 1) {
                    fprintf(outfp, "\n");
               }
               char * p = argv[x];
               for ( ; *p; p++) *p = tolower(*p);

               if (keyword_search) {
                    fprintf(outfp,"Searching for \"%s\"...\n\n", argv[x]);
                    load_multiple_dirs_term(lib_path, argv[x]);
                    //can only search a single term, so break after first iter
                    break;
               } else if (check_kid) {
                    find_specific_module(argv[x], lib_path);
               } else if (tag_search) {
                    load_multiple_dirs_term(lib_path, argv[x]);
               } else if (input_type_search) {
                    load_multiple_dirs_term(lib_path, argv[x]);
               } else if (output_type_search) {
                    load_multiple_dirs_term(lib_path, argv[x]);
               } else {
                    find_specific_module(argv[x], lib_path);
               }
               fflush(outfp);
          }
     } else if (keyword_search) {
          fprintf(stderr, "ERROR: Keyword search requires an argument\n");
     } else if (check_kid) {
          fprintf(stderr, "ERROR: Kid check requires an argument\n");
     } else if (tag_search) {
          fprintf(stderr, "ERROR: Tag search requires an argument\n");
     } else if (input_type_search) {
          fprintf(stderr, "ERROR: Input type search requires an argument\n");
     } else if (output_type_search) {
          fprintf(stderr, "ERROR: Output type search requires an argument\n");
     } else {
          //print all modules in path
          load_multiple_dirs(lib_path);
     }

     if(pclose(outfp) == -1) { // stream not associated with a popen
          fclose(outfp);
     }
     return 0;
}
#ifdef __cplusplus
CPP_CLOSE
#endif

