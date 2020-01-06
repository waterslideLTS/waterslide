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

#include "waterslide.h"
#include "shared/getrank.h"
#include "shared/lock_init.h"
#include "stringhash5.h"
#include "stringhash9a.h"
#include "sht_expire_cnt.h"
#include "sht_registry.h"

#define MAX_CHARS_TYPE 12 // space for longest table type: sh9a_shared
#define MAX_CHARS_NAME 28 // space for longest kid name: keyadd_initial_custom_shared
#define MAX_CHARS_LABEL 12 // max table size reported will be (10^12 - 1) bytes

// Globals
sht_registry_t * sh_registry;
sht_registry_t ** loc_registry;
uint32_t max_sh_tables = 10, max_loc_tables = 10;
uint32_t n_sh_register;
uint32_t * n_loc_register;
extern uint32_t work_size;
extern const char ** global_kid_name;


int init_sht_registry(void) {

     uint32_t i;

     sh_registry = (sht_registry_t *)calloc(max_sh_tables, sizeof(sht_registry_t));
     if (!sh_registry) {
          error_print("failed init_sht_registry calloc of sh_registry");
          return 0;
     }

     n_loc_register = (uint32_t *)calloc(work_size, sizeof(uint32_t));
     if (!n_loc_register) {
          error_print("failed init_sht_registry calloc of n_loc_register");
          return 0;
     }
     loc_registry = (sht_registry_t **)calloc(work_size, sizeof(sht_registry_t *));
     if (!loc_registry) {
          error_print("failed init_sht_registry calloc of loc_registry");
          return 0;
     }
     for (i = 0; i < work_size; i++) {
          loc_registry[i] = (sht_registry_t *)calloc(max_loc_tables, 
                                                 sizeof(sht_registry_t));
          if (!loc_registry[i]) {
               error_print("failed init_sht_registry calloc of sh_registry[i]");
               return 0;
          }
     }

     // array for holding kid names during graph construction
     global_kid_name = (const char **)calloc(sizeof(char *), work_size);
     if (!global_kid_name) {
          error_print("failed init_sht_registry calloc of global_kid_name");
          return 0;
     }

     return 1;
}

void free_sht_registry(void) {

     uint32_t i, j;

     for (i = 0; i < work_size; i++) {
          for (j = 0; j < n_loc_register[i]; j++) {
               free(loc_registry[i][j].sh_type);
               free(loc_registry[i][j].sh_kidname);
          }
          free(loc_registry[i]);
     }
     free(loc_registry);
     free(n_loc_register);
     for (i = 0; i < n_sh_register; i++) {
          free(sh_registry[i].sh_type);
          free(sh_registry[i].sh_kidname);
          free(sh_registry[i].sh_label);
     }
     free(sh_registry);

     free(global_kid_name);
}

void save_proc_name(const char *proc_name) {
     const int nrank = GETRANK();

     //store the proc_name for retrieval by enroll_in_sht_registry
     global_kid_name[nrank] = proc_name;
}

int move_sht_to_local_registry(void * sht, int * index) {
     int i;

     if (sht != sh_registry[*index].sht) {
          error_print("move_sht_to_local_registry: entry %d, sht %p != input value %p",
                      *index, sh_registry[*index].sht, sht);
          return 0;
     }
     else {
          // enroll as a local hash table
          save_proc_name(sh_registry[*index].sh_kidname);
          if(!enroll_in_sht_registry(sht, sh_registry[*index].sh_type, sh_registry[*index].size, 
                           sh_registry[*index].hash_seed)) {
               error_print("move_sht_to_local_registry: entry %d, sht %p failed enroll_in_sht_registry",
                           *index, sht);
               return 0;
          }

          // disenroll as a shared hash table
          for (i = *index+1; i < n_sh_register; i++) {
               strcpy(sh_registry[i-1].sh_type, sh_registry[i].sh_type);
               strcpy(sh_registry[i-1].sh_kidname, sh_registry[i].sh_kidname);
               strcpy(sh_registry[i-1].sh_label, sh_registry[i].sh_label);
               sh_registry[i-1].sht = sh_registry[i].sht;
               sh_registry[i-1].size = sh_registry[i].size;
               sh_registry[i-1].hash_seed = sh_registry[i].hash_seed;
          }
          free(sh_registry[n_sh_register-1].sh_type);
          free(sh_registry[n_sh_register-1].sh_kidname);
          free(sh_registry[n_sh_register-1].sh_label);
          sh_registry[n_sh_register-1].sht = NULL;
          sh_registry[n_sh_register-1].size = 0;
          sh_registry[n_sh_register-1].hash_seed = 0;
          n_sh_register--;
     }

     return 1;
}

// If work_size == 1, walk through all shared stringhash tables,
// make them local in the sht_registry, and free their mutexes 
// and all other shared data (i.e. overhead) formerly associated 
// with them.  Otherwise, do the same steps for each stringhash
// table that has a table->sharedata->cnt of 1.
int verify_shared_tables(void) {
     void * sht;
     char * sh_type;
     int index = 0;
     fprintf(stderr,"Verifying shared hash tables\n");

     // loop over the sht's here - a new function is used to get the
     // addresses of the shared table
     if (index < n_sh_register) {
          sht = sh_registry[index].sht;
          sh_type = sh_registry[index].sh_type;
     
          while (sht) {
               if (strncmp(sh_type, "sh5", 3) == 0) {
                    if (!stringhash5_clean_sharing(sht, &index)) {
                         return 0;
                    }
               }
               else if (strncmp(sh_type, "sh9a", 4) == 0) {
                    if (!stringhash9a_clean_sharing(sht, &index)) {
                         return 0;
                    }
               }
               else {
                    index++;
               }

               // next
               if (index < n_sh_register) {
                    sht = sh_registry[index].sht;
                    sh_type = sh_registry[index].sh_type;
               }
               else {
                    sht = NULL;
               }
          }
     }

     return 1;
}

int enroll_shared_in_sht_registry(void * sht, const char * sh_type, const char * sh_label, 
                                  const uint64_t size, const uint32_t hash_seed) {
     const int nrank = GETRANK();

     // Expand the rows of the registry, if necessary
     if (n_sh_register >= max_sh_tables) {
          max_sh_tables += 10;
          sh_registry = (sht_registry_t *)realloc(sh_registry, max_sh_tables* 
                                                  sizeof(sht_registry_t));
          if (!sh_registry) {
               error_print("failed enroll_shared_in_sht_registry realloc of sh_registry");
               return 0;
          }
     }

     sh_registry[n_sh_register].sht = sht;
     sh_registry[n_sh_register].size = size;
     // sh3, which has no hash seed, is not a shared hash table, so the following is ok
     sh_registry[n_sh_register].hash_seed = hash_seed;
     sh_registry[n_sh_register].sh_type = (char *)calloc(MAX_CHARS_TYPE, sizeof(char));
     if (!sh_registry[n_sh_register].sh_type) {
          error_print("failed enroll_shared_in_sht_registry calloc of sh_registry[n_sh_register].sh_type");
          return 0;
     }
     sh_registry[n_sh_register].sh_kidname = (char *)calloc(MAX_CHARS_NAME, sizeof(char));
     if (!sh_registry[n_sh_register].sh_kidname) {
          error_print("failed enroll_shared_in_sht_registry calloc of sh_registry[n_sh_register].sh_kidname");
          return 0;
     }
     sh_registry[n_sh_register].sh_label = (char *)calloc(MAX_CHARS_LABEL, sizeof(char));
     if (!sh_registry[n_sh_register].sh_label) {
          error_print("failed enroll_shared_in_sht_registry calloc of sh_registry[n_sh_register].sh_label");
          return 0;
     }

     uint32_t cpsz = strlen(sh_type);

     if (cpsz < MAX_CHARS_TYPE) {
          strcpy(sh_registry[n_sh_register].sh_type, sh_type);
     } else {
          strncpy(sh_registry[n_sh_register].sh_type, sh_type, MAX_CHARS_TYPE);
     }

     if (global_kid_name[nrank]) {
          cpsz = strlen(global_kid_name[nrank]);
     }
     else {
          cpsz = 0;
     }
     if (!cpsz) {
          strcpy(sh_registry[n_sh_register].sh_kidname, strdup("kid name not assigned"));
     }
     else if (cpsz < MAX_CHARS_NAME) {
          strcpy(sh_registry[n_sh_register].sh_kidname, global_kid_name[nrank]);
     } else {
          strncpy(sh_registry[n_sh_register].sh_kidname, global_kid_name[nrank], MAX_CHARS_NAME);
     }

     cpsz = strlen(sh_label);
     if (cpsz < MAX_CHARS_LABEL) {
          strcpy(sh_registry[n_sh_register].sh_label, sh_label);
     } else {
          strncpy(sh_registry[n_sh_register].sh_label, sh_label, MAX_CHARS_LABEL);
     }

     n_sh_register++;

     return 1;
}

int enroll_in_sht_registry(void * sht, const char * sh_type, const uint64_t size, 
                           const uint32_t hash_seed) {

     const int nrank = GETRANK();
     uint32_t n_loc_registered = n_loc_register[nrank];
     int i;

     // Expand the rows of the registry, if necessary
     if (n_loc_registered >= max_loc_tables) {
          max_loc_tables += 10;
          for (i = 0; i < work_size; i++) {
               loc_registry[i] = (sht_registry_t *)realloc(loc_registry[i], max_loc_tables* 
                                                           sizeof(sht_registry_t));
               if (!loc_registry[i]) {
                    error_print("failed enroll_in_sht_registry realloc of loc_registry[i]");
                    return 0;
               }
          }
     }

     loc_registry[nrank][n_loc_registered].sht = sht;
     loc_registry[nrank][n_loc_registered].size = size;
     loc_registry[nrank][n_loc_registered].sh_type = (char *)calloc(MAX_CHARS_TYPE, sizeof(char));
     // sh3 has no hash_seed
     if (strncmp(sh_type, "sh3", 3) == 0) {
          loc_registry[nrank][n_loc_registered].hash_seed = 0;
     }
     // sh5 and sh9a have a hash_seed, which will be reported
     else {
          loc_registry[nrank][n_loc_registered].hash_seed = hash_seed;
     }
     if (!loc_registry[nrank][n_loc_registered].sh_type) {
          error_print("failed enroll_in_sht_registry calloc of loc_registry[nrank][n_loc_registered].sh_type");
          return 0;
     }
     loc_registry[nrank][n_loc_registered].sh_kidname = (char *)calloc(MAX_CHARS_NAME, sizeof(char));
     if (!loc_registry[nrank][n_loc_registered].sh_kidname) {
          error_print("failed enroll_in_sht_registry calloc of loc_registry[n_sh_register].sh_kidname");
          return 0;
     }

     uint32_t cpsz = strlen(sh_type);

     if (cpsz < MAX_CHARS_TYPE) {
          strcpy(loc_registry[nrank][n_loc_registered].sh_type, sh_type);
     } else {
          strncpy(loc_registry[nrank][n_loc_registered].sh_type, sh_type, MAX_CHARS_TYPE);
     }

     if (global_kid_name[nrank]) {
          cpsz = strlen(global_kid_name[nrank]);
     }
     else {
          cpsz = 0;
     }
     if (!cpsz) {
          strcpy(loc_registry[nrank][n_loc_registered].sh_kidname, strdup("kid name not assigned"));
     }
     else if (cpsz < MAX_CHARS_NAME) {
          strcpy(loc_registry[nrank][n_loc_registered].sh_kidname, global_kid_name[nrank]);
     } else {
          strncpy(loc_registry[nrank][n_loc_registered].sh_kidname, global_kid_name[nrank], MAX_CHARS_NAME);
     }
     n_loc_register[nrank] = ++n_loc_registered;

     return 1;
}

void get_sht_expire_cnt(void) {
     uint32_t i, j;

     for (i = 0; i < work_size; i++) {
          for (j = 0; j < n_loc_register[i]; j++) {
               if (strncmp(loc_registry[i][j].sh_type, "sh5", 3) == 0) {
                    loc_registry[i][j].expire_cnt = stringhash5_expire_cnt(loc_registry[i][j].sht);
               }
               else if (strncmp(loc_registry[i][j].sh_type, "sh9a", 4) == 0) {
                    loc_registry[i][j].expire_cnt = stringhash9a_expire_cnt(loc_registry[i][j].sht);
               }
          }
     }
}

void get_sht_shared_expire_cnt(void) {
     uint32_t i;

     for (i = 0; i < n_sh_register; i++) {
          if (strncmp(sh_registry[i].sh_type, "sh5", 3) == 0) {
               sh_registry[i].expire_cnt = stringhash5_expire_cnt(sh_registry[i].sht);
          }
          else if (strncmp(sh_registry[i].sh_type, "sh9a", 4) == 0) {
               sh_registry[i].expire_cnt = stringhash9a_expire_cnt(sh_registry[i].sht);
          }
     }
}

void print_sht_registry(const uint32_t print_expire_cnt, const uint32_t mimo_srand_seed) {

     uint64_t mem_local = 0, mem_shared = 0;
     uint32_t i, j, nlocal = 0, n59a = 0;

     WS_MUTEX_LOCK(&endgame_lock);

     for (i = 0; i < work_size; i++) {
          nlocal += n_loc_register[i];
          for (j = 0; j < n_loc_register[i]; j++) {
               mem_local += loc_registry[i][j].size;
               if ((sht_perf > 1 && strncmp(loc_registry[i][j].sh_type, "sh3", 3) == 0) ||
                   strncmp(loc_registry[i][j].sh_type, "sh5", 3) == 0 ||
                   strncmp(loc_registry[i][j].sh_type, "sh9a", 4) == 0) {
                    n59a = 1;
               }
          }
     }

     if (n_sh_register == 0 && nlocal == 0) {
          fprintf(stderr,"\nWS Hash Table Summary:  no hash tables registered\n\n");
          fprintf(stderr," mimo->srand_seed is %d\n\n",mimo_srand_seed);
          WS_MUTEX_UNLOCK(&endgame_lock);
          return;
     }
     else {
          fprintf(stderr,"\n********************************************************\n");
          fprintf(stderr,"\nWS Hash Table Summary:\n\n");
          if (!print_expire_cnt) {
               fprintf(stderr," mimo->srand_seed is %d\n",mimo_srand_seed);
          }
     }

     if (n_sh_register > 0) {
          fprintf(stderr,"\nShared Hash Tables:\n");
          if (print_expire_cnt) {
               for (i = 0; i < n_sh_register; i++) {
                    mem_shared += sh_registry[i].size;
                    if (strncmp(sh_registry[i].sh_type, "sh5", 3) == 0 ||
                        strncmp(sh_registry[i].sh_type, "sh9a", 4) == 0) {
                         fprintf(stderr," type: %12s, label: %8s, kid: %28s, size: %12"PRIu64" bytes,\n",
                                 sh_registry[i].sh_type, sh_registry[i].sh_label, 
                                 sh_registry[i].sh_kidname, sh_registry[i].size); 
                         fprintf(stderr,"            expire_cnt: %12"PRIu64"\n", 
                                 sh_registry[i].expire_cnt);
                    }
                    else {
                         fprintf(stderr," type: %12s, label: %8s, kid: %28s, size: %12"PRIu64" bytes\n", 
                                 sh_registry[i].sh_type, sh_registry[i].sh_label, 
                                 sh_registry[i].sh_kidname, sh_registry[i].size);
                    }
               }
          }
          else {
               for (i = 0; i < n_sh_register; i++) {
                    mem_shared += sh_registry[i].size;
                    fprintf(stderr," type: %12s, label: %8s, kid: %28s, size: %12"PRIu64" bytes\n", 
                            sh_registry[i].sh_type, sh_registry[i].sh_label, sh_registry[i].sh_kidname, 
                            sh_registry[i].size);
                    fprintf(stderr,"             hash_seed: %12u\n", sh_registry[i].hash_seed);
               }
          }
          fprintf(stderr,"\nShared Hash Tables, number: %u\n", n_sh_register);
          fprintf(stderr,  "                    total size: %12"PRIu64" bytes\n", mem_shared);
     }

     if (nlocal > 0) {
          if (n59a) {
               fprintf(stderr,"\nLocal Hash Tables:\n");
          }
          if (print_expire_cnt) {
               for (i = 0; i < work_size; i++) {
                    for (j = 0; j < n_loc_register[i]; j++) {
                         if ((sht_perf > 1 && strncmp(loc_registry[i][j].sh_type, "sh3", 3) == 0) ||
                             strncmp(loc_registry[i][j].sh_type, "sh5", 3) == 0 ||
                             strncmp(loc_registry[i][j].sh_type, "sh9a", 4) == 0) {
                              fprintf(stderr," rank: %4d, type: %12s, kid: %28s, size: %12"PRIu64" bytes,\n",
                                      i, loc_registry[i][j].sh_type, loc_registry[i][j].sh_kidname, 
                                      loc_registry[i][j].size); 
                              fprintf(stderr,"             expire_cnt: %12"PRIu64"\n",
                                      loc_registry[i][j].expire_cnt);
                         }
                         else if (sht_perf > 1 || strncmp(loc_registry[i][j].sh_type, "sh3", 3) != 0) {
                              fprintf(stderr," rank: %4d, type: %12s, kid: %28s, size: %12"PRIu64" bytes\n", i, 
                                      loc_registry[i][j].sh_type, loc_registry[i][j].sh_kidname, 
                                      loc_registry[i][j].size);
                         }
                    }
               }
          }
          else {
               for (i = 0; i < work_size; i++) {
                    for (j = 0; j < n_loc_register[i]; j++) {
                         if (sht_perf > 1 || strncmp(loc_registry[i][j].sh_type, "sh3", 3) != 0) {
                              fprintf(stderr," rank: %4d, type: %12s, kid: %28s, size: %12"PRIu64" bytes\n", i, 
                                      loc_registry[i][j].sh_type, loc_registry[i][j].sh_kidname, 
                                      loc_registry[i][j].size);
                              fprintf(stderr,"            hash_seed: %12u\n", 
                                      loc_registry[i][j].hash_seed);
                         }
                    }
               }
          }
          fprintf(stderr,"\nLocal Hash Tables, number: %u\n",nlocal);
          fprintf(stderr,  "                   global size: %12"PRIu64" bytes\n", mem_local);
          if (sht_perf == 1) {
               fprintf(stderr,  "                   (total includes unlisted sh3 tables)\n");
          }
     }

     if (n_sh_register && nlocal) {
          fprintf(stderr,"\nTotal Hash Tables, number: %u\n", n_sh_register+nlocal);
          fprintf(stderr,  "                   global size: %12"PRIu64" bytes\n", mem_shared+mem_local);
     }
     fprintf(stderr,"\n********************************************************\n\n");
     WS_MUTEX_UNLOCK(&endgame_lock);
}
