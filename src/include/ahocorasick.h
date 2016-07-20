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

/*
     @file ahocorasick.h

     \date Nov 2006

     \brief Aho Corasick string searching API

     \remarks (Feb 2007) --  added Boyer-Moore style searching
     \remarks (Nov 2007) --  fixed bug in search function to return all matches, 
          added keyword removes
     \remarks (Jan 2008) --  fixed bug in finalize function (correct fail transitions),
					simplified and sped up search functions
*/

#ifndef _AHOCORASICK_H
#define _AHOCORASICK_H

#include <stdint.h>
#include <sys/types.h>
#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

#define TRUE     1
#define FALSE    0
#define NUMCHAR  256
#define MAX_UINT 0xffffffff

/** match node information */
typedef struct _term_info_t {
     int     keymapval;
     u_char *key;   /* keyword */
     int     len;   /* size of keyword */
     void   *udata;
} term_info_t;

/** AC tree node */
typedef struct _treenode_t {
  u_char             alpha;      /* node alphabet value */
  uint8_t            children;   /* number of child nodes */
  term_info_t        *match;      /* keyword info */
  struct _treenode_t *nodes[NUMCHAR];
  struct _treenode_t *fail_node;  /* pointer to fail node */
  uint8_t            buflen;      /* max buffer len */
} treenode_t;

/** queue node */
typedef struct _qnode_t {
     struct _qnode_t    *next;
     struct _treenode_t *nodeptr;
} qnode_t;


/** AC type */
typedef struct _ahoc_t {
     treenode_t *root;
     uint32_t    max_shift;
     uint32_t    cshift[NUMCHAR];  
     uint32_t    max_pattern_len;
     uint8_t     case_insensitive;
     uint8_t     below_threshold;

} ahoc_t;

typedef uint8_t     boolean;
typedef treenode_t *ahoc_state_t;

/**
  \brief callback function type on keyword matches in files (for testing)
  \return non-zero to match only once
  \return 0 to match multiple keywords
 */
typedef int (*aho_filecallback)(u_char * /* matchstr */,
                                FILE *   /* pointer after match in file */,
                                void *   /* callback data */);

/**
  \brief callback function type on keyword matches in buffer (normal use)
  \return non-zero to match only once
  \return 0 to match multiple keywords
 */
typedef int (*aho_buffercallback)(u_char * /* matchstr */,
                                  u_char * /* pointer after match in buffer */,
                                  uint32_t /* remaining chars in buffer */,
                                  void *   /* callback data */);

/**
  \brief initializes Aho Corasick type

  must be called before loading keywords
  \return new ahoc_t* on success
  \return NULL on failure
 */
ahoc_t* ac_init(void);

/**
  \brief insert keyword into Aho Corasick tree

  use keymap value for singlesearch returns (for use in 'switch' statements)
  \return 1 on success
  \return 0 on failure
 */
int ac_loadkeyword(ahoc_t * /* initialized ahoc tree */,
                   char *   /* keyword */, 
                   int      /* keywordlen */,
                   int      /* key map value */);

/**
  \brief remove keyword from Aho Corasick tree

  \return 1 on success
  \return 0 on failure
 */
int ac_remove_keyword(ahoc_t * /* initialized ahoc tree */,
                      char *   /* keyword */, 
                      int      /* keywordlen */);

/**
  \brief call after all keywords loaded 

  determines fail nodes on mismatched characters,
  don't need to call for _skip() APIs
  also deteremine which algorithm to use

  \return 1 on success
  \return 0 on failure
 */
int ac_finalize(ahoc_t * /* init & loaded ahoc tree */);

/**
  \brief searches file for keyword matches
  \return -1 on failure
  \return 1 on match
  \return 0 on no match
 */
int ac_searchfile(ahoc_t *         /* init & loaded ahoc tree */,
                  ahoc_state_t *   /* state pointer in keyword tree */,
                  char *           /* filename */,
                  aho_filecallback /* saved file callback function */,
                  void *           /* callback_data */);

/**
  \brief searches buffer for keyword matches, calls callback on each match
 
  will call straight aho-corasick or bmh/aho-corasick depending on number of signatures 
  and length of signatures

  \return -1 on failure
  \return 1 on match
  \return 0 on no match
  int (*ac_search)(ahoc_t *    
                 ahoc_state_t * 
                 u_char *        
                 uint32_t         
                 aho_buffercallback
                 void *             
 */




/**
  \brief searches buffer for keyword matches, calls callback on each match

  \return -1 on failure
  \return 1 on match
  \return 0 on no match
 */
int ac_searchstr(ahoc_t *           /* init & loaded ahoc tree */,
                 ahoc_state_t *     /* state pointer in keyword tree */,
                 u_char *           /* search space */,
                 uint32_t           /* length of search space */,
                 aho_buffercallback /* match callback function */,
                 void *             /* callback_data */);

void ac_free(ahoc_t *ac);

/**
  \brief searches buffer for keyword matches

  starts searching from the end of buffer and does BM skipping,
  does not need state and will not handle cross-buffer matches
  \return -1 on failure
  \return 1 on match
  \return 0 on no match
 */
int ac_searchstr_skip(ahoc_t *           /* init & loaded ahoc tree */, 
		      ahoc_state_t *     /* state pointer in keyword tree */,
                      u_char *           /* search space */,
                      uint32_t           /* length of search space */,
                      aho_buffercallback /* match callback function */,
                      void *             /* callback_data */);

/**
  \brief prints Aho-Corasick tree

  for debugging
 */
void ac_print_tree(ahoc_t*);

term_info_t *ac_singlesearch_trans(ahoc_t *ac, 
    ahoc_state_t *sPtr, 
    u_char *buf, 
    uint32_t buflen, 
    u_char **retbuf,
    uint32_t *retlen,
    uint8_t mask_len,
    uint8_t case_ins,
    uint8_t binary_op);

/**
  \brief searches buffer for first keyword
  \return -1 on failure or no match
  \return 'mapval' on match
 */
int ac_singlesearch(ahoc_t *ac, 
                    ahoc_state_t *sPtr, 
                    u_char *buf, 
                    uint32_t buflen, 
                    u_char **retbuf,
                    uint32_t *retlen);

/**
  \brief searches buffer for first keyword
  \return -1 on failure or no match
  \return 'mapval' on match
 */
int ac_singlesearch_skip(ahoc_t *ac, 
                         ahoc_state_t *sPtr, 
                         u_char *buf, 
                         uint32_t buflen, 
                         u_char **retbuf,
                         uint32_t *retlen);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _AHOCORASICK_H
