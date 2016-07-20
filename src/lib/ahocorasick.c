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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ahocorasick.h"
#include "sysutil.h"
#include "error_print.h"
#include "shared/getrank.h"
#include "shared/lock_init.h"

/**
  \brief searches buffer for keyword matches, calls callback on each match
 
  will call straight aho-corasick or bmh/aho-corasick depending on number of signatures 
  and length of signatures

  \return -1 on failure
  \return 1 on match
  \return 0 on no match
 */
  int (*ac_search)(ahoc_t *           /* init & loaded ahoc tree */,
                 ahoc_state_t *     /* state pointer in keyword tree */,
                 u_char *           /* search space */,
                 uint32_t           /* length of search space */,
                 aho_buffercallback /* match callback function */,
                 void *             /* callback_data */);



/* globals */
extern uint32_t work_size;
static char **_keyword;
int * keywordlencount;
int * num_keywords;
int threshold; //number of signature to have before switching
int min_threshold; //length of skip to have before switching

ahoc_t* ac_init(void)
{
     ahoc_t *retval;
     int i;

     retval = (ahoc_t*) calloc(1, sizeof(ahoc_t));
     if(!retval) {
          error_print("failed ac_init calloc of retval");
          return NULL;
     }

     retval->root = (treenode_t*) calloc(1, sizeof(treenode_t));
     if (!retval->root) {
          error_print("failed ac_init calloc of retval->root");
          return NULL;
     }
     retval->root->fail_node = retval->root;
     retval->max_shift = MAX_UINT;

     /* init for Boyer-Moore style skipping */
     for(i = 0; i < NUMCHAR; i++) {
          retval->cshift[i] = MAX_UINT;
     }

     // Case sensitive by default
     retval->case_insensitive = 0;

     // Alloc the globals
     if (!_keyword) {
          WS_MUTEX_LOCK(&startlock);
          if (!_keyword) {
               _keyword = (char **)calloc(work_size, sizeof(char *));
               if (!_keyword) {
                    error_print("failed ac_init calloc of _keyword");
                    WS_MUTEX_UNLOCK(&startlock);
                    return 0;
               }
               num_keywords = (int *)calloc(work_size, sizeof(int));
               if (!num_keywords) {
                    error_print("failed ac_init calloc of num_keywords");
                    WS_MUTEX_UNLOCK(&startlock);
                    return 0;
               }
               keywordlencount = (int *)calloc(work_size, sizeof(int));
               if (!keywordlencount) {
                    error_print("failed ac_init calloc of keywordlencount");
                    WS_MUTEX_UNLOCK(&startlock);
                    return 0;
               }
          }
          WS_MUTEX_UNLOCK(&startlock);
     }

     const int nrank = GETRANK();
     _keyword[nrank] = NULL;

     // Clear the number of patterns
     num_keywords[nrank] = 0;
     keywordlencount[nrank] = 0;
 
     // Set default threshold and min_threshold
     //current defaults are arbitrary
     threshold = 31;
     min_threshold = 5;

     return retval;
}

static int loadkeyword_helper(ahoc_t *ac, treenode_t *cur, char *keyword, 
                               int keywordlen, int keymapval)
{
     treenode_t *newNode = NULL;
     const int nrank = GETRANK();
     keywordlencount[nrank]++;
     uint32_t    skiplen;
     uint8_t     c = (uint8_t) *keyword;
     if(ac->case_insensitive == 1 && (c >= 'A' && c <= 'Z')) c += 32;
     
     if(keywordlen) {
          if(cur->nodes[c]) {
               newNode = cur->nodes[c];
               if(!loadkeyword_helper(ac, newNode, keyword + 1, 
                                  keywordlen - 1, keymapval)) {
                    return 0;
               }
          }
          else {
               /* no children nodes match current letter */
               cur->children++;
               cur->nodes[c] = (treenode_t*) calloc(1, sizeof(treenode_t));
               if (cur->nodes[c]) {
                    newNode = cur->nodes[c];
                    newNode->alpha = (u_char) c;
                    newNode->fail_node = ac->root;
                    if(!loadkeyword_helper(ac, newNode, keyword + 1, 
                                       keywordlen - 1, keymapval)) {
                         return 0;
                    }
               }
               else {
                    error_print("failed loadkeyword_helper calloc of cur->nodes[c]");
                    return 0;
               }
          }

          /* update char miss shift */
          skiplen = strlen(_keyword[nrank]) - keywordlen;
          if((skiplen > 0) && (skiplen < ac->cshift[c])) {
               ac->cshift[c] = skiplen;
          }
     }
     /* check that keyword not already in tree */
     else if(!cur->match) {
          cur->match = (term_info_t*) calloc(1, sizeof(term_info_t));
          if (cur->match) {
               cur->match->keymapval = keymapval;
               cur->match->key = (u_char*) strdup(_keyword[nrank]);
               cur->match->len = keywordlencount[nrank];
               keywordlencount[nrank] = 0;

               if(strlen(_keyword[nrank]) < ac->max_shift) {
                    ac->max_shift = strlen(_keyword[nrank]);
               }
	  
               if(strlen(_keyword[nrank]) > ac->max_pattern_len){
                    ac->max_pattern_len = strlen(_keyword[nrank]);
               }

               /* increment keyword count */
               num_keywords[nrank]++;
          }
          else {
               error_print("failed loadkeyword_helper calloc of cur->match");
               return 0;
          }
     }

     return 1;
}

int ac_loadkeyword(ahoc_t *ac, char *keyword, int keywordlen, int keymapval)
{
     if(!ac || !ac->root) {
          fprintf(stderr, "uninitialized Aho-Corasick instance\n");
          return 0;
     }

     // Figure out the keyword length if need be..
     if(keywordlen == 0) keywordlen = strlen(keyword);

     // If keywordlen is still 0, we have a problem with the input
     if (keywordlen == 0) {
          return 0;
     }

     const int nrank = GETRANK();
     _keyword[nrank] = strdup(keyword);

     if(!loadkeyword_helper(ac, ac->root, keyword, keywordlen, keymapval)) {
          return 0;
     }

     //free the keyword once loadkeyword_helper has duplicated it
     free(_keyword[nrank]);

     return 1;
}

/**
  \return 1 to tell calling node to delete node* at the given character offset
  \return 0 when do not delete calling node
  */
static int RemoveKeywordHelper(ahoc_t *ac, treenode_t *curNode, char *keyword, int len)
{
     uint8_t  curChar;
     int i, retval;

     /* found keyword, we hope */
     if(len == 0) {
          if(curNode->match) {
               free(curNode->match->key);
               free(curNode->match);
               curNode->match = NULL;
          }
          /* why wouldn't there be a match? */
          else {
               fprintf(stderr, "no match?\n");
               return 0;
          }

          for(i = 0; i < NUMCHAR; i++) {
               if(curNode->nodes[i]) {
                    return 0;
               }
          }

          return 1;
     }

     curChar = *keyword;
     if(ac->case_insensitive == 1 && (curChar >= 'A' && curChar <= 'Z')) curChar += 32;
     if(!curNode->nodes[curChar]) {
          /* no path to match keyword */
          return 0;
     }

     retval = 0;
     /* recursive call */
     if(RemoveKeywordHelper(ac, curNode->nodes[curChar], keyword + 1, len - 1)) {
          free(curNode->nodes[curChar]);
          curNode->nodes[curChar] = NULL;
          curNode->children--;

          retval = 1;

          if(curNode->match) {
               retval = 0;
          }
          else {
               for(i = 0; i < NUMCHAR; i++) {
                    if(curNode->nodes[i]) {
                         retval = 0;
                         break;
                    }
               }
          }
     }

     const int nrank = GETRANK();
     if (num_keywords[nrank] >1)
	 num_keywords[nrank]--;
     return retval;
}

int ac_remove_keyword(ahoc_t *ac, char *keyword, int len)
{
     if(!ac || !ac->root) {
          fprintf(stderr, "uninitialized Aho-Corasick instance\n");
          return 0;
     }

     RemoveKeywordHelper(ac, ac->root, keyword, len);

     return 1;
}

static void ResetFailNodes(ahoc_t *ac, treenode_t *curNode)
{
     int c;

     if(!curNode) return;

     for(c = 0; c < NUMCHAR; c++) {
          if(curNode->nodes[c]) {
               ResetFailNodes(ac, curNode->nodes[c]);
          }
     }

     curNode->fail_node = ac->root;
}

int ac_finalize(ahoc_t *ac)
{
     treenode_t *root, *cur, *state;
     qnode_t    *qroot, *qiter, *qtail, *qtmp;
     uint8_t     i, num, children;

     if(!ac || !ac->root) {
          fprintf(stderr, "uninitialized Aho-Corasick tree\n");
          return 0;
     }

     /* important for getting keyword removal to work */
     ResetFailNodes(ac, ac->root);

     /* permanent pointer to tree root */
     root = ac->root;

     /* allocate memory for root node in vertex, init to zero */
     qroot = (qnode_t*) calloc(1, sizeof(qnode_t));
     if (!qroot) {
          error_print("failed ac_finalize calloc of qroot");
          return 0;
     }
     qroot->nodeptr = root;
     qtail = qroot;      /* last node in queue */

     /*fprintf(stderr, "setting level 1 children\n");*/
     
     /* set depth one children fail states and add to queue */
     cur = root;
     children = cur->children;
     for(i = 0, num = 0; num < children; i++) {
          if(!cur->nodes[i]) {
              continue; 
          }

          num++;

          /* add new node to end of queue */
          qtail->next = (qnode_t*) calloc(1, sizeof(qnode_t));
          if (!qtail->next) {
               error_print("failed ac_finalize calloc of qtail->next");
               return 0;
          }
          qtail = qtail->next;
          qtail->nodeptr = cur->nodes[i];
          qtail->nodeptr->fail_node = root;
     }

     /*fprintf(stderr, "before for\n");*/
     for(qiter = qroot->next; qiter; qiter = qiter->next) {
          cur = qiter->nodeptr;

          /* add children to queue (breadth-first search) */
          if((children = cur->children) > 0) {
               for(i = 0, num = 0; num < children; i++) {
                    /* check to see if char node exists */
                    if(!cur->nodes[i]) {
                         continue;
                    }

                    /*fprintf(stderr, "child %c\n", (char)i);*/
                    num++;
                    
                    /* add new node to end of queue */
                    qtail->next = (qnode_t*) calloc(1, sizeof(qnode_t));
                    if (!qtail->next) {
                         error_print("failed ac_finalize calloc of qtail->next");
                         return 0;
                    }
                    qtail = qtail->next;
                    qtail->nodeptr = cur->nodes[i];

                    state = cur->fail_node;

                    /*fprintf(stderr, "before while, root = %x, state = %x\n", root, state);*/
                    while(!state->nodes[i] && (state != root)) {
                        state = state->fail_node;
                        /*fprintf(stderr, "end of loop, root = %x, state = %x\n", root, state);*/
                    }

                    if(state->nodes[i]) {
                        qtail->nodeptr->fail_node = state->nodes[i];
                    }
                    else {
                        qtail->nodeptr->fail_node = root;
                    }
               }
          }
     }

     /* free up queue */
     qiter = qroot;
     while(qiter) {
          qtmp = qiter->next;
          free(qiter);
          qiter = qtmp;
     }

     /* now we see which search function gets mapped */
     const int nrank = GETRANK();
     if (num_keywords[nrank] < threshold){
          if (ac->max_shift < min_threshold){
               //fprintf(stderr, "%d Keywords below threshold of %d --> using BMH skipping.\n", num_keywords[nrank], threshold);
               ac_search = &ac_searchstr_skip;
               ac->below_threshold = 1;
               return 1;
          }
     }
     ac_search = &ac_searchstr;
     ac->below_threshold = 0;
     return 1;
}


int ac_searchfile(ahoc_t *ac, ahoc_state_t *sPtr, char *fname,
                  aho_filecallback callback_func, void * callback_data)
{
     FILE       *stream;     /* File containing input stream */
     int         c;
     int         callback_ret;
     boolean     pass;
     u_char     *buf;
     treenode_t *cur;

     if(!ac || !ac->root || !sPtr || !(*sPtr)) {
          fprintf(stderr, "uninitialized Aho-Corasick tree or state ptr\n");
          return -1;
     }

     if ((stream = sysutil_config_fopen(fname, "r")) == NULL) {
          error_print("ac_searchfile input file %s could not be located\n", fname);
          error_print("AC search file not found.");
          return -1;
     }

     cur = *sPtr;

     while((c = fgetc(stream)) != EOF) {
          pass = FALSE;

          if(cur->nodes[c]) {
               cur = cur->nodes[c];
               while(cur->match) {
                    buf = cur->match->key;

                    if(cur->children == 0) {
                         cur = cur->fail_node;
                         if(!callback_func) {
                              continue;
                         }
                         callback_ret = callback_func(buf, stream,
                                                      callback_data);
                         if(callback_ret) {
                              *sPtr = cur;
                              return 1;
                         }
                    }
                    else { 
                         if(callback_func) {
                              callback_ret = callback_func(buf, stream,
                                                           callback_data);

                              if(callback_ret) {
                                   *sPtr = cur;
                                   return 1;
                              }
                         }

                         break;
                    }
               }
          }
          else {
               while(!pass) {
                    cur = cur->fail_node;
                    if(cur == ac->root) {
                         if(cur->nodes[c]) {
                              cur = cur->nodes[c];
                         }
                         pass = TRUE;
                    }
                    else if(cur->nodes[c]) {
                         cur = cur->nodes[c];
                         if(cur->match) {
                              buf = cur->match->key;

                              if(!callback_func) {
                                   continue;
                              }

                              if(callback_func(buf, stream, callback_data)) {
                                   *sPtr = cur;
                                   return 1;
                              }
                         }
                         pass = TRUE;
                    }
               }
          }
     }
     sysutil_config_fclose(stream);

     *sPtr = cur;

     return 0;
}




int ac_searchstr(ahoc_t *ac, ahoc_state_t *tree_state, u_char *buf, uint32_t buflen, 
                 aho_buffercallback callback_func, void *callback_data)
{
     uint8_t     c;
     u_char     *tmpbuf;
     int         callback_ret;
     treenode_t *cur, *failstate;
     uint32_t    remainingBufLen;

     if(!ac || !ac->root || !tree_state || !(*tree_state)) {
          fprintf(stderr, "uninitialized Aho-Corasick tree or state ptr\n");
          return -1;
     }

     if(!callback_func) {
          fprintf(stderr, "you can call single-search\n");          
          return -1;
     }

     cur = *tree_state;
     remainingBufLen = buflen;
     tmpbuf = buf;

     while(remainingBufLen) {
          c = (uint8_t) *tmpbuf;

          /* matched character */
          if(cur->nodes[c]) {
               cur = cur->nodes[c];
               if(cur->match) {
                    callback_ret = callback_func(cur->match->key, tmpbuf+1, 
                                                 remainingBufLen-1, callback_data);
                    if(callback_ret) {
                         *tree_state = cur;
                         return 1;
                    }
               }

               /* match also on fail node */
               failstate = cur->fail_node;
               while(failstate->match) {
                    callback_ret = callback_func(cur->fail_node->match->key, tmpbuf+1,
                                                 remainingBufLen-1, callback_data);
                    if(callback_ret) {
                         *tree_state = cur;
                         return 1;
                    }

                    failstate = failstate->fail_node;
               }
          }
          /* try fail node when current node is not the tree root */
          else if (cur != ac->root) {
               cur = cur->fail_node;
               continue;
          }

          tmpbuf++;
               //printf("Remaining %u\n", remainingBufLen); fflush(stdout);
          remainingBufLen--;
     }

     *tree_state = cur;

     return 0;
}


/** updates Boyer-Moore shift for mismatched characters */
static void update_searchbuf(ahoc_t *ac, u_char **buf, uint32_t *len, uint8_t c)
{
     if((ac->cshift[c] < ac->max_shift) && 
        (ac->cshift[c] <= *len)) {
          *buf -= ac->cshift[c];
          *len += ac->cshift[c];
     }
     else if(ac->max_shift <= *len) {
          *buf -= ac->max_shift;
          *len += ac->max_shift;
     }
     else {
          *buf = NULL;
          *len = MAX_UINT;
     }
}


int ac_searchstr_skip(ahoc_t *ac, ahoc_state_t *tree_state, u_char *str, uint32_t len, 
                      aho_buffercallback callback_func, void *callback_data)
{
     uint8_t     c;
     u_char     *matchstr;
     u_char     *searchbuf, *matchbuf;
     treenode_t *cur;
     uint32_t    searchlen, matchlen;
     int         callback_ret;

     if(!ac || !ac->root) {
          fprintf(stderr, "uninitialized Aho-Corasick tree or state ptr\n");
          return -1;
     }

     cur = ac->root;

     /* start searching from end of string */
     searchlen = ac->max_shift;
     searchbuf = str + (len - searchlen);

     while(searchlen <= len) {
          c = (uint8_t) *searchbuf;

          /* character match */
          if(cur->nodes[c]) {
               /* save for return on mismatch */
               matchbuf = searchbuf;
               matchlen = searchlen;

               /* while character match */
               while(cur->nodes[c]) {
                    /*printf("matched char %c\n", c);*/
                    cur = cur->nodes[c];

                    /* keyword match */
                    if(cur->match) {
                         /*printf("matched on term\n");*/
                         matchstr = cur->match->key;

                         if(callback_func) {
                              callback_ret = callback_func(matchstr, 
                                   searchbuf+1, searchlen-1, callback_data);

                              if(callback_ret) {
                                   return 1;
                              }
                         }
                    }

                    /* move forward while match */
                    searchbuf++;
                    searchlen--;
                    c = (uint8_t) *searchbuf;
               }

               /* skip to next char before match char */
               update_searchbuf(ac, &matchbuf, &matchlen, (uint8_t)*matchbuf);
               searchbuf = matchbuf;
               searchlen = matchlen;
               cur = ac->root;
          }
          else {
               /*printf("mismatched char %c\n", c);*/
               update_searchbuf(ac, &searchbuf, &searchlen, c);
          }
     }

     return 0;
}




static void PrintTreeHelper(treenode_t *currentNode)
{
     int i;

     if(!currentNode) return;

     if(currentNode->alpha) {
          /*
          printf("currentNode = 0x%x\n", currentNode);
          printf("failnode = 0x%x\n", currentNode->fail_node);
          */

          fprintf(stderr, "%c", currentNode->alpha);

          if(currentNode->match) {
               fprintf(stderr, " '%s'", currentNode->match->key);
          }
          else {
               fprintf(stderr, " none");
          }

          if(currentNode->fail_node->match) {
               fprintf(stderr, " '%s'", currentNode->fail_node->match->key);
          }
          else {
               fprintf(stderr, " none");
          }

          fprintf(stderr, "\n");
     }

     for(i = 0; i < NUMCHAR; i++) PrintTreeHelper(currentNode->nodes[i]);

     fprintf(stderr, "---\n");
}

void ac_print_tree(ahoc_t *ac)
{
     if(!ac) return;

     /*
     printf("root = 0x%x\n", ac->searchTree);
     */
     PrintTreeHelper(ac->root);
}

term_info_t *ac_singlesearch_trans(ahoc_t *ac, 
				   ahoc_state_t *sPtr, 
				   u_char *buf, 
				   uint32_t buflen, 
				   u_char **retbuf,
				   uint32_t *retlen,
				   uint8_t mask_len,
				   uint8_t case_ins,
				   uint8_t binary_op)
{
  uint8_t     c, c_next;
  u_char     *tmpbuf = buf;
  uint32_t    tmpbuflen = buflen;
  //int         retval;
  treenode_t *cur;
  term_info_t *leaf;
  
     if(!ac || !ac->root || !sPtr || !(*sPtr)) {
          fprintf(stderr, "uninitialized Aho-Corasick tree or state ptr\n");
          return NULL;
     }

     if(!buf) {
       fprintf(stderr, "Uninitialized buffer in Aho-Corasick search\n");
       return NULL;
     }

     cur = *sPtr; 

     while(tmpbuflen > mask_len) {
          c_next = (uint8_t)*(tmpbuf+mask_len);
		  c = (uint8_t)*tmpbuf;
		  
		  //Always perform case-insensitive matching
		  if(case_ins == 1){
			  if((c >='A' && c <= 'Z'))
				  c+=32;
			  if((c_next >='A' && c_next <= 'Z'))
				  c_next+=32;
		  }
		  if(binary_op == 2)
		      c = c_next - c;
		  else
		      c = c ^ c_next;
          /* matched character */
          if(cur->nodes[c]) {
               cur = cur->nodes[c];
               if(cur->match) {
                    //retval = cur->match->keymapval; 
					leaf = cur->match;
                    //printf("found match '%d'\n", leaf->keymapval);
                    if(cur->children == 0) {
                         cur = cur->fail_node;
                    }

                    /* move past last character */
                    *retbuf = tmpbuf + 1;
                    *retlen = tmpbuflen - 1;
                    *sPtr = cur;
                    return leaf;
               }

               /* match on fail node */
               if(cur->fail_node->match) {
                    /*printf("found match on fail_node '%s'\n", cur->fail_node->match->key);*/
                    //retval = cur->fail_node->match->keymapval; 
					leaf = cur->match;
					//printf("found match failnode '%s'\n", cur->match->key);
                    if(cur->children == 0) {
                         cur = cur->fail_node;
                    }

                    /* move past last character */
                    *retbuf = tmpbuf + 1;
                    *retlen = tmpbuflen - 1;
                    *sPtr = cur;
                    return leaf;
               }
          }
          else if (cur != ac->root) {
               cur = cur->fail_node;
							 continue;
          }

          tmpbuf++;
          tmpbuflen--;
     }

     *sPtr = cur;

     return NULL;
}

inline int ac_singlesearch(ahoc_t *ac, 
                                  ahoc_state_t *sPtr, 
                                  u_char *buf, 
                                  uint32_t buflen, 
                                  u_char **retbuf,
                                  uint32_t *retlen)
{
     uint8_t     c;
     u_char     *tmpbuf = buf;
     uint32_t    tmpbuflen = buflen;
     int         retval;
     treenode_t *cur;

     if(!ac || !ac->root || !sPtr || !(*sPtr)) {
          fprintf(stderr, "uninitialized Aho-Corasick tree or state ptr\n");
          return -1;
     }

     if(!buf) {
       fprintf(stderr, "Uninitialized buffer in Aho-Corasick search\n");
       return -1;
     }

     cur = *sPtr; 

     while(tmpbuflen) {
          c = (uint8_t) *tmpbuf;
          // Enable case insensitive checking if needed
          if(ac->case_insensitive == 1 && (c >= 'A' && c <= 'Z')) c += 32;

          /* matched character */
          if(cur->nodes[c]) {
               /*printf("matched char %c\n", c);*/
               cur = cur->nodes[c];
               if(cur->match) {
                    retval = cur->match->keymapval; 

                    /*printf("found match '%s'\n", cur->match->key);*/
                    if(cur->children == 0) {
                         cur = cur->fail_node;
                    }

                    /* move past last character */
                    *retbuf = tmpbuf + 1;
                    *retlen = tmpbuflen - 1;
                    *sPtr = cur;
                    return retval;
               }

               /* match on fail node */
               if(cur->fail_node->match) {
                    /*printf("found match on fail_node '%s'\n", cur->fail_node->match->key);*/
                    retval = cur->fail_node->match->keymapval; 

                    if(cur->children == 0) {
                         cur = cur->fail_node;
                    }

                    /* move past last character */
                    *retbuf = tmpbuf + 1;
                    *retlen = tmpbuflen - 1;
                    *sPtr = cur;
                    return retval;
               }
          }
          else if (cur != ac->root) {
               cur = cur->fail_node;
	       continue;
          }

          tmpbuf++;
          tmpbuflen--;
     }

     *sPtr = cur;

     return -1;
}

inline int ac_singlesearch_skip(ahoc_t *ac, 
                                  ahoc_state_t *sPtr, 
                                  u_char *buf, 
                                  uint32_t buflen, 
                                  u_char **retbuf,
                                  uint32_t *retlen)
{
     uint8_t     c;
     u_char     *tmpbuf = buf;
     u_char     *searchbuf;
     uint32_t    tmpbuflen = buflen;
     int         retval;
     treenode_t *cur;
     uint32_t    searchlen;

     if(!ac || !ac->root || !sPtr || !(*sPtr)) {
          fprintf(stderr, "uninitialized Aho-Corasick tree or state ptr\n");
          return -1;
     }

     if(!buf) {
       fprintf(stderr, "Uninitialized buffer in Aho-Corasick search\n");
       return -1;
     }

     cur = *sPtr; 
     
     //now we hop back since we are doing a reverse search
     /* start searching from end of string */
     searchlen = ac->max_shift;
     searchbuf = tmpbuf + (tmpbuflen - searchlen);


     while(searchlen <= tmpbuflen) {
          c = (uint8_t) *searchbuf
;
          // Enable case insensitive checking if needed
          if(ac->case_insensitive == 1 && (c >= 'A' && c <= 'Z')) c += 32;

          /* matched character */
          if(cur->nodes[c]) {
               /*printf("matched char %c\n", c);*/
               cur = cur->nodes[c];
               if(cur->match) {
                    retval = cur->match->keymapval; 

                    /*printf("found match '%s'\n", cur->match->key);*/
                    if(cur->children == 0) {
                         cur = cur->fail_node;
                    }

                    /* move past last character */
                    *retbuf = tmpbuf + 1;
                    *retlen = tmpbuflen - 1;
                    *sPtr = cur;
                    return retval;
               }

               /* match on fail node */
               if(cur->fail_node->match) {
                    /*printf("found match on fail_node '%s'\n", cur->fail_node->match->key);*/
                    retval = cur->fail_node->match->keymapval; 

                    if(cur->children == 0) {
                         cur = cur->fail_node;
                    }

                    /* move past last character */
                    *retbuf = tmpbuf + 1;
                    *retlen = tmpbuflen - 1;
                    *sPtr = cur;
                    return retval;
               }
          }
          else if (cur != ac->root) {
               cur = cur->fail_node;
	       continue;
          }

          tmpbuf++;
          tmpbuflen--;
     }

     *sPtr = cur;

     return -1;
}

static void FreeTreeHelper(treenode_t *currentNode)
{
     int i;

     if(!currentNode) return;
     if(currentNode->match) {
       if(currentNode->match->key) free(currentNode->match->key);
       free(currentNode->match);
     }
     for(i = 0; i < NUMCHAR; i++) FreeTreeHelper(currentNode->nodes[i]);
     free(currentNode);
}

void ac_free(ahoc_t *ac)
{
     //free globals:  first arrival thread will do this
     if (_keyword) {
          WS_MUTEX_LOCK(&startlock);
          if (_keyword) {
               free(_keyword);
               free(keywordlencount);
               free(num_keywords);
               _keyword = NULL;
               keywordlencount = NULL;
               num_keywords = NULL;
          }
          WS_MUTEX_UNLOCK(&startlock);
     }

     // free ac
     if(!ac) return;
     if(ac->root) FreeTreeHelper(ac->root);
     free(ac);
}
