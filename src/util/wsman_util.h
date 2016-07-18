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
#ifndef _WSMAN_UTIL_H
#define _WSMAN_UTIL_H

#include <string.h>
#include <stddef.h>

#include "wsman_color.h"
#include "wsman_word_wrap.h"


// print titles in color if output is a terminal, else don't
#define title(fp, fmt, ...) if (isatty(fileno(fp))) { \
     fprintf(fp, CYAN fmt "\n" RESET, ##__VA_ARGS__); \
} else { \
     fprintf(fp, fmt "\n", ##__VA_ARGS__); \
}

#define highlight(p, buf, kw, fp, width, tab) if (isatty(fileno(fp))) { \
     p = search_and_highlight(fp,buf, kw);                              \
     if (p) {                                                           \
          print_wrap(fp, p, width, tab);                                \
          free(p);                                                      \
     }                                                                  \
} else {                                                                \
     print_wrap(fp, buf, width, tab);                                   \
}


//prototypes
static inline void print_divider(FILE *fp);
static inline void print_helpful_message(FILE *fp);
static inline void print_list(FILE *fp, char **list);
static inline void print_list_search(FILE *fp, char **list, char * keyword);
static inline void print_portlist(FILE * fp, proc_port_t * portlist);
static inline void print_portlist_search(FILE * fp, proc_port_t * portlist, char * keyword);
static inline void print_rst_header(FILE * fp, char * text);
static inline void print_rst_list(FILE *fp, char **list);
static inline void print_rst_portlist(FILE * fp, proc_port_t * portlist);
static inline void print_rst_subheader(FILE * fp, char * text);
static inline void print_usage(FILE *fp);
int _asprintf(char **strp, const char *fmt, ...);
int _vasprintf(char **strp, const char *fmt, va_list args);
char * _strcasestr(char *a, char *b);
char * search_and_highlight(FILE * outfp, char * text, char * keyword);
char * str_index_of(char * s1, char * s2, int * index);
char * trim(char *c);


static inline void print_divider(FILE * fp)
{
     char * divider = \
          "----------------------------------------" \
          "----------------------------------------";
     fprintf(fp, "\n%s\n", divider);
}

static inline void print_helpful_message(FILE * fp)
{
     fprintf(fp, "wsman - version %d.%d.%d", WS_MAJOR_VERSION, WS_MINOR_VERSION, WS_SUBMINOR_VERSION);
     fprintf(fp,  " - For detailed kid information, use the -v flag.\n\n");
}

static inline void print_list(FILE * fp, char ** list)
{
     static const size_t BUF_SZ = ((80-4)*10); /* 10 lines */
     char buf[BUF_SZ];
     size_t i = 0;
     while ( list[i] ) {
          memset(buf,0,sizeof(buf));
          size_t len = 0;
          while ( list[i] ) {
               size_t newlen = 3+strlen(list[i]);
               if ( (len + newlen) >= BUF_SZ ) {
                    if ( (len + 2) < BUF_SZ )
                         strncat(buf, ",", 2);
                    break;
               }
               if ( len > 0 ) {
                    strncat(buf, ", ", 2);
                    len += 2;
               }
               strncat(buf, list[i], strlen(list[i]));
               len += strlen(list[i]);
               ++i;
          }
          print_wrap(fp, buf, WRAP_WIDTH, 4);
     }
}

static inline void print_list_search(FILE * fp, char ** list, char * keyword)
{
     char buf[1000];
     memset(buf,0,sizeof(buf));
     int i;
     int len = 0;
     for (i = 0; list[i]; i++) {
          if (i >0) {
               char * commaspace = ", ";
               strncat(buf, commaspace, strlen(commaspace));
               len += 1;
          }
          strncat(buf, list[i], strlen(list[i]));
          len += strlen(list[i]);
     }
     char * p;
     highlight(p, buf, keyword, fp, WRAP_WIDTH, 4);
}

static inline void print_portlist(FILE * fp, proc_port_t * portlist) 
{
     int i;
     for (i = 0; portlist[i].label != NULL; i++) {
          int len = 5 + strlen(portlist[i].label) + \
                    strlen(portlist[i].description);
          char * port = (char *) malloc(len+1);
          if (!port) {
               error_print("failed print_portlist malloc of port");
               return;
          }
          snprintf(port,len+1,":%s  - %s", portlist[i].label,
                    portlist[i].description);
          print_wrap(fp, port, WRAP_WIDTH, 4);
          free(port);
     }
}

static inline void print_portlist_search(FILE * fp, proc_port_t * portlist,
     char * keyword) 
{
     int i;
     for (i = 0; portlist[i].label != NULL; i++) {
          int len = 5 + strlen(portlist[i].label) + \
                    strlen(portlist[i].description);
          char * port = (char *) malloc(len+1);
          if (!port) {
               error_print("failed print_portlist_search malloc of port");
               return;
          }
          snprintf(port,len+1,":%s  - %s", portlist[i].label,
                    portlist[i].description);
          char * p;
          highlight(p, port, keyword, fp, WRAP_WIDTH, 4);
     }
}

static inline void print_usage(FILE * fp)
{
     fprintf(fp, "wsman - version %d.%d.%d\n\n", WS_MAJOR_VERSION, WS_MINOR_VERSION, WS_SUBMINOR_VERSION); 
     fprintf(fp, "wsman is the WaterSlide documentation system.\n");
     fprintf(fp, "- To list all kids and purpose: wsman\n");
     fprintf(fp, "- To list detailed documentation for all kids: wsman -v\n\n");
     fprintf(fp, "standard usage: wsman [-cstiov] KID|TAG|KEYWORD|TYPE\n");
     fprintf(fp, "\n");
     fprintf(fp, "Arguments:\n");
     fprintf(fp, "   -c        Check the completeness of documentation for the given kid (use\n");
     fprintf(fp, "             with -v for verbose). (NOTE: This option is used for development\n");
     fprintf(fp, "             purposes only.)\n");
     fprintf(fp, "   -s        Search for kids containing a keyword.\n");
     fprintf(fp, "   -t        Search for kids with a specific tag.\n");
     fprintf(fp, "   -i        Search for kids with a specific input type.\n");
     fprintf(fp, "   -o        Search for kids with a specific output type.\n");
     fprintf(fp, "   -r        Generate RST output.\n");
     fprintf(fp, "   -v,-V     Give detailed documentation.\n");
     fprintf(fp, "   -h        Display this message.\n");
}


/* Behaves the exact same way as strstr, but additionally takes an integer pointer 
 * as an argument to return the index into s1 where the substrng was found. */ 
char * str_index_of(char * s1, char * s2, int * index) 
{
     char * found = _strcasestr(s1, s2);
     if (found) {
          *index = (int) (found - s1);
     } else {
          *index = -1; 
     }
     return found;
}


// implemented because vasprintf(3) is a GNU extension and not portable
int _vasprintf(char **strp, const char *fmt, va_list args)
{
    va_list args_copy;
    int status, needed;

    va_copy(args_copy, args);
    needed = vsnprintf(NULL, 0, fmt, args_copy);
    va_end(args_copy);
    if (needed < 0) {
        *strp = NULL;
        return needed;
    }
    *strp = malloc(needed + 1);
    if (!*strp) {
        error_print("failed _vasprintf malloc of *strp");
        return -1;
    }
    status = vsnprintf(*strp, needed + 1, fmt, args);
    if (status >= 0)
        return status;
    else {
        free(*strp);
        *strp = NULL;
        return status;
    }
}

// implemented because strcasestr(3) is a GNU extension and not portable
char * _strcasestr(char *a, char *b)
{
     size_t l;
     char f[3];
     snprintf(f, sizeof(f), "%c%c", tolower(*b), toupper(*b));
     for (l = strcspn(a, f); l != strlen(a); l += strcspn(a + l + 1, f) + 1)
          if (strncasecmp(a + l, b, strlen(b)) == 0)
               return(a + l);
     return(NULL);
}

// implemented because asprintf(3) is a GNU extension and not portable
int _asprintf(char **strp, const char *fmt, ...)
{
    va_list args;
    int status;

    va_start(args, fmt);
    status = _vasprintf(strp, fmt, args);
    va_end(args);
    return status;
}


char * search_and_highlight(FILE * outfp, char * text, char * keyword)
{
    if (!text || !keyword || !isatty(fileno(outfp))) return NULL;

    char * highlighted;

    char * colored_keyword;
    _asprintf(&colored_keyword, "%s%s%s", COLOR_START, keyword, COLOR_END); 
    if (!colored_keyword) return NULL;

    int first_time = 1;
    int offset;
    char * found = text;
    while (found) {
        char * beg = found;
        found = str_index_of(found, keyword, &offset);
        // if keyword not found, append rest of string to highlighted and
        // break from loop 
        if (!found) {
            if (!first_time) {
                char * p = highlighted;
                _asprintf(&highlighted, "%s%s", highlighted, beg);
                free(p);
            } else {
                _asprintf(&highlighted, "%s", beg);
            }
            break;
        }

        if (!first_time) {
            char * p = highlighted;
            _asprintf(&highlighted, "%s%.*s%s%s%s", 
                highlighted,offset, beg, COLOR_START, keyword, COLOR_END);
            free(p);
        } else {
            _asprintf(&highlighted, "%.*s%s%s%s", 
                offset, beg, COLOR_START, keyword, COLOR_END);
        }
        found = found + strlen(keyword);
        first_time = 0;
    }
    free(colored_keyword);
    return highlighted;
}

char * strreplace(const char *str, const char *old, const char *new)
{
     char *ret, *r;
     const char *p, *q;
     size_t oldlen = strlen(old);
     size_t count, retlen, newlen = strlen(new);

     if (oldlen != newlen) {
          for (count = 0, p = str; (q = strstr(p, old)) != NULL; p = q + oldlen)
               count++;
          /* this is undefined if p - str > PTRDIFF_MAX */
          retlen = p - str + strlen(p) + count * (newlen - oldlen);
    } else
          retlen = strlen(str);

    if ((ret = malloc(retlen + 1)) == NULL)
          return NULL;

    for (r = ret, p = str; (q = strstr(p, old)) != NULL; p = q + oldlen) {
          /* this is undefined if q - p > PTRDIFF_MAX */
          ptrdiff_t l = q - p;
          memcpy(r, p, l);
          r += l;
          memcpy(r, new, newlen);
          r += newlen;
     }
     strcpy(r, p);

     return ret;
}
 
char * trim(char *c) {
     char * e = c + strlen(c) - 1;
     while(*c && isspace(*c)) c++;
     while(e > c && isspace(*e)) *e-- = '\0';
     return c;
}

static inline void print_rst_header(FILE * fp, char * text) {
     fprintf(fp, "\n");
     fprintf(fp, "%s\n", text);
     int len = strlen(text);
     int i;
     for(i=0; i < len; i++) fprintf(fp, "=");
     fprintf(fp, "\n");
}

static inline void print_rst_subheader(FILE * fp, char * text) {
     fprintf(fp, "\n");
     fprintf(fp, "%s\n", text);
     int len = strlen(text);
     int i;
     for(i=0; i < len; i++) fprintf(fp, "-");
     fprintf(fp, "\n");
}

static inline void print_rst_list(FILE *fp, char **list) {
     int i;
     for (i = 0; list[i]; i++) {
          if (i >0) {
               fprintf(fp, ", ");
          }
          fprintf(fp, "%s", list[i]);
     }
     fprintf(fp, "\n");
}

static inline void print_rst_portlist(FILE * fp, proc_port_t * portlist) 
{
     fprintf(fp, "\n");
     int i;
     for (i = 0; portlist[i].label != NULL; i++) {
          fprintf(fp, "``:%s``\n", portlist[i].label);
          fprintf(fp, "    %s\n", portlist[i].description);
     }
}

#endif // _WSMAN_UTIL_H
