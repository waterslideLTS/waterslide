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

#ifndef _GRAPHBUILDER_H
#define _GRAPHBUILDER_H

#include "cppwrap.h"

#ifdef __cplusplus
CPP_OPEN
#endif // __cplusplus

/* Forward declarations */
struct parse_graph_t;
struct mimo_t;

/**
 * Add a file to the list to be parsed
 * */
void pg_add_file(const char *fname);

/**
 * Set the string (from the commandline) to be parsed
 */
void pg_set_cmdstring(const char *str);


/**
 * Returns 1 on success, 0 on error
 */
int pg_parse();


/**
 * Compiles the parsed AST
 */
parse_graph_t* pg_buildGraph(mimo_t *mimo);

/**
 * Call when done to cleanup memory
 */
void pg_cleanup(void);

#ifdef __cplusplus
CPP_CLOSE
#endif // __cplusplus

#endif // _GRAPHBUILDER_H
