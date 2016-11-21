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

#ifndef _POPEN2_H
#define _POPEN2_H

#include <unistd.h>

#define POPEN_READ 0
#define POPEN_WRITE 1


// taken from https://dzone.com/articles/simple-popen2-implementation
static inline pid_t popen2(const char *command, int *infp, int *outfp) {
     int p_stdin[2], p_stdout[2];
     pid_t pid;

     //if you want non-blocking pipe, use pipe2 instead
     if (pipe(p_stdin) != 0 || pipe(p_stdout) != 0)
          return -1;

     pid = fork();

     if (pid < 0) {
          return pid;
     }
     else if (pid == 0) {
          close(p_stdin[POPEN_WRITE]);
          dup2(p_stdin[POPEN_READ], POPEN_READ);
          close(p_stdout[POPEN_READ]);
          dup2(p_stdout[POPEN_WRITE], POPEN_WRITE);

          execl("/bin/sh", "sh", "-c", command, NULL);
          perror("execl");
          exit(1);
     }

     if (infp == NULL) {
          close(p_stdin[POPEN_WRITE]);
     }
     else {
          *infp = p_stdin[POPEN_WRITE];
     }

     if (outfp == NULL) {
          close(p_stdout[POPEN_READ]);
     }
     else {
          *outfp = p_stdout[POPEN_READ];
     }

     return pid;
}
#endif // _POPEN2_H
