" Vim syntax file
" Language: WaterSlide
" Maintainer: 
" Latest Revision: 02 July 2013
"
" Usage: Add the following line to your .vimrc to set
" the vim filetype when you open the file:
"
" au BufNewFile,BufRead waterslide_* setlocal ft=ws
"
" Copy this file into ~/.vim/syntax/

if exists("b:current_syntax")
  finish
endif

syn match   wsKeyword     "%[a-zA-Z0-9-_]*"
syn match   wsOperator    "|"
syn match   wsOperator    "->"
syn match   wsComment     "\(\/\/.*\)"
syn match   wsComment     "\(#.*\)"
syn match   wsNumber      "[-+]\=\<\d\+\(\.\d*\)\=\>"
syn match   wsLabel       "\<[A-Z0-0][A-Z0-9_]\+\>"
syn match   wsVariable    "$[a-zA-Z0-9_]*"

syn region  wsString      start=+"+ skip=+\\"+ end=+"+
syn region  wsString      start=+'+ skip=+\\'+ end=+'+
syn region  wsString      start=+`+ skip=+\\'+ end=+`+

" All the WS procs, will need to be manually maintained
let kidstring=system("wsalias 2>/dev/null | tr ',' '\n' | tr ':' '\n' | sed -e 's/^[ ]*//'")
for kid in split(kidstring, '\v\n')
     let s:syntax = 'syn keyword wsProc ' . kid
     exec s:syntax
endfor

let b:current_syntax = "ws"

" map ws groups to standard groups
hi def link wsTodo        Todo
hi def link wsComment     Comment
hi def link wsProc        Keyword
hi def link wsKeyword     Keyword
hi def link wsString      Constant
hi def link wsNumber      Constant
hi def link wsOperator    Operator
hi def link wsLabel       Special
hi def link wsVariable    Identifier
