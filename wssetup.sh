#!/bin/bash

if [ "$BASH_ARGV" != "" ] ; then
     REALHOME=`readlink -f ${BASH_SOURCE[0]}`
     WS_PATH=`dirname $REALHOME`/bin
     export PATH=${WS_PATH}:$PATH
     echo "*** Path to waterslide set to ${WS_PATH}"
else 
     echo "*** This file ($0) has to be sourced in bash, not just executed;"
     echo "    e.g., source $0."
fi
