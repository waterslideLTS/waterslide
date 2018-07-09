
# General project Makefile contents

# Build verbosity
#   Define WS_VERBOSEBUILD for verbose build

# As this file (common.mk) is stored at .../src,
#   derive WS_HOME and WS_BUILTROOT from here
WS_BUILDROOT := $(patsubst %/,%, $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
WS_HOME := $(patsubst %/,%, $(dir $(abspath $(WS_BUILDROOT))))

ifndef WS_VERBOSEBUILD
  QUIET = @
endif
ifdef QUIET
  QUIETOUT = > /dev/null 2>&1
endif

WS_BIN_DIR = $(WS_HOME)/bin
WS_LIB_DIR = $(WS_HOME)/lib
WS_PROCS_DIR = $(WS_HOME)/procs
WS_INC_DIR = $(WS_BUILDROOT)/include

ifndef NOPB
  PBDIR = $(WS_LIB_DIR)/protobuflib
  PBBUILD = $(WS_BUILDROOT)/pkg/pbtemp
  PBPKG = protobuf-2.5.0*
  PBEXE = $(QUIET)$(PBDIR)/bin/protoc
endif

ifndef NORE2
  RE2DIR = $(WS_LIB_DIR)/re2
  RE2BUILD = $(WS_BUILDROOT)/pkg/re2temp
  RE2PKG = re2-20140304.tgz
  RE2INCLUDE = $(RE2DIR)/include
  RE2LIB = $(RE2DIR)/lib
endif

ifndef NOHWLOC
  HWLOC_VER = 1.9
  HWLOC_DIR = $(WS_LIB_DIR)/hwloc-$(HWLOC_VER)
  HWLOC_BUILDROOT = $(WS_BUILDROOT)/pkg/hwlocBuildDir
  HWLOC_BUILDDIR = $(HWLOC_BUILDROOT)/hwloc-$(HWLOC_VER)
  HWLOC_PKG = hwloc-$(HWLOC_VER).tar.bz2
  HWLOC_INCLUDE = $(HWLOC_DIR)/include
  HWLOC_LIB = $(HWLOC_DIR)/lib
  HWLOC_LINK = $(HWLOC_LIB)/libhwloc_embedded.a
  HWLOC_CFLAGS = -I$(HWLOC_INCLUDE) -DUSE_HWLOC
endif

# Compiler settings

ifndef OPT_LEVEL
  OPT_LEVEL = -O3
endif

CFLAGS = $(OPT_LEVEL) -Wall -fpic -std=gnu99
ifndef WS_STRIPPED
  CFLAGS += -g
endif
CFLAGS += -D_GNU_SOURCE
ifdef HUGETUPLE
  CFLAGS += -DHUGETUPLE
endif

ifdef USEM64
  CFLAGS += -m64
endif

WS_INCLUDE = -I$(WS_BUILDROOT)/include -I$(WS_BUILDROOT) -I.
LDFLAGS += -L$(WS_LIB_DIR)

LDFLAGS += -lm -lz -lpthread

ifndef WSLIB
  WSLIB=$(INSTALL_TARGET)/lib
endif



# OS specific items

ifndef OSNAME
  OSNAME = $(word 1,$(shell uname))
  ifndef OSNAME
    OSNAME = "unknown"
  endif
endif

# Handle BSD a little differently

ifeq "$(OSNAME)" "FreeBSD"
  NODL=1
  ISBSD=1
  OSNAME=LINUX
  ISBSD=1
endif

ifndef NODL
  LDFLAGS += -ldl
endif

# Other options: "SunOS", "Linux", "CYGWIN_NT-5.0", "Darwin"

ifeq "$(OSNAME)" "Darwin"
  CFLAGS += -undefined dynamic_lookup
  ISBSD=1
  ISDARWIN=1
  WHOLE_ARCHIVE=-all_load
  NO_WHOLE_ARCHIVE=-noall_load
else
  WHOLE_ARCHIVE=--whole-archive
  NO_WHOLE_ARCHIVE=--no-whole-archive
  LDFLAGS += -lrt
endif

# Tools
CC = $(QUIET)gcc
CPP = $(QUIET)g++
FLEX = $(QUIET)flex
BISON = $(QUIET)bison
LINK = $(QUIET)ln -s
AR = $(QUIET)ar
RM = $(QUIET)rm -f
IF = $(QUIET)if
ifdef ISBSD
RMDIR = -$(QUIET)rmdir
else
RMDIR = $(QUIET)rmdir --ignore-fail-on-non-empty
endif
MKDIR = $(QUIET)mkdir -p
TAR = $(QUIET)tar
CD = $(QUIET)cd
CP = $(QUIET)cp
LINK = $(QUIET)ln -s -f
TOUCH = $(QUIET)touch
FIND = $(QUIET)find
ifndef INSTALL_EXEC
  INSTALL_EXEC = install
endif
INSTALL = $(QUIET)$(INSTALL_EXEC)
INSTALL_DIR_CMD = $(INSTALL) -d
WSALIAS = $(QUIET)$(WS_BIN_DIR)/wsalias

SHOWFILE = @echo "    $@"
# to elide errors, for use with CP or INSTALL where there may be no sources
NOERROR = 2>/dev/null || true

CFLAGS += $(WS_INCLUDE)

ifdef HASWSPERF
  PERF_FLAG += -DWS_PERF
endif
ifdef HASSQPERF
  PERF_FLAG += -DSQ_PERF
endif
ifdef HASLOCKDBG
  CFLAGS += -DWS_LOCK_DBG
endif

ATOMIC_STACK_STATE = -DUSE_ATOMICS=1

#THREAD_DEFS = -DWS_PTHREADS -DUSE_ATOMIC_STACK=1 $(ATOMIC_STACK_STATE)
THREAD_DEFS = -DWS_PTHREADS -DUSE_MUTEX_HOMED_FREE_LIST=1 $(ATOMIC_STACK_STATE) $(HWLOC_CFLAGS)

WS_SFX = .ws_so
ifdef WS_PARALLEL
  CFLAGS += $(THREAD_DEFS)
  SH_FLAGS = -DSHARED_KID
  OWMR_FLAGS = -DOWMR_TABLES
  WS_SFX = .wsp_so
endif

CPPFLAGS = $(filter-out -std=gnu99, $(CFLAGS))

