
# WS Procs (a.k.a. kids)

include ../common.mk

CFLAGS += $(PERF_FLAG)

# Switches for various build options

# HASMAGIC = 1	     # if you have magic.h

# NOPB = 1           # to skip the wsproto build
# NORE2 = 1          # to skip the re2 build

ifndef NOPB
  CFLAGS += -DPBMETA
endif

#CFLAGS += $(WS_INCLUDE) $(WS_DEFS)
CFLAGS += -Wno-unknown-pragmas -Wno-strict-aliasing
LDFLAGS += -shared 

MODULES = $(patsubst %.c, %$(WS_SFX), $(wildcard *.c))
MODULES += $(patsubst %.cc, %$(WS_SFX), $(wildcard *.cc))

all: $(WS_PROCS_DIR) $(MODULES) 

$(WS_PROCS_DIR): 
	$(MKDIR) $(WS_PROCS_DIR)

#********

proc_calc$(WS_SFX): proc_calc.c $(WS_BUILDROOT)/include/wscalc.h

proc_tcpcatch$(WS_SFX): proc_tcpcatch.c $(WS_BUILDROOT)/include/tcp_rw.h
	$(SHOWFILE) 
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)

proc_tcpthrow$(WS_SFX): proc_tcpthrow.c $(WS_BUILDROOT)/include/tcp_rw.h
	$(SHOWFILE) 
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)

ifdef HASMAGIC
proc_filemagic$(WS_SFX): proc_filemagic.c
	$(SHOWFILE) 
	$(CC) $(CFLAGS) $< -o $@ -lmagic $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)
else
proc_filemagic$(WS_SFX): proc_filemagic.c
	@echo " not building $@, set HASMAGIC=1 to build"

endif


ifdef HASCRYPTO
proc_hmac$(WS_SFX): proc_hmac.c
	$(SHOWFILE) 
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS) -lcrypto
	$(INSTALL) $@ $(WS_PROCS_DIR)
else
proc_hmac$(WS_SFX): proc_hmac.c
	@echo " not building $@, set HASCRYPTO=1 to build"
endif


ifdef HASPCAP
proc_pcapin$(WS_SFX): proc_pcapin.c
	$(SHOWFILE) 
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS) -lpcap 
	$(INSTALL) $@ $(WS_PROCS_DIR)
else
proc_pcapin$(WS_SFX): proc_pcapin.c
	@echo " not building $@, set HASPCAP=1 to build"
endif


ifndef NOPB
  ifdef PBNONLOCAL
    PBFLAGS += -rdynamic -Wl,-rpath,`pkg-config protobuf --variable=libdir`
    PBLIB+= `pkg-config --cflags --libs protobuf-lite`
  else
    PBFLAGS += -rdynamic -I$(PBDIR)/include -pthread
    PBLIB+= -L$(PBDIR)/lib -lprotobuf-lite -pthread -lpthread
  endif

protobuf/wsserial.pb.cc: 
	$(QUIET)$(MAKE) --no-print-directory -C protobuf

proc_wsproto_in$(WS_SFX):proc_wsproto_in.cc protobuf/wsserial.pb.cc protobuf/wsproto.pb.cc
	$(SHOWFILE) 
	$(CPP) $(PBFLAGS) $(CPPFLAGS) $^ -o $@ $(PBLIB) $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)
proc_wsproto_out$(WS_SFX): proc_wsproto_out.cc protobuf/wsserial.pb.cc protobuf/wsproto.pb.cc
	$(SHOWFILE) 
	$(CPP) $(PBFLAGS) $(CPPFLAGS) $^ -o $@ $(PBLIB) $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)
proc_lastn$(WS_SFX): proc_lastn.c protobuf/wsproto.pb.cc
	$(SHOWFILE) 
	$(CPP) $(PBFLAGS) $(CPPFLAGS) $^ -o $@ $(PBLIB) $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)

else
proc_lastn$(WS_SFX): proc_lastn.c
	$(SHOWFILE) 
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)
proc_wsproto_in$(WS_SFX): proc_wsproto_in.cc
	@echo " not building $@, unset NOPB to build"
proc_wsproto_out$(WS_SFX): proc_wsproto_out.cc
	@echo " not building $@, unset NOPB to build"
endif

ifndef NORE2
proc_re2$(WS_SFX): proc_re2.cc
	$(SHOWFILE) 
	$(CPP) $(CPPFLAGS) -I$(RE2INCLUDE) $< -o $@ $(RE2LIB)/libre2.a $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)
else
proc_re2$(WS_SFX): proc_re2.cc
	@echo " not building $@, unset NORE2 to build"
endif

ifdef HASDPU
DPUFLAGS += `pkg-config --cflags funnycar`
DPULIB   += `pkg-config --libs funnycar`
proc_dpu$(WS_SFX): proc_dpu.c
	$(SHOWFILE)
	$(CC) $(CFLAGS) $(DPUFLAGS) $< -o $@ $(DPULIB) $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)
else
proc_dpu$(WS_SFX): proc_dpu.c
	@echo " not building $@, set HASDPU=1 to build"
endif

ifdef HASRDKAFKA
proc_kafka_in$(WS_SFX): proc_kafka_in.c
	$(SHOWFILE) 
	$(CC) $(CFLAGS) $< -o $@ -lrdkafka $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)
proc_kafka_multi_in$(WS_SFX): proc_kafka_multi_in.c
	$(SHOWFILE) 
	$(CC) $(CFLAGS) $< -o $@ -lrdkafka $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)
proc_kafka_out$(WS_SFX): proc_kafka_out.c
	$(SHOWFILE) 
	$(CC) $(CFLAGS) $< -o $@ -lrdkafka $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)
else
proc_kafka_in$(WS_SFX): proc_kafka_in.c
	@echo " not building $@, need librdkafka, set HASRDKAFKA=1 to build"
proc_kafka_multi_in$(WS_SFX): proc_kafka_multi_in.c
	@echo " not building $@, need librdkafka, set HASRDKAFKA=1 to build"
proc_kafka_out$(WS_SFX): proc_kafka_out.c
	@echo " not building $@, need librdkafka, set HASRDKAFKA=1 to build"
endif

ifdef HASREDIS
proc_redis$(WS_SFX): proc_redis.c
	$(SHOWFILE) 
	$(CC) $(CFLAGS) $< -o $@ -lhiredis $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)
else
proc_redis$(WS_SFX): proc_redis.c
	@echo " not building $@, need libhiredis, set HASREDIS=1 to build"
endif


ifndef WS_PARALLEL
# These implement fanout, and so are only useful for pthreads
proc_workbalance$(WS_SFX): proc_workbalance.c
	@echo " not building $@, only built for WS_PARALLEL=1"
proc_workreceive$(WS_SFX): proc_workreceive.c
	@echo " not building $@, only built for WS_PARALLEL=1"
endif

#********
#*****
#********

# Default processing

%$(WS_SFX): %.c
	$(SHOWFILE)
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)

%$(WS_SFX): %.cc
	$(SHOWFILE)
	$(CPP) $(CPPFLAGS) $< -o $@ $(LDFLAGS)
	$(INSTALL) $@ $(WS_PROCS_DIR)

clean:
	$(RM) *.o .*.swp *.dSYM ._* *.8 *_so 
	$(QUIET)$(MAKE) --no-print-directory -C protobuf clean

