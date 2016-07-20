
# WS Libraries

include ../common.mk

CFLAGS += $(PERF_FLAG)

FILELIST = $(patsubst %.c, %.o, $(wildcard *.c))
FILELIST += $(patsubst %.cc, %.o, $(wildcard *.cc))
FILELIST += wscalc.o wscalc.tab.o
FILELIST += graph.tab.o graph.lex.o

FILES = $(FILELIST)

ifdef WS_PARALLEL
  SUFFIX = -p
  WSARCHIVEFILE = libwaterslide-parallel.a
else
  SUFFIX = -s
  WSARCHIVEFILE = libwaterslide.a
endif

MODULES = $(sort $(patsubst %.o, %$(SUFFIX).o, $(FILES)))

all: $(MODULES) $(WSARCHIVEFILE)

wsperf.c: 
	$(SHOWFILE)

$(WSARCHIVEFILE): $(MODULES)
	$(SHOWFILE)
	$(RM) $(WSARCHIVEFILE)  # otherwise ar leaves old code in there
	$(AR) rcs $@ $(MODULES)
	$(CP) $@ $(WS_LIB_DIR) 

graph.lex.cc: graph.l graph.tab.hh
	$(SHOWFILE)
	$(FLEX) -Ppg -ograph.lex.cc graph.l

graph.tab.cc graph.tab.hh: graph.y
	$(SHOWFILE)
	$(BISON) $(BISON_FLAGS) -d -ppg graph.y -o graph.tab.cc

wscalc.c: wscalc.l wscalc.tab.h $(WS_BUILDROOT)/include/wscalc.h
	$(SHOWFILE)
	$(FLEX) -owscalc.c -Pwscalc wscalc.l

wscalc.tab.c wscalc.tab.h: wscalc.y $(WS_BUILDROOT)/include/wscalc.h
	$(SHOWFILE)
	$(BISON) -d -pwscalc wscalc.y -o wscalc.tab.c

%$(SUFFIX).o: %.c
	$(SHOWFILE)
	$(CC) $(CFLAGS) -c $< -o $@

%$(SUFFIX).o: %.cc
	$(SHOWFILE)
	$(CPP) $(CPPFLAGS) -c $< -o $@

clean:
	$(RM) *.so .*.swp *.dSYM ._* *.o shared/*.o 
	$(RM) parse_graph.* graph.lex* graph.tab* wscalc.c wscalc.tab* wscalc_int.c wscalc_int.tab.c wscalc_int.tab.h
	$(RM) libwaterslide.dylib libwaterslide.a libwaterslide-parallel.a

graph.tab$(SUFFIX).o: graph.tab.cc
graph.lex$(SUFFIX).o: graph.lex.cc ast.h
graphBuilder$(SUFFIX).o: graph.tab.hh ast.h
ast$(SUFFIX).o: ast.cc ast.h
