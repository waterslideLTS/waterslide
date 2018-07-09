#
# WS Utils
#

include ../common.mk

LDFLAGS += -rdynamic -lstdc++

SOURCES = $(wildcard *.c)
UTILS = $(patsubst %.c, %, $(SOURCES))

all: $(UTILS)

ifndef WS_PARALLEL
waterslide: waterslide.c ../lib/libwaterslide.a
	$(SHOWFILE)
	$(CC) $(CFLAGS) $(PERF_FLAG) $< -o $@ -Wl,$(WHOLE_ARCHIVE) $(WS_LIB_DIR)/libwaterslide.a -Wl,$(NO_WHOLE_ARCHIVE) $(LDFLAGS)
ifndef ISBSD
	$(CP) -t $(WS_BIN_DIR) $@
else
  ifeq "$(WS_BIN_DIR)" ""
  $(error WS_BIN_DIR is empty...cannot copy into unspecified directory)
  endif
	$(CP) $@ $(WS_BIN_DIR)
endif

else
waterslide: waterslide.c
	@echo " Unset WS_PARALLEL to build $@"
endif

ifdef WS_PARALLEL
waterslide-parallel: waterslide-parallel.c ../lib/libwaterslide-parallel.a $(HWLOC_LINK)
	$(SHOWFILE)
	$(CC) $(CFLAGS) $(PERF_FLAG) $< -o $@ -Wl,$(WHOLE_ARCHIVE) $(WS_LIB_DIR)/libwaterslide-parallel.a -Wl,$(NO_WHOLE_ARCHIVE) $(LDFLAGS) $(HWLOC_LINK)
ifndef ISBSD
	$(CP) -t $(WS_BIN_DIR) $@
else
  ifeq "$(WS_BIN_DIR)" ""
  $(error WS_BIN_DIR is empty...cannot copy into unspecified directory)
  endif
	$(CP) $@ $(WS_BIN_DIR)
endif

else
waterslide-parallel: waterslide-parallel.c
	@echo " Set WS_PARALLEL=1 to build $@"
endif

wsman: wsman.c wsman_color.h wsman_map.h wsman_util.h wsman_word_wrap.h
	$(SHOWFILE)
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)
ifndef ISBSD
	$(CP) -t $(WS_BIN_DIR) $@
else
  ifeq "$(WS_BIN_DIR)" ""
  $(error WS_BIN_DIR is empty...cannot copy into unspecified directory)
  endif
	$(CP) $@ $(WS_BIN_DIR)
endif

%: %.c
	$(SHOWFILE)
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)
ifndef ISBSD
	$(CP) -t $(WS_BIN_DIR) $@
else
  ifeq "$(WS_BIN_DIR)" ""
  $(error WS_BIN_DIR is empty...cannot copy into unspecified directory)
  endif
	$(CP) $@ $(WS_BIN_DIR)
endif

clean:
	$(RM) -r *.dSYM *.o $(UTILS)
