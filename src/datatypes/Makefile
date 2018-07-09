#
# WS Datatypes
#

include ../common.mk

CFLAGS += $(PERF_FLAG)

MODULES = $(patsubst %.c, %$(WS_SFX), $(wildcard wsdt_*.c))

CFLAGS += -shared

#DESTDIR = $(WS_HOME)/datatypes/$(PDIR)
DESTDIR = $(WS_LIB_DIR)

#all: $(PDIR) $(MODULES) $(DESTDIR)
all: $(DESTDIR) $(MODULES)

#$(PDIR):
#	$(MKDIR) $(PDIR)

$(DESTDIR):
	$(MKDIR) $(DESTDIR)

%$(WS_SFX): %.c
	$(SHOWFILE)
	$(CC) $(CFLAGS) $< -o $@
ifndef ISBSD
	$(CP) -t $(DESTDIR) $@
else
	$(CP) $@ $(DESTDIR)
endif

clean:
	$(RM) *.o .*.swp ._* *.8 *_so
	$(RM) -r *.dSYM
