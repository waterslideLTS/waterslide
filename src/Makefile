#
# Main build
#

include ./common.mk

all: $(PBDIR) rule rule2

$(PBDIR):
	@echo "Building protobuf library (takes a while)" 
	$(MKDIR) -p $(PBBUILD)
	$(TAR) xvfj $(WS_BUILDROOT)/pkg/$(PBPKG) -C $(PBBUILD) $(QUIETOUT)
	$(CD) $(PBBUILD)/* && CXXFLAGS="$(OPT_LEVEL)" ./configure --prefix=$(PBDIR) --enable-shared=no --with-pic=yes \
		--disable-debug $(QUIETOUT) && $(MAKE) install $(QUIETOUT)
	$(RM) -r $(PBBUILD)

ifndef NORE2
$(RE2DIR):
	@echo "Building re2 library (takes a while)"
	$(MKDIR) -p $(RE2BUILD)
	$(TAR) xfz $(WS_BUILDROOT)/pkg/$(RE2PKG) -C $(RE2BUILD) $(QUIETOUT)
	$(MAKE) -C $(RE2BUILD)/re2 CXXFLAGS='$(OPT_LEVEL) -g -pthread -fPIC' prefix=$(RE2DIR) all install $(QUIETOUT)
	$(RM) -r $(RE2BUILD)
endif

ifndef NOHWLOC
# Switching to automake would make this nicer.
# When building in "embedded mode", hwloc's Make install is a no-op
$(HWLOC_LINK):
	@echo "Building hwloc library"
	$(MKDIR) -p $(HWLOC_BUILDDIR)
	$(TAR) xfj $(WS_BUILDROOT)/pkg/$(HWLOC_PKG) -C $(HWLOC_BUILDROOT) $(QUIETOUT)
	$(CD) $(HWLOC_BUILDDIR) && ./configure --enable-embedded-mode --disable-libxml2 --disable-pci --disable-libnuma --disable-opencl --disable-cuda --disable-nvml --disable-gl $(QUIETOUT) && $(MAKE) $(QUIETOUT)
	$(INSTALL) -d -m 755 $(HWLOC_INCLUDE)
	$(INSTALL) -d -m 755 $(HWLOC_INCLUDE)/hwloc
	$(INSTALL) -d -m 755 $(HWLOC_INCLUDE)/hwloc/autogen
	$(INSTALL) -m 644 $(HWLOC_BUILDDIR)/include/*.h $(HWLOC_INCLUDE)/
	$(INSTALL) -m 644 $(HWLOC_BUILDDIR)/include/hwloc/*.h $(HWLOC_INCLUDE)/hwloc/
	$(INSTALL) -m 644 $(HWLOC_BUILDDIR)/include/hwloc/autogen/*.h $(HWLOC_INCLUDE)/hwloc/autogen/
	$(INSTALL) -d -m 755 $(HWLOC_LIB)
	$(INSTALL) -m 644 $(HWLOC_BUILDDIR)/src/.libs/libhwloc_embedded.a $(HWLOC_LIB)/
	$(RM) -r $(HWLOC_BUILDROOT)
endif

# Hold off on building any WS components until protobuf is done
rule: $(PBDIR) $(RE2DIR) $(HWLOC_LINK)
	@echo "Building core libraries"
	$(QUIET)$(MAKE) --no-print-directory -C datatypes 
	$(QUIET)$(MAKE) --no-print-directory -C lib 
	@echo "Building utilities"
	$(QUIET)$(MAKE) --no-print-directory -C util 

# Note: -<action> : ignore return status
rule2: rule $(PBDIR) $(RE2DIR) $(HWLOC_LINK)
	@echo "Making kids"
ifndef NOPB
	-$(QUIET)$(MAKE) --no-print-directory -C procs/protobuf 
endif
	$(QUIET)$(MAKE) --no-print-directory -k -C procs 
	$(WSALIAS) > $(WS_PROCS_DIR)/.wsalias

ifdef DESTDIR
  DEFAULT_WS_INSTALL=$(DESTDIR)/waterslide
else ifeq "$(USER)" "root"
  DEFAULT_WS_INSTALL=/usr/local/waterslide
else
  DEFAULT_WS_INSTALL=$(HOME)/local/waterslide
endif

# if WS_INSTALL is not set by the user....
WS_INSTALL ?= $(DEFAULT_WS_INSTALL)

INSTALL_BIN = $(WS_INSTALL)/bin
INSTALL_LIB = $(WS_INSTALL)/lib
INSTALL_PROCS = $(WS_INSTALL)/procs
INSTALL_INC = $(WS_INSTALL)/include


install:
# if we can't create the target directory or it already exists but we can't write to it...
	$(MKDIR) $(WS_INSTALL) 2>/dev/null && [ -w $(WS_INSTALL) ] || (echo "***ERROR: Can't install into $(WS_INSTALL) as current user" ; exit 1)
	@echo "Installing waterslide into $(WS_INSTALL)"

# INCLUDE (and Datatypes):
	@/bin/bash -c '(cd $(WS_INC_DIR) ; for d in `find . -type d`; do $(INSTALL_EXEC) -d $(INSTALL_INC)/$${d} && files=`find $${d} -maxdepth 1 -not -type d` && [ "$${files}" != "" ] && $(INSTALL_EXEC) -m 0644 $${files} $(INSTALL_INC)/$${d} ; done)'
	@/bin/bash -c '(cd $(WS_BUILDROOT)/datatypes ; for d in `find . -type d`; do $(INSTALL_EXEC) -d $(INSTALL_INC)/datatypes/$${d} && files=`find $${d} -maxdepth 1 -not -type d` && [ "$${files}" != "" ] && $(INSTALL_EXEC) -m 0644 $${files} $(INSTALL_INC)/datatypes/$${d} ; done)'
# BIN:
	@$(INSTALL_EXEC) -d $(INSTALL_BIN)
	@$(INSTALL_EXEC) $(WS_BIN_DIR)/* $(INSTALL_BIN)
# LIB:
	@/bin/bash -c '(cd $(WS_LIB_DIR) ; for d in `find . -type d`; do $(INSTALL_EXEC) -d $(INSTALL_LIB)/$${d} && files=`find $${d} -maxdepth 1 -not -type d` && [ "$${files}" != "" ] && $(INSTALL_EXEC) -m 0755 $${files} $(INSTALL_LIB)/$${d} ; done)'
# PROCS:
	@/bin/bash -c '(cd $(WS_PROCS_DIR) ; for d in `find . -type d`; do $(INSTALL_EXEC) -d $(INSTALL_PROCS)/$${d} && files=`find $${d} -maxdepth 1 -not -type d` && [ "$${files}" != "" ] && $(INSTALL_EXEC) -m 0755 $${files} $(INSTALL_PROCS)/$${d} ; done)'

# clear any version control remnants
	$(FIND) $(WS_INSTALL) -name ".git*" -exec rm {} \;

ifdef DESTDIR
	@echo "To make the tools globally available, execute:"
	@echo "  sudo ln -sf $(WS_INSTALL)/bin/* /usr/local/bin"
	@echo -n "Otherwise, "
else ifeq "$(USER)" "root"
	@echo "To make the tools globally available, execute:"
	@echo "  sudo ln -sf $(WS_INSTALL)/bin/* /usr/local/bin"
	@echo -n "Otherwise, "
endif
	@echo "copy/paste the line below to use the newly installed components"
	@echo '  export PATH=$(INSTALL_BIN):$${PATH}'


uninstall:
	@echo "Uninstalling waterslide from $(WS_INSTALL)"
	$(RM) -r $(INSTALL_LIB) $(INSTALL_PROCS) $(INSTALL_INC) $(INSTALL_BIN)
# Line below will only work if $(WS_INSTALL) is empty
	$(RMDIR) $(WS_INSTALL)

clean:
	@echo "Cleaning core libraries"
	$(QUIET)$(MAKE) --no-print-directory -C datatypes clean
	$(QUIET)$(MAKE) --no-print-directory -C lib clean
	@echo "Cleaning utilities"
	$(QUIET)$(MAKE) --no-print-directory -C util clean
	@echo "Cleaning kids"
	$(QUIET)$(MAKE) --no-print-directory -C procs clean

