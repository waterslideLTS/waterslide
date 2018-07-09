#
# Top-level build for waterslide
#
#   Invokes the next layer twice:  first for serial WS, second for parallel WS
#

include ./src/common.mk

DIR_ERROR=0
ifeq "$(WS_BIN_DIR)" ""
  DIR_ERROR = 1
endif
ifeq "$(WS_LIB_DIR)" ""
  DIR_ERROR = 1
endif

.PHONY: all install test uninstall clean
all:
#	@export WS_HOME
ifndef WS_PARALLEL
	@echo "Building SERIAL waterslide"
	@unset WS_PARALLEL ; $(MAKE) --no-print-directory -C src
	$(LINK) bin/waterslide
else
	@echo "Building PARALLEL waterslide"
	@WS_PARALLEL=1 $(MAKE) --no-print-directory -C src
	$(LINK) bin/waterslide-parallel
endif
	$(LINK) bin/wsalias
	$(LINK) bin/wsman

	@echo "waterslide build completed"

install:
	$(MAKE) -C src install

test: all install test/kidtest/kidtest.sh
	./test/kidtest/kidtest.sh
	@echo "waterslide kidtests completed"

uninstall:
	$(MAKE) -C src uninstall

clean:
	@unset WS_PARALLEL ; $(MAKE) --no-print-directory -C src clean
	$(RM) waterslide waterslide-parallel wsalias wsman
ifneq "$(DIR_ERROR)" "1"
#	$(RM) -r $(WS_LIB_DIR)
# don't delete the protobuf stuff; use "make scour" for that
ifneq "$(wildcard $(WS_LIB_DIR))" ""
	$(FIND) $(WS_LIB_DIR) -maxdepth 1 -type f -delete
endif
	$(RM) $(WS_BIN_DIR)/* $(WS_PROCS_DIR)/proc_*
	$(RM) -r $(RPM_OUTDIR)
else
	@echo "*** Error: problem with distribution directories"
endif

.PHONY: scour
scour: clean
	-$(RM) -r $(WS_LIB_DIR)


DT := $(shell date -u +%Y%m%d.%H%M)
BASEDISTRO := `basename $(PWD)`
DIRNAME := `dirname $(PWD)`
tar: scour
	@echo "Cleaning git repository"
	@echo "distro $(BASEDISTRO)  --  $(PWD) -- $(DIRNAME)"
	-@git gc
	@echo "Building ../$(BASEDISTRO)_$(DT).tar.bz2"
	@cd .. && tar -cvjf $(BASEDISTRO)_$(DT).tar.bz2 $(BASEDISTRO)/. --exclude=".nfs*" > /dev/null
	@du -h "$(DIRNAME)/$(BASEDISTRO)_$(DT).tar.bz2"

tar_ng: scour
	@echo "Cleaning git repository"
	@echo "Building ../$(BASEDISTRO)_$(DT)_nogit.tar.bz2"
	@cd .. && tar -cvjf $(BASEDISTRO)_$(DT)_nogit.tar.bz2 $(BASEDISTRO)/. --exclude=.git* --exclude=*.nfs* --group=nobody --owner=nobody >/dev/null
	@du -h "$(DIRNAME)/$(BASEDISTRO)_$(DT)_nogit.tar.bz2"

tar_nogit: tar_ng

