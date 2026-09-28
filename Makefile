all::

## Configurable defaults
PREFIX=/usr/local
FIND=find
TAR=tar

VWORDS:=$(shell src/getversion.sh --prefix=v MAJOR MINOR)
VERSION:=$(word 1,$(VWORDS))
BUILD:=$(word 2,$(VWORDS))

## Provide a version of $(abspath) that can cope with spaces in the
## current directory.
myblank:=
myspace:=$(myblank) $(myblank)
MYCURDIR:=$(subst $(myspace),\$(myspace),$(CURDIR)/)
MYABSPATH=$(foreach f,$1,$(if $(patsubst /%,,$f),$(MYCURDIR)$f,$f))

-include gridmon-env.mk
-include $(call MYABSPATH,config.mk)

hidden_scripts += perfsonar-stats
hidden_scripts += static-metrics
hidden_scripts += xrootd-monitor
hidden_scripts += xrootd-stats
hidden_scripts += xrootd-detail
hidden_scripts += qstats-exporter
hidden_scripts += cephhealth-exporter
hidden_scripts += pgcomp
hidden_scripts += ip-statics-exporter
hidden_scripts += get-cert-expiry
hidden_scripts += hammercloud-events
hidden_scripts += jiggers-events
hidden_scripts += kafka-exporter
hidden_scripts += push-srr


BINODEPS_SHAREDIR=src/share
BINODEPS_SCRIPTDIR=$(BINODEPS_SCRIPTDIR)
SHAREDIR ?= $(PREFIX)/share/gridmon
LIBEXECDIR ?= $(PREFIX)/libexec/gridmon

python3_zips += apps
apps_pyproto += lancs_gridmon/metrics/remote_write



include binodeps.mk
include pynodeps.mk

all:: python-zips out/VERSION out/BUILD
install:: install-python-zips
install:: install-hidden-scripts

out/gridmon.tgz: PREFIX=scratch/out
out/gridmon.tgz: all install
	$(TAR) czf '$@' -C $(PREFIX) share

tidy::
	$(FIND) . -name "*~" -delete

MYCMPCP=$(CMP) -s '$1' '$2' || $(CP) '$1' '$2'
.PHONY: prepare-version
prepare-version:
	@$(MKDIR) tmp/
ifneq ($(BUILD),)
	$(file >tmp/BUILD,$(BUILD))
endif
ifneq ($(VERSION),)
	$(file >tmp/VERSION,$(VERSION))
endif
out/BUILD: prepare-version
	@$(call MYCMPCP,tmp/BUILD,$@)
out/VERSION: prepare-version
	@$(call MYCMPCP,tmp/VERSION,$@)
