all::

## Configurable defaults
PREFIX=/usr/local
FIND=find

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
hidden_scripts += xrootd-stats
hidden_scripts += xrootd-detail
hidden_scripts += qstats-exporter
hidden_scripts += cephhealth-exporter
hidden_scripts += ip-statics-exporter


BINODEPS_SHAREDIR=src/share
BINODEPS_SCRIPTDIR=$(BINODEPS_SCRIPTDIR)
SHAREDIR ?= $(PREFIX)/share/gridmon
LIBEXECDIR ?= $(PREFIX)/libexec/gridmon

python3_zips += apps
apps_pyproto += remote_write



include binodeps.mk
include pynodeps.mk

all:: python-zips
install:: install-python-zips
install:: install-hidden-scripts

tidy::
	$(FIND) . -name "*~" -delete
