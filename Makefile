all::

## Configurable defaults
PREFIX=/usr/local

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
datafiles += metrics.py
datafiles += perfsonar.py
datafiles += xrootd.py

BINODEPS_SHAREDIR=src/share
BINODEPS_SCRIPTDIR=$(BINODEPS_SCRIPTDIR)
SHAREDIR ?= $(PREFIX)/share/gridmon
LIBEXECDIR ?= $(PREFIX)/libexec/gridmon
include binodeps.mk

install:: install-hidden-scripts install-data
