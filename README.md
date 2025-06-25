# Purpose

These are bespoke scripts to augment metrics available for collection by Prometheus for monitoring components of a Grid installation, beyond those metrics provided by the likes of the Prometheus node exporter, Ceph monitors, etc.

## Installation

[Binodeps](https://github.com/simpsonst/binodeps) is required to use the `Makefile`.
You'll also need [Protocol Buffers](https://developers.google.com/protocol-buffers) and [Snappy compression](http://google.github.io/snappy/), and `defusedxml` and `frozendict` Python 3 packages, so try one of these:

```
sudo dnf install protobuf-compiler python3-snappy python3-protobuf python3-frozendict python3-defusedxml python3-kafka
sudo apt-get install protobuf-compiler python3-snappy python3-protobuf python3-frozendict python3-defusedxml python3-kafka
```

(Technically, you'll probably only need the Python packages to run some of the scripts, not to build/install.)

To install to the default location `/usr/local`:

```
make
sudo make install
```

Python/Bash sources and executables are then installed in `/usr/local/share/gridmon/`:

- [`ip-statics-exporter`](static.md) &ndash; Run continuously, this reads a YAML file describing static intent, and writes it in to Prometheus, along with ping times.
- [`xrootd-monitor`](xrootd.md) &ndash; Run continuously, this receives UDP summaries from XRootD's `xrd.report` setting, and detailed monitoring from `xrootd.monitor`, and pushes metrics derived from them to Prometheus.
- [`xrootd-stats`](xrootd-summary.md) (deprecated; use `xrootd-monitor` instead) &ndash; Run continuously, this receives UDP summaries from XRootD's `xrd.report` setting, and serves or pushes them to Prometheus.
- [`xrootd-detail`](xrootd-detail.md) (deprecated; use `xrootd-monitor` instead) &ndash; Run continuously, this receives detailed monitoring over UDP from `xrootd.monitor`, and pushes metrics derived from it to Prometheus.
- [`cephhealth-exporter`](cephhealth.md) &ndash; Run continuously, this scans disc health metrics retained by Ceph, and serves them to Prometheus.
- [`hammercloud-events`](hammercloud.md) &ndash; Run from Procmail, this converts a HammerCloud notification email into a metric point, best used for annotating HammerCloud exclusions.
- [`kafka-exporter`](kafka.md) &ndash; Run continuously, this consumes from one or more Kafka queues, counting key/value bytes, messages and connections, and reporting whether up.
- [`perfsonar-stats`](perfsonar.md) &ndash; Run continuously, this polls a perfSONAR endpoint for measurements, and serves them to Prometheus.
<!-- - `static-metrics` (deprecated; use `ip-statics-exporter` instead) &ndash; Run as a cronjob, this generates a file holding Prometheus metrics describing static intent, and bungs in some ping times just for the sake of high coupling and low cohesion. -->



## Configuration of Prometheus

For each script, either Prometheus scrapes the running process with an HTTP GET, or the process pushes metrics into Prometheus as soon as it has them.
Even if pushing is preferred, the existence of an endpoint for scraping is often retain, as it allows metrics' documentation like `# HELP` to be loaded, and it's an easy way to detect when a collector has failed.

### Scraping

Prometheus YAML configuration will include a `scrape_configs` section, specifying HTTP locations to issue GETs to periodically.
For example:

```
scrape_configs:
  - job_name: 'alerts'
    scrape_interval: 15s
    static_configs:
      - targets:
          - 'localhost:9093'
  - job_name: 'statics'
    scrape_interval: 1m
    static_configs:
      - targets:
          - 'localhost:9363'
```

The first entry causes `http://localhost:9093/metrics` to be fetched every 15 seconds.
These metrics will have a `job` label of `alerts`.
The second entry causes `http://localhost:9363/metrics` to be fetched every minute, with a `job` label of `statics`.


### Remote-write

There doesn't seem to be much authoritative-looking documentation on this, but there might be a [specification](https://docs.google.com/document/d/1LPhVRSFkGNSuU1fBd81ulhsCPR4hkSZyyBj1SZ8fWOM/edit#).
Prometheus needs to run with `--enable-feature=remote-write-receiver` to enable such an endpoint, which is at `/api/v1/write` on port 9090 by default.
For example, a typical endpoint might be `http://localhost:9090/api/v1/write`.


## Domain information

Some applications take a domain configuration, which specifies how specific hostnames and IP addresses should be collapsed into domain names.
These domains are then used to label various metrics, yielding a lower cardinality for the whole metric set than what recording hostnames would.
The configuration is a YAML mapping with with an entry called `domains`.
Its value should be an array of mappings with fields `match` (a Python regular expression) and `value` (a replacement string).
When a hostname is to be reduced to a domain name, it is tested against the `match` expression of each entry.
On the first match, the `value` is yielded, with expressions such as `$1` replaced with the first captured group, etc.

For example:

```
domains:
  - value: $1
    match: ^(?:[^.]+\.)*?([^.]+\.(?:edu\.au|edu\.hk|edu\.tw|gov\.pl|(?:scotgrid\.|rl\.)?ac\.uk|ac\.cn|ac\.il|[a-zA-Z]+))$
  - value: local
    match: ^stor[^.]+$
  - value: local
    match: ^\[::ffff:10\..*\]$
  - value: anonv4
    match: ^\[::ffff:([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)\]$
  - value: anonv6
    match: ^.*$
```

