# Purpose

These are bespoke scripts to augment metrics available for collection by Prometheus for monitoring components of a Grid installation, beyond those metrics provided by the likes of the Prometheus node exporter, Ceph monitors, etc.

## Installation

[Binodeps](https://github.com/simpsonst/binodeps) is required to use the `Makefile`.
You'll also need (Protocol Buffers)[https://developers.google.com/protocol-buffers] and [Snappy compression](http://google.github.io/snappy/), and `defusedxml` and `frozendict` Python 3 packages, so try one of these:

```
sudo dnf install protobuf-compiler python3-snappy python3-protobuf python3-frozendict python3-defusedxml
sudo apt-get install protobuf-compiler python3-snappy python3-protobuf python3-frozendict python3-defusedxml
```

(Technically, you'll probably only need the Python packages to run some of the scripts, not to build/install.)

To install to the default location `/usr/local`:

```
make
sudo make install
```

Python/Bash sources and executables are then installed in `/usr/local/share/gridmon/`:

- `static-metrics` &ndash; Run as a cronjob, this generates a file holding Prometheus metrics describing static intent, and bungs in some ping times just for the sake of high coupling and low cohesion.
- `xrootd-stats` &ndash; Run continuously, this receives UDP summaries from XRootD's `xrd.monitor` setting, and serves or pushes them to Prometheus.
- `perfsonar-stats` &ndash; Run continuously, this polls a perfSONAR endpoint for measurements, and serves them to Prometheus.
  This is a bit flakey at the moment, and suspected of driving Prometheus nuts, so use with caution.
- `cephhealth-exporter` &ndash; Run continuously, this scans disc health metrics retained by Ceph, and serves them to Prometheus.


## Configuration of Prometheus

For each script, either Prometheus scrapes the running process with an HTTP GET, or the process pushes metrics into Prometheus as soon as it has them.

### Scraping

Prometheus YAML configuration will include a `scrape_configs` section, specifying HTTP locations to issue GETs to periodically.
For example:

```
scrape_configs:
  - job_name: 'alerts'
    static_configs:
      - targets:
          - 'localhost:9093'
  - job_name: 'statics'
    scrape_interval: 15s
    metrics_path: '/ip.metrics'
    static_configs:
      - targets:
          - 'localhost:80'
```

The second entry specifies that `http://localhost:80/ip.metrics` is to be fetched every 15 seconds.
`metrics_path` is `/metrics` by default, so the first entry fetches `http://localhost:9093/metrics` at whatever the default interval is.


### Remote-write

There doesn't seem to be much authoritative-looking documentation on this, but there might be a [specification](https://docs.google.com/document/d/1LPhVRSFkGNSuU1fBd81ulhsCPR4hkSZyyBj1SZ8fWOM/edit#).
Prometheus needs to run with `--enable-feature=remote-write-receiver` to enable such an endpoint, which is at `/api/v1/write` on port 9090 by default.
For example, a typical endpoint might be `http://localhost:9090/api/v1/write`.


## Static metrics

The script `static-metrics` is used to generate Prometheus-compatible metrics expressing essentially static intent.
(It also includes `ping` RTTs, just to muddy the waters.)
The script is to be run as a cronjob to generate a file served statically through a regular HTTP server.

The following options are accepted:

- `-o *file*` &ndash; Atomically write output to the file.
  An adjacent file is created, and then moved into place.
- `+o` &ndash; Write to standard output.

Other arguments are taken as source filenames.
Each is read in turn, and then listed hosts are `ping`ed, and their RTTs are recorded.
Finally, the metrics are written out.

### Source format

Each line identifies a host, and defines several attributes to be presented as metrics.
Each attribute has the form `name=value`, and attributes are separated by spaces.
The following are recognized:

- `addrs` specifies a comma-separated list of interfaces by their hostnames or IP addresses.
  Each element may optionally identify the device by suffixing `#device`.
  Each element may optionally identify the connected network by suffixing `/network`.

- `building` specifies the building housing the physical device.

- `room` specifies the machine room or laboratory housing the physical device.

- `rack` specifies the rack identifier housing the physical device.

- `level` specifies the position within the rack housing the physical device.

- `osds` specifies the number of Ceph OSDs expected to be 'up' on the host.

- `roles` specifies a comma-separated list of roles that the host fulfils (e.g., `storage`, `ceph_data`, `ceph_monitor`, `ceph_manager`, `ceph_metadata`, `storage_gateway`, etc.

- `func` is a deprecated field acting as a shorthand for `roles`:

  - `storage-data` &rArr; `storage`, `ceph_data`
  - `storage-monitor` &rArr; `storage`, `ceph_monitor`, `ceph_manager`
  - `storage-metadata` &rArr; `storage`, `ceph_metadata`
  - `storage-gateway` &rArr; `storage`, `storage_gateway`

  Anything else is mapped to itself as a role.

### Generated metrics

Three groups of metrics are generated, with the following prefixes:

- `machine_` metrics with the same `node` label describe a physical or virtual machine.

- `ip_` metrics with the same `iface` label describe an IP interface of a machine (which may have several such interfaces).

- `xrootd_` metrics with the same `xrdid` label describe an XRootD instance.

#### Machine metrics

These metrics include `node` as a key label, identifying the host:

- `machine_osd_drives` specifies the number of block storage devices on the host that should be under Ceph management as OSDs.
  An `exported_instance` label is included, but is deprecated in favour of `node`.

- `machine_location` is always 1, and includes the optional labels `building`, `room`, `rack`, `level`, as specified by the attributes provided as input.
  An `exported_instance` label is included, but is deprecated in favour of `node`.

- `machine_role` is always 1, and includes the label `role`, with one metric point for each role specified by the `roles` attribute on input.
  An `exported_instance` label is included, but is deprecated in favour of `node`.

- `machine_roles` is always 1, and includes the label `roles`, which includes each of the roles specified by the `roles` attribute on input, and separated/surrounded by `/`.
  The intention is to be able to match a role with a regular expression such as `.*/storage/.*`.

It's not yet clear whether to favour one of `machine_role` and `machine_roles`, and then deprecate the other.
One of the problems with `machine_roles` is that the ordering of roles is undefined.
If it should change arbitrarily at some point, the same data could appear as two distinct time series, even though they are meant to be the same one.
For that reason, it's more probable that `machine_roles` will be deprecated.

#### IP interface metrics

Most of these metrics include `iface` as a key label, which is an IP address or a resolvable host name:

- `ip_up` is 1 if the host was reachable with `ping`, or 0 otherwise.
  An `exported_instance` label is included, but is deprecated in favour of `node` on `ip_metadata`.

- `ip_ping` is the last RTT to the interface in milliseconds.
  An `exported_instance` label is included, but is deprecated in favour of `node` on `ip_metadata`.

- `ip_metadata` is always 1, and includes the following additional labels:

  - `node` &ndash; the machine to which the interface belongs
  
  - `device` (optional) &ndash; the name of the interface within the host, e.g., `eth0`
  
  - `network` (optional) &ndash; the name of the network that the interface connects to
  
  An `exported_instance` label is included, but is deprecated in favour of `node` on `ip_metadata`.
  The `hostname` label is deprecated in favour of `iface`.
  The labels `building`, `room`, `rack` and `level` are deprecated in favour of the metric `machine_location`.
  `func`, `role_*` and `roles` are deprecated in favour of metrics `machine_role(s)`.
  
- `ip_osd_drives` is deprecated in favour of `machine_osd_drives`.

- `ip_heartbeat` is a counter in seconds giving the time when the script was invoked.
  It can be used to detect whether the script is properly running and updating its output metrics.
  Exceptionally, it has no `iface` label.

#### XRootD metrics

These metrics include `xrdid` as a key label of the form `instance@hostname`, as an XRootD instance identifies itself.
Only one metric is actually defined:

- `xrootd_expect` always has the value 1, and the following additional labels:

  - `host` (deprecated) &ndash; the hostname that the XRootD instance deduced by scanning local interfaces

  - `name` (deprecated) &ndash; the name of the XRootD instance within its host
  
  - `node` &ndash; the name of the machine on which the XRootD instance runs

Note that the deprecated fields `host` and `name` are incorporated into the `xrdid` field, and can be otherwise derived by combining with the `xrootd_meta` metric provided by the XRootD-Prometheus bridge.

## Ceph disc health metrics exporter

Ceph can be made to collect SMART metrics to ascertain the health of its discs.
`cephhealth-exporter` pulls these metrics at a scheduled time, and makes them available to Prometheus.
At the scheduled time, it performs this command:

```
ceph device ls --format=json
```

Using the output, it forms a list of devices indexed by device id with the format `MAKE_MODEL_SERIAL`, and invokes the following on each one:

```
ceph device get-health-metrics --format=json DEVID
```

From the last entry of the result, it forms the following metrics, each with a `devid` label:

- `cephhealth_scsi_grown_defect_list_total` &ndash; the `scsi_grown_defect_list` reading; a counter, also with a `_created` time, which is always 0
- `cephhealth_scsi_uncorrected_total` &ndash; the `scsi_error_counter_log` reading; a counter, also with a `_created` time, which is always 0; a `mode` label distingishes between `read`, `write` and `verify`
- `cephhealth_metadata` &ndash; always 1; includes the label `path` giving the value of `location` with the `/dev/disk/by-path/` prefix lopped off

These metrics are cached based on the timestamp that the `ceph` command supplies with the data.
They can then be scraped by Prometheus in [OpenMetrics format](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md).
Because they are normally obtained once per day, you will probably need to use `last_over_time(cephhealth_metadata[25h])` to ensure they are detected.

The `devid` label can be correlated with the `device_ids` label of `ceph_disk_occupation` metrics supplied by Ceph itself.
However, `device_ids` must be processed first to get a match.
For example, it might contain `nvme0n1=Foo_Zippy1.5_987987532,sda=Bar_Whooshy2.0_927343`, while `devid` only contains `Bar_Whooshy2.0_927343`.
You need to use `label_replace` on it in PromQL to create a suitable label.

For example, to get a list of discs with at least one defect, include which OSD they serve, which host they are on, their `/dev/` names and device paths:

```
(last_over_time(cephhealth_scsi_grown_defect_list_total[25h]) > 0) * on(devid) group_right() avg without (device_ids, devices) (label_replace(label_replace(avg without (exported_instance, instance, job) (ceph_disk_occupation), "devid", '$2', "device_ids", `([^=]+=[^,]+,)?[^=]+=(.*)`), "disk", '$2', "devices", `([^,]+,)?(.*)`)) * on(devid) group_left(path) last_over_time(cephhealth_metadata[25h])
```

The following arguments are accepted:

- `-l *int*` &ndash; the number of seconds of lag; default 20
- `-s *HH:MM*` &ndash; Add the time of day to the daily schedule.
  Ceph is scanned at each scheduled time.
- `-h *int*` &ndash; days of horizon, beyond which metrics are discarded; 3 is the default
- `-t *port*` &ndash; port number to bind to (HTTP/TCP); 8799 is the default
- `-T *host*` &ndash; hostname/IP address to bind to (HTTP/TCP); empty string is `INADDR_ANY`; `localhost` is default
- `-z` &ndash; Open `/dev/null` and duplicate it to `stdout` and `stderr`.
  Use this in a cronjob to obviate starting a separate shell to perform redirection.
- `--log=*level*` &ndash; Set the log level.
  `info` is good.
- `--log-file=*file*` &ndash; Append logging to a file.
- `--disk-limit=*num*` &ndash; Stop after getting non-empty data from this many discs.
  This is intended mainly for debugging on a small scale, without having to wait six minutes to scrape 700 discs!

Any remaining arguments are prefixed to the executed commands.
This allows the script to run on a different host to Ceph, and SSH into it, for example.
Use `--` if any of the arguments could be mistaken for switches to this script.


### Future directions

A future version will instead obtain its own current metrics with commands such as:

```
ceph device query-daemon-health-metrics osd.312
```

This yields largely the same data as `ceph device get-health-metrics`, except it also gives details of the database device.
The advantage is that the data returned is current, and could be immediately pushed to Prometheus via its remote-write interface.
In contrast, `get-health-metrics` returns historical data, which might be too old by the time the exporter has obtained it, and Prometheus has asked for it.
(We have observed that discs for the first or last 30-or-so OSDs sometimes do not get exported, because the scan of Ceph or Prometheus's scrape of the script started too late.)

## XRootD-Prometheus bridge

The script `xrootd-stats` allows metrics emitted by XRootD to be absorbed by Prometheus.
XRootD pushes metrics over UDP through its [`xrd.report`](https://xrootd.slac.stanford.edu/doc/dev50/xrd_config.htm#_report) configuration, while Prometheus pulls metrics over HTTP exporters.
The script listens on a UDP socket, and stores timestamped metrics in XML [Summary Monitoring Data Format](https://xrootd.slac.stanford.edu/doc/dev51/xrd_monitoring.htm#_Toc49119259) received over UDP.
It also runs an HTTP server, and serves timestamped metrics over it in [OpenMetrics format](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md).
The script distinguishes HTTP clients by `Authorization` header, and remembers the last time each client was successfully issued with metrics, and so only serves metrics with later timestamps.

The following arguments are accepted:

- `-h *int*` &ndash; minutes of horizon, beyond which metrics are discarded; 30 is the default
- `-u *port*` &ndash; port number to bind to (UDP); 9485 is the default
- `-U *host*` &ndash; hostname/IP address to bind to (UDP); empty string is `INADDR_ANY`, and is default
- `-t *port*` &ndash; port number to bind to (HTTP/TCP); 8744 is the default
- `-T *host*` &ndash; hostname/IP address to bind to (HTTP/TCP); empty string is `INADDR_ANY`; `localhost` is default
- `-z` &ndash; Open `/dev/null` and duplicate it to `stdout` and `stderr`.
  Use this in a cronjob to obviate starting a separate shell to perform redirection.
- `--log=*level*` &ndash; Set the log level.
  `info` is good.
- `--log-file=*file*` &ndash; Append logging to a file.
- `-E *endpoint*` &ndash; Push metrics to a remote-write endpoint.

If you use `-E`, metrics are pushed to the endpoint as soon as a UDP report arrives and its contents are converted.
In this case, the HTTP server serves no metrics, but still yields metric documentation in the form of `# HELP`, `# TYPE` and `# UNIT`, which you might choose to scrape infrequently.

### XRootD metrics

Each variable specified by the XRootD format is represented by an OpenMetrics metric family by converting dots to underscores, prefixing with `xrootd_`, and suffixing with additional terms as expected by OpenMetrics.

For example, the variable `buff.mem` is presented as the metric `xrootd_buff_mem_bytes`.
XRootD documents this variable with &#2194;, which is taken to mean that the value can both rise and fall; it is therefore given the metric type `gauge`.
In contrast, the variable `buff.reqs` lacks this (or any) mark, so it is treated as a counter, and is represented as two metrics, `xrootd_buff_req_total` (the value of the variable) and `xrootd_buff_req_created` (the Unix timestamp of when it was last reset to zero).

Note that the translation of many metrics has not yet been implemented.
The following have been implemented, and are indexed by the properties `host` and `name` (taken from the `<stats id="info">` element):

- `xrootd_buff_adj_total` &ndash; adjustments to the buffer profile
- `xrootd_buff_buffs` &ndash; number of allocated buffers
- `xrootd_buff_mem_bytes` &ndash; memory allocated to buffers
- `xrootd_buff_reqs_total` &ndash; requests for a buffer
- `xrootd_link_ctime_seconds_total` &ndash; session time in connections
- `xrootd_link_in_bytes_total` &ndash; data received
- `xrootd_link_maxn_total` &ndash; maximum concurrent connections
- `xrootd_link_num` &ndash; concurrent connections
- `xrootd_link_out_bytes_total` &ndash; data sent
- `xrootd_link_sfps_total` &ndash; occurrences of partial sendfile operations
- `xrootd_link_stall_total` &ndash; occurrences of partial data
- `xrootd_link_tmo_total` &ndash; read request timeouts
- `xrootd_link_tot_total` &ndash; connections
- `xrootd_ofs_bxq_total` &ndash; background tasks processed
- `xrootd_ofs_dly_total` &ndash; delays imposed
- `xrootd_ofs_err_total` &ndash; errors encountered
- `xrootd_ofs_han` &ndash; active file handles
- `xrootd_ofs_opp` &ndash; files open in read-write POSC mode
- `xrootd_ofs_opr` &ndash; files open in read mode
- `xrootd_ofs_opw` &ndash; files open in read-write mode
- `xrootd_ofs_rdr_total` &ndash; redirects processed
- `xrootd_ofs_rep_total` &ndash; background replies processed
- `xrootd_ofs_ser_total` &ndash; received events indicating failure
- `xrootd_ofs_sok_total` &ndash; received events indicating success
- `xrootd_ofs_ups_total` &ndash; occurrences of POSC-mode file unpersisted
- `xrootd_ofs_tpc_grnt_total` &ndash; TPCs allowed
- `xrootd_ofs_tpc_deny_total` &ndash; TPCs denied
- `xrootd_ofs_tpc_err_total` &ndash; TPCs that failed
- `xrootd_poll_att` &ndash; file descriptors attached for polling
- `xrootd_poll_en_total` &ndash; poll-enable operations
- `xrootd_poll_ev_total` &ndash; polling events
- `xrootd_poll_int_total` &ndash; unsolicited polling operations
- `xrootd_proc_sys_seconds_total` &ndash; system time
- `xrootd_proc_usr_seconds_total` &ndash; user time
- `xrootd_sched_idle` &ndash; number of scheduler threads waiting for work
- `xrootd_sched_inq` &ndash; number of jobs in run queue
- `xrootd_sched_jobs_total` &ndash; jobs requiring a thread
- `xrootd_sched_maxinq_total` &ndash; longest run-queue length
- `xrootd_sched_tcr_total` &ndash; thread creations
- `xrootd_sched_tde_total` &ndash; thread destructions
- `xrootd_sched_threads_total` &ndash; current scheduler threads
- `xrootd_sched_tlimr_total` &ndash; occurrences of reaching thread limit
- `xrootd_sgen_as` &ndash; asynchronous flag
- `xrootd_sgen_et_seconds` &ndash; time to complete statistics
- `xrootd_sgen_toe_seconds_total` &ndash; Unix time when statistics gathering ended
- `xrootd_xrootd_dly_total` &ndash; requests ending with delay
- `xrootd_xrootd_err_total` &ndash; requests ending with error
- `xrootd_xrootd_num_total` &ndash; requests selecting `xrootd` protocol
- `xrootd_xrootd_rdr_total` &ndash; requests redirected
- `xrootd_xrootd_aio_max_total` &ndash; maximum concurrent asynchronous requests
- `xrootd_xrootd_aio_num_total` &ndash; asynchronous requests processed
- `xrootd_xrootd_aio_rej_total` &ndash; asynchronous requests converted to synchronous
- `xrootd_xrootd_lgn_af_total` &ndash; authentication failkures
- `xrootd_xrootd_lgn_au_total` &ndash; successful authenticated logins
- `xrootd_xrootd_lgn_num_total` &ndash; login attempts
- `xrootd_xrootd_lgn_ua_total` &ndash; successful unauthenticated logins
- `xrootd_xrootd_ops_getf_total` &ndash; `getfile` requests
- `xrootd_xrootd_ops_misc_total` &ndash; 'other' requests
- `xrootd_xrootd_ops_open_total` &ndash; file-open requests
- `xrootd_xrootd_ops_pr_total` &ndash; pre-read requests
- `xrootd_xrootd_ops_putf_total` &ndash; `putfile` requests
- `xrootd_xrootd_ops_rd_total` &ndash; read requests
- `xrootd_xrootd_ops_rf_total` &ndash; cache-refresh requests
- `xrootd_xrootd_ops_rs_total` &ndash; readv segments
- `xrootd_xrootd_ops_rv_total` &ndash; readv requests
- `xrootd_xrootd_ops_sync_total` &ndash; sync requests
- `xrootd_xrootd_ops_wr_total` &ndash; write requests


The following metrics offer metadata:

- `xrootd_ofs_meta_info` &ndash; holds reporter's role as property `role`

In addition to `host` and `name`, the following are defined to have `lp` and `rp` properties:

- `xrootd_oss_paths_free_bytes` &ndash; free space
- `xrootd_oss_paths_tot_bytes` &ndash; capacity
- `xrootd_oss_paths_ifr_inodes` &ndash; free inodes (TODO: This comes up as -1, probably meaning the information isn't available.  It should therefore not exist as a metric.)
- `xrootd_oss_paths_ino_inodes` &ndash; total inodes

(It's not clear which of `lp` and `rp` should be considered 'key fields', so both are included for now.  A future version might drop one, and provide it as metadata.)

## PerfSONAR statistics

The script `perfsonar-stats` pulls data from the `esmond` service of a PerfSONAR instance, and turns it into Prometheus-compatible metrics.

The following arguments are accepted:

- `-h *int*` &ndash; minutes of horizon, beyond which metrics are discarded; 30 is the default
- `-l *int*` &ndash; the number of seconds of lag; default 20
- `-f *int*` &ndash; seconds before the scraped period to look for metadata keys; default 0
- `-a *int*` &ndash; seconds before the scraped period to look for metadata keys; default 60
- `-z` &ndash; Open `/dev/null` and duplicate it to `stdout` and `stderr`.
  Use this in a cronjob to obviate starting a separate shell to perform redirection.
- `-t *port*` &ndash; port number to bind to (HTTP/TCP); 8732 is the default
- `-T *host*` &ndash; hostname/IP address to bind to (HTTP/TCP); empty string is `INADDR_ANY`; `localhost` is default
- `-E *endpoint*` &ndash; the `esmond` endpoint to fetch metrics from
- `-S *host*` &ndash; the host of the `esmond` endpoint, from which `https://*host*/esmond/perfsonar/archive/` is formed
- `--log=level` &ndash; Set the log level.
- `--log-file=file` &ndash; Set the log file; default is probably to `stderr`.

One of `-E` or `-S` is required.
The specified endpoint is consulted periodically to obtain timestamped metric points, which can then be scraped by Prometheus.
A consultation considers two intervals, *scan* and *scrape*, each of which is passed as `time-start` and `time-end` parameters specified in the call to the endpoint (the scan interval) and its derivatives (the scrape interval).
Consultations are approximately 30s apart.

For a consultation at time `T`, the scan interval ends at `T-20` (as set by `-l`).
The scrape interval ends no later than `T-20-60` (as set by `-a`).
It begins wherever the previous scrape interval ended, so if that was 30s earlier, that's at `T-20-60-30`.
The scan interval begins no later than `T-20-60-30-0` (as set by `-f`).

The extra 20s is the lag, and is intended to deal with measurements recorded up to 20s later than they are stamped.
This might happen if (say) a measurement takes several seconds to make, but is given a timestamp based on when it started, rather than when it ended.

The scrape interval is used when obtaining actual measurements.
Because it always abuts with previous and next scrapes, no measurement is ever read more than once, and none should be missed.

Several calls to derivatives of the configured endpoint are required to get all measurements of interest within the scrape interval, and these derivatives vary by metadata key (identifying the measurement task) and event type (the metric, e.g., `throughput`).
The derivates are obtained through a single call to the configured endpoint, using the *scan* interval, which is wider than the scrape interval.
This is because the timestamps from the scan lag behind the measurements by a few to tens of seconds, and might not fall within the same scrape interval.

The following metrics are defined:

- `perfsonar_packets_lost` &ndash; the number of packets lost
- `perfsonar_packets_sent` &ndash; the number of packets sent
- `perfsonar_events_packets_total` &ndash; the number of packet-loss measurements
- `perfsonar_throughput` &ndash; a throughput measurement
- `perfsonar_events_throughput_total` &ndash; the number of throughput measurements
- `perfsonar_owdelay` &ndash; a one-way delay measurement
- `perfsonar_events_owdelay_total` &ndash; the number of one-way-delay measurements
- `perfsonar_ttl` &ndash; a TTL measurement
- `perfsonar_events_ttl_total` &ndash; the number of TTL measurements
- `perfsonar_metadata` &ndash; metadata for an ongoing measurement something something
- `perfsonar_ip_metadata` &ndash; additional IP metadata for an ongoing measurement something something

All measurements have a label `metadata_key` which can be cross-referenced with the metadata.
The metadata itself provides source and destination addresses (`src_addr` and `dst_addr`), and corresponding names as submitted (`src_name` and `dst_name`).
`agent_addr` and `agent_name` are copies either of `src_addr` and `src_name` if the source matches the measurement agent, or of `dst_addr` and `dst_name` otherwise.
In either case, the other pair of fields are copied to `peer_addr` and `peer_name`.
The `tool` label is a copy of the `tool-name` field, `subj_type` is a copy of `subject-type`, and `psched_type` is a copy of `pscheduler-test-type`.
The IP metadata additionally provides `ip_transport_protocol`.
