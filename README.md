# Purpose

These are bespoke scripts to augment metrics available for collection by Prometheus for monitoring components of a Grid installation, beyond those metrics provided by the likes of the Prometheus node exporter, Ceph monitors, etc.

## Installation

[Binodeps](https://github.com/simpsonst/binodeps) is required to use the `Makefile`.
To install to the default location `/usr/local`:

```
make
sudo make install
```

Python/Bash sources and executables are then installed in `/usr/local/share/gridmon/`:

- `static-metrics` &ndash; Run as a cronjob, this generates a file holding Prometheus metrics describing static intent, and bungs in some ping times just for the sake of high coupling and low cohesion.
- `xrootd-stats` &ndash; Run continuously, this receives UDP summaries from XRootD's `xrd.monitor` setting, and serves them to Prometheus.
- `perfsonar-stats` &ndash; Run continuously, this polls a perfSONAR endpoint for measurements, and serves them to Prometheus.

(If you want to by-pass Binodeps, you could probably just copy `src/share/` to `/usr/local/share/gridmon/`.)

## Static metrics

The script `static-metrics` is used to generate Prometheus-compatible metrics expressing essentially static intent.
(It also includes `ping` RTTs, just to muddy the waters.)
The script is to be run as a cronjob to generate a file served statically through a regular HTTP server.

The following options are accepted:

- `-o *file*` &ndash; Atomically write output to the file.  An adjacent file is created, and then moved into place.
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

- `machine_roles` is always 1, and includes the label `roles`, which includes each of the roles specified by the `roles` attribute on input, and separated/surrounded by `\`.
  The intention is to be able to match a role with a regular expression such as `.*/storage/.*`.

It's not yet clear whether to favour one of `machine_role` and `machine_roles`, and then deprecate the other.
One of the problems with `machine_roles` is that the ordering of roles is undefined.
If it should change arbitrarily at some point, the same data could appear as two distinct time series, even though they are meant to be the same one.
For that reason, it's more probably that `machine_roles` will be deprecated.

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
- `-l *lag*` &ndash; the number of seconds of lag; default 20
- `-t *port*` &ndash; port number to bind to (HTTP/TCP); 8732 is the default
- `-T *host*` &ndash; hostname/IP address to bind to (HTTP/TCP); empty string is `INADDR_ANY`; `localhost` is default
- `-E *endpoint*` &ndash; the `esmond` endpoint to fetch metrics from
- `-S *host*` &ndash; the host of the `esmond` endpoint, from which `https://*host*/esmond/perfsonar/archive/` is formed

One of `-E` or `-S` is required.
The specified endpoint is scraped every 30 seconds.
Referenced measurements are then fetched, stored locally as timestamped metrics, and served on demand to scraping clients such as Prometheus.
The additional fetches can take some time, and even over-run.
In that case, the next top-level scrape is delayed until the current additional fetches are complete.
Regardless of when a top-level scrape is performed, its time range always abuts with the previous scrape's range.

To deal with some measurements arriving out of order, or being timestamped by start rather than end, the requested time range is for some seconds into the past.
This 'lag' period is set by `-l`, and defaults to 20s.
So, if a measurement that starts at `t0` and completes at `t1 = t0 + 10` is timestamped `t0`, but cannot be known until `t1`, the upper bound of any requested range will be `r1 < t1 - 20 < t0`, so the measurement will be picked up in the next scrape, and not be missed or regarded out-of-order by prometheus.

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
