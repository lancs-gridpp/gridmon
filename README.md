# Purpose

These are bespoke scripts to augment metrics available for collection by Prometheus for monitoring components of a Grid installation, beyond those metrics provided by the likes of the Prometheus node exporter, Ceph monitors, etc.

## Static metrics

The script `static-metrics` is used to generate Prometheus-compatible metrics expressing essentially static intent.
(It also includes `ping` RTTs, just to muddy the waters.)
The script is to be run as a cronjob to generate a file served statically through a regular HTTP server.

The following options are accepted:

- `-o *file*` &#8211; Atomically write output to the file.  An adjacent file is created, and then moved into place.
- `+o` &#8211; Write to standard output.

Other arguments are taken as source filenames.
Each is read in turn, and then listed hosts are `ping`ed, and their RTTs are recorded.
Finally, the metrics are written out.

### Source format

Each line identifies a host, and defines several attributes to be presented as metrics.
Attributes are space-separated, and follow the host identifier (an IP address or DNS name).
The default order of attributes is:

1. `exported_instance` &#8211; the key field identifying the host in the metric, defaulting to the host identifier
1. `function` &#8211; a deprecated field acting as a shorthand for `roles` (below)
1. `building` &#8211; the building housing the physical device
1. `room` &#8211; the machine room or laboratory housing the physical device
1. `rack` &#8211; the rack identifier housing the physical device
1. `level` &#8211; the position within the rack housing the physical device

Trailing attributes are optional, and most can be named, e.g., `rack=21`, in defiance of the default order.
The following attributes are also optional, and must be named if used:

- `osds` &#8211; the number of Ceph OSDs expected to be 'up' on the host
- `roles` &#8211; a comma-separated list of roles (e.g., `storage`, `ceph_data`, `ceph_monitor`, `ceph_manager`, `ceph_metadata`, `storage_gateway`, etc

`function` may be any of the following, with their `roles` equivalents:

- `storage-data` &#21d2; `storage`, `ceph_data`
- `storage-monitor` &#21d2; `storage`, `ceph_monitor`, `ceph_manager`
- `storage-metadata` &#21d2; `storage`, `ceph_metadata`
- `storage-gateway` &#21d2; `storage`, `storage_gateway`

Anything else is mapped to itself as a role.

### Generated metrics

All metrics include `exported_instance` as an attribute.

`ip_up` is 1 if the host was reachable with `ping`, or 0 otherwise.
`ip_ping` is the RTT in milliseconds.

`ip_osd_drives` is the number of OSDs expected to be up on a host.
It normally corresponds to the number of discs installed.

`ip_metadata` is always 1, but includes other attributes such as `hostname` (the host identifier), along with `building`, `room`, `rack`, `level` if specified.
It also includes an attribute `roles`, which is a slash-separated and slash-surrounded concatenation of the roles for the host, e.g., `/storage/ceph_data/`.
This should be relatively easy to select host sets with certain roles using regular expressions.

## XRootD-Prometheus bridge

The script `xrootd-stats` allows metrics emitted by XRootD to be absorbed by Prometheus.
XRootD pushes metrics over UDP through its [`xrd.report`](https://xrootd.slac.stanford.edu/doc/dev50/xrd_config.htm#_report) configuration, while Prometheus pulls metrics over HTTP exporters.
The script listens on a UDP socket, and stores timestamped metrics in XML [Summary Monitoring Data Format](https://xrootd.slac.stanford.edu/doc/dev51/xrd_monitoring.htm#_Toc49119259) received over UDP.
It also runs an HTTP server, and serves timestamped metrics over it in [OpenMetrics format](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md).
The script distinguishes HTTP clients by `Authorization` header, and remembers the last time each client was successfully issued with metrics, and so only serves metrics with later timestamps.

The following arguments are accepted:

- `-h *int*` &#8211; minutes of horizon, beyond which metrics are discarded; 30 is the default
- `-u *port*` &#8211; port number to bind to (UDP); 9485 is the default
- `-U *host*` &#8211; hostname/IP address to bind to (UDP); empty string is `INADDR_ANY`, and is default
- `-t *port*` &#8211; port number to bind to (HTTP/TCP); 8744 is the default
- `-T *host*` &#8211; hostname/IP address to bind to (HTTP/TCP); empty string is `INADDR_ANY`; `localhost` is default

Each variable specified by the XRootD format is represented by an OpenMetrics metric family by converting dots to underscores, prefixing with `xrootd_`, and suffixing with additional terms as expected by OpenMetrics.

For example, the variable `buff.mem` is presented as the metric `xrootd_buff_mem_bytes`.
XRootD documents this variable with &#2194;, which is taken to mean that the value can both rise and fall; it is therefore given the metric type `gauge`.
In contrast, the variable `buff.reqs` lacks this (or any) mark, so it is treated as a counter, and is represented as two metrics, `xrootd_buff_req_total` (the value of the variable) and `xrootd_buff_req_created` (the Unix timestamp of when it was last reset to zero).

Note that the translation of many metrics has not yet been implemented.
The following have been implemented, and are indexed by the properties `host` and `name` (taken from the `<stats id="info">` element):

- `xrootd_buff_adj_total` &#8211; adjustments to the buffer profile
- `xrootd_buff_buffs` &#8211; number of allocated buffers
- `xrootd_buff_mem_bytes` &#8211; memory allocated to buffers
- `xrootd_buff_reqs_total` &#8211; requests for a buffer
- `xrootd_link_ctime_seconds_total` &#8211; session time in connections
- `xrootd_link_in_bytes_total` &#8211; data received
- `xrootd_link_maxn_total` &#8211; maximum concurrent connections
- `xrootd_link_num` &#8211; concurrent connections
- `xrootd_link_out_bytes_total` &#8211; data sent
- `xrootd_link_sfps_total` &#8211; occurrences of partial sendfile operations
- `xrootd_link_stall_total` &#8211; occurrences of partial data
- `xrootd_link_tmo_total` &#8211; read request timeouts
- `xrootd_link_tot_total` &#8211; connections
- `xrootd_ofs_bxq_total` &#8211; background tasks processed
- `xrootd_ofs_dly_total` &#8211; delays imposed
- `xrootd_ofs_err_total` &#8211; errors encountered
- `xrootd_ofs_han` &#8211; active file handles
- `xrootd_ofs_opp` &#8211; files open in read-write POSC mode
- `xrootd_ofs_opr` &#8211; files open in read mode
- `xrootd_ofs_opw` &#8211; files open in read-write mode
- `xrootd_ofs_rdr_total` &#8211; redirects processed
- `xrootd_ofs_rep_total` &#8211; background replies processed
- `xrootd_ofs_ser_total` &#8211; received events indicating failure
- `xrootd_ofs_sok_total` &#8211; received events indicating success
- `xrootd_ofs_ups_total` &#8211; occurrences of POSC-mode file unpersisted
- `xrootd_ofs_tpc_grnt_total` &#8211; TPCs allowed
- `xrootd_ofs_tpc_deny_total` &#8211; TPCs denied
- `xrootd_ofs_tpc_err_total` &#8211; TPCs that failed
- `xrootd_poll_att` &#8211; file descriptors attached for polling
- `xrootd_poll_en_total` &#8211; poll-enable operations
- `xrootd_poll_ev_total` &#8211; polling events
- `xrootd_poll_int_total` &#8211; unsolicited polling operations
- `xrootd_proc_sys_seconds_total` &#8211; system time
- `xrootd_proc_usr_seconds_total` &#8211; user time
- `xrootd_sched_idle` &#8211; number of scheduler threads waiting for work
- `xrootd_sched_inq` &#8211; number of jobs in run queue
- `xrootd_sched_jobs_total` &#8211; jobs requiring a thread
- `xrootd_sched_maxinq_total` &#8211; longest run-queue length
- `xrootd_sched_tcr_total` &#8211; thread creations
- `xrootd_sched_tde_total` &#8211; thread destructions
- `xrootd_sched_threads_total` &#8211; current scheduler threads
- `xrootd_sched_tlimr_total` &#8211; occurrences of reaching thread limit
- `xrootd_sgen_as` &#8211; asynchronous flag
- `xrootd_sgen_et_seconds` &#8211; time to complete statistics
- `xrootd_sgen_toe_seconds_total` &#8211; Unix time when statistics gathering ended
- `xrootd_xrootd_dly_total` &#8211; requests ending with delay
- `xrootd_xrootd_err_total` &#8211; requests ending with error
- `xrootd_xrootd_num_total` &#8211; requests selecting `xrootd` protocol
- `xrootd_xrootd_rdr_total` &#8211; requests redirected
- `xrootd_xrootd_aio_max_total` &#8211; maximum concurrent asynchronous requests
- `xrootd_xrootd_aio_num_total` &#8211; asynchronous requests processed
- `xrootd_xrootd_aio_rej_total` &#8211; asynchronous requests converted to synchronous
- `xrootd_xrootd_lgn_af_total` &#8211; authentication failkures
- `xrootd_xrootd_lgn_au_total` &#8211; successful authenticated logins
- `xrootd_xrootd_lgn_num_total` &#8211; login attempts
- `xrootd_xrootd_lgn_ua_total` &#8211; successful unauthenticated logins
- `xrootd_xrootd_ops_getf_total` &#8211; `getfile` requests
- `xrootd_xrootd_ops_misc_total` &#8211; 'other' requests
- `xrootd_xrootd_ops_open_total` &#8211; file-open requests
- `xrootd_xrootd_ops_pr_total` &#8211; pre-read requests
- `xrootd_xrootd_ops_putf_total` &#8211; `putfile` requests
- `xrootd_xrootd_ops_rd_total` &#8211; read requests
- `xrootd_xrootd_ops_rf_total` &#8211; cache-refresh requests
- `xrootd_xrootd_ops_rs_total` &#8211; readv segments
- `xrootd_xrootd_ops_rv_total` &#8211; readv requests
- `xrootd_xrootd_ops_sync_total` &#8211; sync requests
- `xrootd_xrootd_ops_wr_total` &#8211; write requests


The following metrics offer metadata:

- `xrootd_ofs_meta_info` &#8211; holds reporter's role as property `role`

In addition to `host` and `name`, the following are defined to have `lp` and `rp` properties:

- `xrootd_oss_paths_free_bytes` &#8211; free space
- `xrootd_oss_paths_tot_bytes` &#8211; capacity
- `xrootd_oss_paths_ifr_inodes` &#8211; free inodes (TODO: This comes up as -1, probably meaning the information isn't available.  It should therefore not exist as a metric.)
- `xrootd_oss_paths_ino_inodes` &#8211; total inodes

(It's not clear which of `lp` and `rp` should be considered 'key fields', so both are included for now.  A future version might drop one, and provide it as metadata.)

## PerfSONAR statistics

The script `perfsonar-stats` is supposed to pull data from the `esmond` service of a PerfSONAR instance, and turn it into Prometheus-compatible metrics.
However, it still needs a lot of work, as the data it currently fetches is not meaningful.
For example, doubling the scraping rate scales up the values of the metrics, which shouldn't happen if the metrics are genuine; changes to the rate should only affect the granularity of the metrics, not their values.
The script also needs work to accept configuration options.
