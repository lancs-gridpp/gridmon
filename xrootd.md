# XRootD-Prometheus bridge

XRootD pushes metrics over UDP through its [`xrd.report`](https://xrootd.slac.stanford.edu/doc/dev50/xrd_config.htm#_report) and [`xrootd.monitor`](https://xrootd.slac.stanford.edu/doc/dev50/xrd_config.htm#_monitor) directives, while Prometheus normally pulls metrics over HTTP exporters.
The script `xrootd-monitor` allows detailed and summary metrics emitted by XRootD to be absorbed by Prometheus.
The script listens on a UDP socket, and distinguishes the two types of messages (XML for summary reports; a binary format for detailed monitoring), derives metrics from them, and remote-writes them to a Promethes endpoint.
Metrics extracted from summary messages are written immediately, while detailed messages are collated and turned into events.
At a given interval, statistics derived from these events are written.
The events are also logged (separately from the script's own log), and can be inspected manually.

`xrootd-monitor` combines and replaces `xrootd-stats` (for summary reports) and `xrootd-detail` (for detailed monitoring), which are deprecated.

## Run-time dependencies

`xrootd-monitor` requires `frozendict`, [Protocol Buffers](https://developers.google.com/protocol-buffers) and [Snappy compression](http://google.github.io/snappy/) for pushing to Prometheus, and `defusedxml` for parsing XRootD summary reports, so try one of these:

```
sudo dnf install python3-snappy python3-protobuf python3-frozendict python3-defusedxml
```

```
sudo apt-get install python3-snappy python3-protobuf python3-frozendict python3-defusedxml
```


## Configuration

`xrootd-monitor` treats non-switch arguments as names of YAML files, which are loaded and merged, overriding some defaults.
Switches then override files' contents.
The following configuration is recognized:

```yaml
source:
  xrootd:
    host: ""
    port: 9484
  pcap:
    filename: null
    limit: null
destination:
  push:
    endpoint: null
    summary_job: xrootd
    detail_job: xrootd_detail
  scrape:
    host: localhost
    port: 8743
  log: "/tmp/xrootd-detail.log"
data:
  horizon: "5m"
  fake_port: null
  dictids:
    timeout: "2h"
  sequencing:
    window: 240
    timeout: "750ms"
  domains:
    filename: null
process:
  silent: false
  id_filename: null
  log:
    filename: null
    format: "%(asctime)s %(levelname)s %(message)s"
    datefmt: "%Y-%m-%dT%H:%M:%SZ"
    level: null
```

### Source configuration

`source.xrootd` specifies the UDP host and port to bind to for receiving XRootD monitoring messages.
By default, all interfaces are bound to, using port 9484.
`-U` overrides the hostname, and `-u` overrides the port.

If `source.pcap.filename: file` is specified (also set with `-P file` or `--pcap=file`), no UDP socket is created.
Instead, the file is treated as a PCAP recording, and read using:

```
tshark -r file -t u -Tfields -e frame.time.epoch -e ip.src -e udp.srcport -e data
```

The output is parsed as tab-separated data, and processed as if it were live data.
`source.pcap.limit: 10` or `--pcap-limit=10` can be set to limit processing to the first (say) 10 packets.
It is recommended that recordings are made using defragmenation (with `-o ip.defragment:TRUE` on `tshark`).

### Destination configuration

`destination.push` specifies how to write metrics to Prometheus.

- `endpoint` should be the remote-write endpoint URL, e.g., `http://localhost:9090/api/v1/write`.
  If not specified, the message that would be sent is printed on `stdout`.
- `summary_job` specifies the value of the `job` label added to metrics derived from summary reports.
- `detail_job` specifies the value of the `job` label added to metrics derived from detailed messages.

`log` specifies a file to write a log of events derived from detailed messages.
`-o` sets it from the command line.

`destination.scrape` specifies a host and port which Prometheus can scrape.
Although this provides no actual metrics, it can provide documentation for pushed metrics, and serve to detect when the script is not running.
`-T` and `-t` set the host and port from the command line.

### Data configuration

`data.horizon` specifies young data must be to be accepted.
Not that higher values cause delays in pushing detailed metrics, as anything within the time period until now is deferred until it is certain that no further out-of-order information will arrive.

`data.dictids.timeout` specifies a timeout for discarding old dictids.

`data.sequencing` specifies how to re-order detailed messages.

- `timeout` specifies how long to wait for an expected sequence number.
- `window` is an integer less than 257, and indicates how far ahead of the earliest awaited sequence number another sequence number is to be considered ahead of it, rather than behind.

`data.domains.filename` specifies a [host-to-domain mapping](README.md#domain-information).

### Process configuration

`process.id_filename` specifies a file to write the process's PID to.
It must end with `.pid`.
It will be deleted when the process dies.

`process.silent` can be set to cause `/dev/null` to be opened and duplicated to `stdout` and `stderr`.
Use this in a cronjob to obviate starting a separate shell to perform redirection.
`-z` also sets this flag.

`process.log.filename` specifies a filename for general logging of the script's operation.
`--log-file=file` also sets it.

`process.log.level` can be set to a Python logging level (e.g., `info`, `debug`, etc).
`--log=level` also sets it.

## Summary metrics

Each variable specified by the XRootD format is represented by an OpenMetrics metric family by converting dots to underscores, prefixing with `xrootd_`, and suffixing with additional terms as expected by OpenMetrics.

For example, the variable `buff.mem` is presented as the metric `xrootd_buff_mem_bytes`.
XRootD documents this variable with &#2194;, which is taken to mean that the value can both rise and fall; it is therefore given the metric type `gauge`.
In contrast, the variable `buff.reqs` lacks this (or any) mark, so it is treated as a counter, and is represented as two metrics, `xrootd_buff_req_total` (the value of the variable) and `xrootd_buff_req_created` (the Unix timestamp of when it was last reset to zero).

Note that the translation of many metrics has not yet been implemented.
The following have been implemented, and are indexed by the label `xrdid` (formed as `name@host` from the `name` and `host` elements of `<stats id="info">`):

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
- `xrootd_cms_meta` &ndash; CMS metadata
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

In addition to `xrdid`, the following labels are defined to have `lp` and `rp` properties:

- `xrootd_oss_paths_free_bytes` &ndash; free space
- `xrootd_oss_paths_tot_bytes` &ndash; capacity
- `xrootd_oss_paths_ifr_inodes` &ndash; free inodes (TODO: This comes up as -1, probably meaning the information isn't available.
  It should therefore not exist as a metric.)
- `xrootd_oss_paths_ino_inodes` &ndash; total inodes

(It's not clear which of `lp` and `rp` should be considered 'key fields', so both are included for now.
A future version might drop one, and provide it as metadata.)

All pushed summary metrics include the label `job="xrootd"`.
This can be overridden in configuration through the field `destination.push.summary_job`.



## Detailed metrics

The following metrics are defined:

- `xrootd_dictid_skip_total` and `xrootd_dictid_skip_created` &ndash; The process expects dictids to be defined sequentially.
  If they are out of order, the sizes of the gaps are accumulated in these counters.
  Labels are `pgm` and `xrdid`, as reported by the `=` messages.
- `xrootd_dictid_unknown_total` and `xrootd_dictid_unknown_created` &ndash; When a dictid is looked up, but is not found, this counter is incremented.
  Labels are `pgm`, `xrdid`, `record` and `field`.
- `xrootd_data_opens_total` and `xrootd_data_opens_created` &ndash; When an [Open event](https://xrootd.slac.stanford.edu/doc/dev51/xrd_monitoring.htm#_Toc49119288) is received in an `f`-stream, this counter is incremented.
  Labels are `pgm`, `xrdid`, `protocol`, `client_domain`, `ip_version` and `auth`.
- `xrootd_data_opens_rw_total` and `xrootd_data_opens_rw_created` &ndash; When an [Open event](https://xrootd.slac.stanford.edu/doc/dev51/xrd_monitoring.htm#_Toc49119288) for read/write is received in an `f`-stream, this counter is incremented.
  Note that this counter is incorporated into `xrootd_data_opens_total`.
  Labels are `pgm`, `xrdid`, `protocol`, `client_domain`, `ip_version` and `auth`.
- `xrootd_data_closes_total` and `xrootd_data_closes_created` &ndash; When a [Close event](https://xrootd.slac.stanford.edu/doc/dev51/xrd_monitoring.htm#_Toc49119289) is received in an `f`-stream, this counter is incremented.
  Labels are `pgm`, `xrdid`, `protocol` and `client_domain`.
- `xrootd_data_closes_forced_total` and `xrootd_data_closes_forced_created` &ndash; When a [Close event](https://xrootd.slac.stanford.edu/doc/dev51/xrd_monitoring.htm#_Toc49119289) is received in an `f`-stream, and it is forced, this counter is incremented.
  Labels are `pgm`, `xrdid`, `protocol` and `client_domain`.
- `xrootd_data_read_bytes_total` and `xrootd_data_read_bytes_created`; `xrootd_data_readv_bytes_total` and `xrootd_data_readv_bytes_created`; `xrootd_data_write_bytes_total` and `xrootd_data_write_bytes_created` &ndash; When a [Close event](https://xrootd.slac.stanford.edu/doc/dev51/xrd_monitoring.htm#_Toc49119289) is received in an `f`-stream, this counter is incremented by the number of bytes indicated in the `read`, `readv` and `writev` fields.
  Labels are `pgm`, `xrdid`, `protocol` and `client_domain`.
- `xrootd_data_disconnects_total` and `xrootd_data_disconnects_created` &ndash; When a [Disc event](https://xrootd.slac.stanford.edu/doc/dev51/xrd_monitoring.htm#_Toc49119287) is received in an `f`-stream, this counter is incremented.
  Labels are `pgm`, `xrdid`, `protocol`, `client_domain`, `ip_version` and `auth`.
- `xrootd_tpc_*` &ndash; These accumulate TPCs, indexed by:
  - `direction`: `pull` or `push`;
  - `ip_version`: `4` or `6`;
  - `protocol`: such as `https` or `root`;
  - `streams`: the number of streams used;
  - `commander_domain`: the domain of the client instigating the copy;
  - `peer_domain`: the domain of the source for a pull, or of the destination for a push.
  The metrics include:
  - `xrootd_tpc_total` &ndash; the total number of TPCs.
  - `xrootd_tpc_failure_total` &ndash; the number of failed TPCs (with a non-zero exit code);
  - `xrootd_tpc_success_total` &ndash; the number of successful TPCs (with a non-ero exit code);
  - `xrootd_tpc_volume_bytes_total` &ndash; the number of bytes transferred by TPCs;
  - `xrootd_tpc_duration_seconds_total` &ndash; the time spent performing TPCs.
- `xrootd_redirection_total` &ndash; This counts redirections, indexed by:
  - `ip_version`: `4` or `6`;
  - `protocol`: such as `https` or `root`;
  - `redhost`: the hostname of the server redirected to;
  - `redport`: the port of the server redirected to.
  The port can be overridden by setting `data.fake_port`.

All pushed metrics include the label `job="xrootd_detail"`, overridden by `destination.push.detail_job`.



## Event log

Here's some example output (wrapped and spaced) of the log generated by `destination.log`/`-o` (derived from detailed monitoring):

```
2023-03-20T16:10:52.000 cluster@foo.example.com xrootd open rw=True \
path=/foo/bar/baz.upload prot=xroot user=prdatlas \
client_name=[::ffff:10.18.6.92] client_addr=[::ffff:10.18.6.93] ipv=4 dn="" \
auth=gsi client_domain=local

2023-03-20T16:10:54.000 cluster@baz.example.com xrootd close read_bytes=0 \
readv_bytes=0 write_bytes=605376041 forced=False prot=xroot user=foo \
client_name=[::ffff:10.23.41.12] path=/foo/bar/baz.upload client_domain=local

2023-03-20T16:07:30.000 cluster@baz.example.com xrootd disconnect \
prot=https user=bar client_name=foo.example.com \
client_addr=[::ffff:321.123.92.48] ipv=4 dn=##### auth=gsi \
client_domain=###.example.com
```
