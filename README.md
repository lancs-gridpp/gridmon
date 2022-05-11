# Purpose



## XRootD-Prometheus bridge

The script `xrootd-stats` allows metrics emitted by XRootD to be absorbed by Prometheus.
XRootD pushes metrics over UDP through its [`xrd.report`](https://xrootd.slac.stanford.edu/doc/dev50/xrd_config.htm#_report) configuration, while Prometheus pulls metrics over HTTP exporters.
The script listens on a UDP socket, and stores timestamped metrics in XML [Summary Monitoring Data Format](https://xrootd.slac.stanford.edu/doc/dev51/xrd_monitoring.htm#_Toc49119259) received over UDP.
It also runs an HTTP server, and serves timestamped metrics over it in [OpenMetrics format](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md).
The script distinguishes HTTP clients by `Authorization` header, and remembers the last time each client was successfully issued with metrics, and so only serves metrics with later timestamps.

The following arguments are accepted:

- `-h *int*` -- minutes of horizon, beyond which metrics are discarded
- `-u *port*` -- port number to bind to (UDP); 9485 is the default
- `-U *host*` -- hostname/IP address to bind to (UDP); empty string is `INADDR_ANY`, and is default
- `-t *port*` -- port number to bind to (HTTP/TCP); 8744 is the default
- `-T *host*` -- hostname/IP address to bind to (HTTP/TCP); empty string is `INADDR_ANY`; `localhost` is default


Each variable specified by the XRootD format is represented by an OpenMetrics metric family by converting dots to underscores, prefixing with `xrootd_`, and suffixing with additional terms as expected by OpenMetrics.

For example, the variable `buff.mem` is presented as the metric `xrootd_buff_mem_bytes`.
XRootD documents this variable with ↔, which is taken to mean that the value can both rise and fall; it is therefore given the metric type `gauge`.
In contrast, the variable `buff.reqs` lacks this (or any) mark, so it is treated as a counter, and is represented as two metrics, `xrootd_buff_req_total` (the value of the variable) and `xrootd_buff_req_created` (the Unix timestamp of when it was last reset to zero).

Note that the translation of many metrics has not yet been implemented.
The following have been implemented, and are indexed by the properties `host` and `name` (taken from the `<stats id="info">` element):

- `xrootd_buff_adj_total` -- adjustments to the buffer profile
- `xrootd_buff_buffs` -- number of allocated buffers
- `xrootd_buff_mem_bytes` -- memory allocated to buffers
- `xrootd_buff_reqs_total` -- requests for a buffer
- `xrootd_link_ctime_seconds_total` -- session time in connections
- `xrootd_link_in_bytes_total` -- data received
- `xrootd_link_maxn_total` -- maximum concurrent connections
- `xrootd_link_num` -- concurrent connections
- `xrootd_link_out_bytes_total` -- data sent
- `xrootd_link_sfps_total` -- occurrences of partial sendfile operations
- `xrootd_link_stall_total` -- occurrences of partial data
- `xrootd_link_tmo_total` -- read request timeouts
- `xrootd_link_tot_total` -- connections
- `xrootd_ofs_bxq_total` -- background tasks processed
- `xrootd_ofs_dly_total` -- delays imposed
- `xrootd_ofs_err_total` -- errors encountered
- `xrootd_ofs_han` -- active file handles
- `xrootd_ofs_opp` -- files open in read-write POSC mode
- `xrootd_ofs_opr` -- files open in read mode
- `xrootd_ofs_opw` -- files open in read-write mode
- `xrootd_ofs_rdr_total` -- redirects processed
- `xrootd_ofs_rep_total` -- background replies processed
- `xrootd_ofs_ser_total` -- received events indicating failure
- `xrootd_ofs_sok_total` -- received events indicating success
- `xrootd_ofs_ups_total` -- occurrences of POSC-mode file unpersisted
- `xrootd_ofs_tpc_grnt_total` -- TPCs allowed
- `xrootd_ofs_tpc_deny_total` -- TPCs denied
- `xrootd_ofs_tpc_err_total` -- TPCs that failed
- `xrootd_poll_att` -- file descriptors attached for polling
- `xrootd_poll_en_total` -- poll-enable operations
- `xrootd_poll_ev_total` -- polling events
- `xrootd_poll_int_total` -- unsolicited polling operations
- `xrootd_proc_sys_seconds_total` -- system time
- `xrootd_proc_usr_seconds_total` -- user time
- `xrootd_sched_idle` -- number of scheduler threads waiting for work
- `xrootd_sched_inq` -- number of jobs in run queue
- `xrootd_sched_jobs_total` -- jobs requiring a thread
- `xrootd_sched_maxinq_total` -- longest run-queue length
- `xrootd_sched_tcr_total` -- thread creations
- `xrootd_sched_tde_total` -- thread destructions
- `xrootd_sched_threads_total` -- current scheduler threads
- `xrootd_sched_tlimr_total` -- occurrences of reaching thread limit
- `xrootd_sgen_as` -- asynchronous flag
- `xrootd_sgen_et_seconds` -- time to complete statistics
- `xrootd_sgen_toe_seconds_total` -- Unix time when statistics gathering ended
- `xrootd_xrootd_dly_total` -- requests ending with delay
- `xrootd_xrootd_err_total` -- requests ending with error
- `xrootd_xrootd_num_total` -- requests selecting `xrootd` protocol
- `xrootd_xrootd_rdr_total` -- requests redirected
- `xrootd_xrootd_aio_max_total` -- maximum concurrent asynchronous requests
- `xrootd_xrootd_aio_num_total` -- asynchronous requests processed
- `xrootd_xrootd_aio_rej_total` -- asynchronous requests converted to synchronous
- `xrootd_xrootd_lgn_af_total` -- authentication failkures
- `xrootd_xrootd_lgn_au_total` -- successful authenticated logins
- `xrootd_xrootd_lgn_num_total` -- login attempts
- `xrootd_xrootd_lgn_ua_total` -- successful unauthenticated logins
- `xrootd_xrootd_ops_getf_total` -- `getfile` requests
- `xrootd_xrootd_ops_misc_total` -- 'other' requests
- `xrootd_xrootd_ops_open_total` -- file-open requests
- `xrootd_xrootd_ops_pr_total` -- pre-read requests
- `xrootd_xrootd_ops_putf_total` -- `putfile` requests
- `xrootd_xrootd_ops_rd_total` -- read requests
- `xrootd_xrootd_ops_rf_total` -- cache-refresh requests
- `xrootd_xrootd_ops_rs_total` -- readv segments
- `xrootd_xrootd_ops_rv_total` -- readv requests
- `xrootd_xrootd_ops_sync_total` -- sync requests
- `xrootd_xrootd_ops_wr_total` -- write requests


The following metrics offer meta data:

- `xrootd_ofs_meta_info` -- holds reporter's role as property `role`

In addition to `host` and `name`, the following are defined to have `lp` and `rp` properties:

- `xrootd_oss_paths_free_bytes` -- free space
- `xrootd_oss_paths_tot_bytes` -- capacity
- `xrootd_oss_paths_ifr_inodes` -- free space
- `xrootd_oss_paths_ino_inodes` -- capacity
