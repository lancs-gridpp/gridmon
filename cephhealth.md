# Ceph disc health metrics exporter

Ceph can be made to collect SMART metrics to ascertain the health of its discs.
`cephhealth-exporter` pulls these metrics at a scheduled time, and pushes them into Prometheus.
At the scheduled time, it performs this command to identify all OSD numbers:

```
ceph osd ls --format=json
```

For each OSD number `NUM`, it invokes the following:

```
ceph device query-daemon-health-metrics --format=json osd.NUM
```

The result is a map from a device id, usually in the form `MAKE_MODEL_SERIAL`, to various device metrics.
The following are extracted from SCSI devices, each indexed by a `devid` label:

- `cephhealth_scsi_grown_defect_list_total` &ndash; the `scsi_grown_defect_list` reading; a counter, also with a `_created` time, which is always 0
- `cephhealth_scsi_uncorrected_total` &ndash; the `scsi_error_counter_log` reading; a counter, also with a `_created` time, which is always 0; a `mode` label distingishes between `read`, `write` and `verify`

The following are extracted from NVME devices (from `nvme_smart_health_information_log`), also indexed by `devid`:

- `cephhealth_nvme_unsafe_shutdowns_total` &ndash; the `unsafe_shutdowns` field
- `cephhealth_nvme_errlog_entries_total` &ndash; the `num_err_log_entries` field
- `cephhealth_nvme_temperature_celsius` &ndash; the `temperature` field
- `cephhealth_nvme_media_errors_total` &ndash; the `media_errors` field
- `cephhealth_nvme_controller_busy_minutes_total` &ndash; the `controller_busy_time` field
- `cephhealth_nvme_power_cycles_total` &ndash; the `power_cycles` field
- `cephhealth_nvme_power_on_hours_total` &ndash; the `power_on_hours` field
- `cephhealth_nvme_percentage_used` &ndash; the `percentage_used` field
- `cephhealth_nvme_host_reads_total` &ndash; the `host_reads` field
- `cephhealth_nvme_host_writes_total` &ndash; the `host_writes` field
- `cephhealth_nvme_data_read_megabytes_total` &ndash; the `data_units_read` field, multipled by 1000 and the block size, then divided by 1024×1024
- `cephhealth_nvme_data_written_megabytes_total` &ndash; the `data_units_written` field, multipled by 1000 and the block size, then divided by 1024×1024

The block size is obtained from `logical_block_size`.
All metrics ending in `_total` are counters, and have a dual ending in `_created` which is always 0.

These metrics are timestamped according to a field in the mapped value, and pushed to a remote-write endpoint, including the label `job="cephhealth"`.
This process can take a second or so per disc, so remote-writing ensures that the metrics for each disc are delivered to Prometheus in a timely manner.

The process also establishes a scraping endpoint for metrics that can be obtained relatively quickly:

- `cephhealth_disk_fitting` (formerly `cephhealth_metadata`; still present, but deprecated) &ndash; always 1; includes `devid`; includes the label `path` giving the value of `location` with the `/dev/disk/by-path/` prefix lopped off
- `cephhealth_status_check` &ndash; a gauge in the cluster health status as reported by `ceph status`; includes a label `type` indicating what is being counted, e.g., `PG_NOT_DEEP_SCRUBBED`
- `cephhealth_osd_pg_complaint` &ndash; always 1; includes `ceph_daemon` in the form `osd.NUM`, indicating which OSD is complaining; includes `pg_id`, the PG id being complained about; includes `pool_id`, the pool to which the PG belongs

They can then be scraped by Prometheus in [OpenMetrics format](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md).
Obtaining these metrics dynamically is not exactly instantaneous, so a scrape interval of at least 5 minutes is recommended.
For a large interval (say, an hour), you will probably need to use `last_over_time(cephhealth_status_check[1h10m])` to ensure they are detected.

The `devid` label can be correlated with the `device_ids` label of `ceph_disk_occupation` metrics supplied by Ceph itself.
However, `device_ids` must be processed first to get a match.
For example, it might contain `nvme0n1=Foo_Zippy1.5_987987532,sda=Bar_Whooshy2.0_927343`, while `devid` only contains `Bar_Whooshy2.0_927343`.
You need to use `label_replace` on it in PromQL to create a suitable label.

For example, to get a list of discs with at least one defect, include which OSD they serve, which host they are on, their `/dev/` names and device paths:

```
(last_over_time(cephhealth_scsi_grown_defect_list_total[1d1h]) > 0) * on(devid) group_right() avg without (device_ids, devices) (label_replace(label_replace(avg without (exported_instance, instance, job) (ceph_disk_occupation), "devid", '$2', "device_ids", `([^=]+=[^,]+,)?[^=]+=(.*)`), "disk", '$2', "devices", `([^,]+,)?(.*)`)) * on(devid) group_left(path) last_over_time(cephhealth_metadata[1d1h])
```

The following arguments are accepted:

- `-l INT` &ndash; the number of seconds of lag; default 20
- `-s HH:MM` &ndash; Add the time of day to the daily schedule.
  Ceph is scanned at each scheduled time.
- `-h INT` &ndash; seconds of horizon, beyond which metrics are discarded; 30 is the default
- `-t PORT` &ndash; port number to bind to (HTTP/TCP); 8799 is the default
- `-T HOST` &ndash; hostname/IP address to bind to (HTTP/TCP); empty string is `INADDR_ANY`; `localhost` is default
- `-z` &ndash; Open `/dev/null` and duplicate it to `stdout` and `stderr`.
  Use this in a cronjob to obviate starting a separate shell to perform redirection.
- `--log=LEVEL` &ndash; Set the log level.
  `info` is good.
- `--log-file=FILE` &ndash; Append logging to a file.
- `--disk-limit=INT` &ndash; Stop after getting non-empty data from this many discs.
  This is intended mainly for debugging on a small scale, without having to wait six minutes to scrape 700 discs!
- `--now` &ndash; Perform a single scrape of the discs immediately, then settle into the configured schedule.

Any remaining arguments are prefixed to the executed commands.
This allows the script to run on a different host to Ceph, and SSH into it, for example.
Use `--` if any of the arguments could be mistaken for switches to this script.
