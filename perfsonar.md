## PerfSONAR statistics

The script `perfsonar-stats` pulls data from the `esmond` service of a PerfSONAR instance, and turns it into Prometheus-compatible metrics.

The following arguments are accepted:

- `-h INT` &ndash; minutes of horizon, beyond which metrics are discarded; 30 is the default
- `-l INT` &ndash; the number of seconds of lag; default 20
- `-f INT` &ndash; seconds before the scraped period to look for metadata keys; default 0
- `-a INT` &ndash; seconds before the scraped period to look for metadata keys; default 60
- `-z` &ndash; Open `/dev/null` and duplicate it to `stdout` and `stderr`.
  Use this in a cronjob to obviate starting a separate shell to perform redirection.
- `-t PORT` &ndash; port number to bind to (HTTP/TCP); 8732 is the default
- `-T HOST` &ndash; hostname/IP address to bind to (HTTP/TCP); empty string is `INADDR_ANY`; `localhost` is default
- `-E ENDPOINT` &ndash; the `esmond` endpoint to fetch metrics from
- `-S HOST` &ndash; the host of the `esmond` endpoint, from which `https://HOST/esmond/perfsonar/archive/` is formed
- `--log=level` &ndash; Set the log level.
- `--log-file=file` &ndash; Set the log file; default is probably to `stderr`.
- `-M ENDPOINT` &ndash; Push metrics to a remote-write endpoint.

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

If you use `-M`, metrics are pushed to the endpoint immediately at the end of a consultation.
In this case, the HTTP server serves no metrics, but still yields metric documentation in the form of `# HELP`, `# TYPE` and `# UNIT`, which you might choose to scrape infrequently.

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

All pushed metrics include the label `job="perfsonar"`.
