# XRootD-Prometheus bridge

(deprecated)

The script `xrootd-stats` allows metrics emitted by XRootD to be absorbed by Prometheus.
The script listens on a UDP socket, and stores timestamped metrics in XML [Summary Monitoring Data Format](https://xrootd.slac.stanford.edu/doc/dev51/xrd_monitoring.htm#_Toc49119259) received over UDP.
It also runs an HTTP server, and serves timestamped metrics over it in [OpenMetrics format](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md).
The script distinguishes HTTP clients by `Authorization` header, and remembers the last time each client was successfully issued with metrics, and so only serves metrics with later timestamps.

The following arguments are accepted:

- `-h INT` &ndash; minutes of horizon, beyond which metrics are discarded; 30 is the default
- `-u PORT` &ndash; port number to bind to (UDP); 9485 is the default
- `-U HOST` &ndash; hostname/IP address to bind to (UDP); empty string is `INADDR_ANY`, and is default
- `-t PORT` &ndash; port number to bind to (HTTP/TCP); 8744 is the default
- `-T HOST` &ndash; hostname/IP address to bind to (HTTP/TCP); empty string is `INADDR_ANY`; `localhost` is default
- `-z` &ndash; Open `/dev/null` and duplicate it to `stdout` and `stderr`.
  Use this in a cronjob to obviate starting a separate shell to perform redirection.
- `--log=LEVEL` &ndash; Set the log level.
  `info` is good.
- `--log-file=FILE` &ndash; Append logging to a file.
- `-E ENDPOINT` &ndash; Push metrics to a remote-write endpoint.

If you use `-E`, metrics are pushed to the endpoint as soon as a UDP report arrives and its contents are converted.
In this case, the HTTP server serves no metrics, but still yields metric documentation in the form of `# HELP`, `# TYPE` and `# UNIT`, which you might choose to scrape infrequently.
