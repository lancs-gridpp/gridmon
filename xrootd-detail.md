# Detailed XRootD statistics

(deprecated)

The script `xrootd-detail` receives UDP packets in the format defined
by [Detailed Monitoring Data Format](https://xrootd.slac.stanford.edu/doc/dev51/xrd_monitoring.htm#_Toc49119279), generated using the [`xrootd.monitor`](https://xrootd.slac.stanford.edu/doc/dev50/xrd_config.htm#_monitor) configuration.
It pushes some metrics derived from these into Prometheus, and generates a `logfmt`-compatible log for Loki to tail.
It also runs an HTTP server, and serves metric documentation over it in [OpenMetrics format](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md).

The following arguments are accepted:

- `-u PORT` &ndash; port number to bind to (UDP); 9486 is the default
- `-U HOST` &ndash; hostname/IP address to bind to (UDP); empty string is `INADDR_ANY`, and is default
- `-t PORT` &ndash; port number to bind to (HTTP/TCP); 8746 is the default
- `-T HOST` &ndash; hostname/IP address to bind to (HTTP/TCP); empty string is `INADDR_ANY`; `localhost` is default
- `-z` &ndash; Open `/dev/null` and duplicate it to `stdout` and `stderr`.
  Use this in a cronjob to obviate starting a separate shell to perform redirection.
- `--log=LEVEL` &ndash; Set the log level.
  `info` is good.
- `--log-file=FILE` &ndash; Append logging to a file.
- `-M ENDPOINT` &ndash; Push metrics to a remote-write endpoint.
- `-o FILE` &ndash; Append stream-derived log entries to this file.
- `--pidfile=FILE.pid` &ndash; Write the process id to this file.
  It should be deleted on exit.
- `-d FILE` &ndash; Load domain information from this file.
- `-i NUM` &ndash; Timeout in minutes for dictids.
  The default is 120.
