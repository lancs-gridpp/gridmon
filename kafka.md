# Kafka metrics

The script `kafka-exporter` connects to one or more Kafka queues as a consumer, and counts messages, key bytes, value bytes and connections.
These can be scraped as Prometheus-compatible metrics.

The following arguments are accepted:

- `-f FILE` Load queue configuration from YAML file.
  Can be used multiple times, merging details for identical queues.
- `-z` &ndash; Open `/dev/null` and duplicate it to `stdout` and `stderr`.
  Use this in a cronjob to obviate starting a separate shell to perform redirection.
- `-t PORT` &ndash; port number to bind to (HTTP/TCP); 8567 is the default
- `-T HOST` &ndash; hostname/IP address to bind to (HTTP/TCP); empty string is `INADDR_ANY`; `localhost` is default
- `--log=level` &ndash; Set the log level.
- `--log-file=file` &ndash; Set the log file; default is probably to `stderr`.

Queue configuration is a YAML file consisting of a map `queues`.
Each entry key is an arbitrary queue name which appears in matrics as the label `queue`.
Each entry value is a map with the following members:

- `bootstrap` &ndash; an array of `host:port` bootstrap addresses
- `topics` &ndash; an array of topic names to subscribe to
- `group` &ndash; the consumer group name, defaulting to `monitor`

Metrics can be scraped from the endpoint `http://host:port/metrics`, as specified by `-T` and `-t` above.

The following metrics are defined with the `topic` label specifying the topic:

- `kafka_key_volume_bytes_total` &ndash; the number of key bytes received
- `kafka_value_volume_bytes_total` &ndash; the number of value bytes received
- `kafka_messages_total` &ndash; the number of messages received

The following metrics are also defined without the `topic` label:

- `kafka_up` &ndash; `1` if a connection with the queue is established; `0` otherwise
- `kafka_connections_total` &ndash; the number of connection attempts

(Each `_total` metric has a corresponding `_created` metric giving the time of the metric's reset.)

For each queue, connections are attempted continuously on failure.
However, 30 seconds are guaranteed between any two connection attempts on the same queue.
