# SRR pusher

This command reads an SRR, and pushes it as Prometheus metrics to a remote-write endpoint.


## Run-time dependencies

`push-srr` requires `frozendict`, [Protocol Buffers](https://developers.google.com/protocol-buffers) and [Snappy compression](http://google.github.io/snappy/) for pushing to Prometheus.


## Invocation

`push-srr` accepts the following arguments:

- `-i srr.json` or `--input=srr.json` &ndash; filename of source SRR
- `-o URL` or `--output=URL` &ndash; Remote-Write endpoint
- `--log-file=file` &ndash; filename for logging
- `--log=level` &ndash; Python logging level (e.g., `info`, `debug`, etc)

The program reads the source SRR, and writes it to the endpoint.
If no endpoint is specified, a Protobuf dump is written to standard output.

Although the program is short-lived, its logging can still be controlled.
Because it is short-lived, there's no need to send a signal to it to relinquish its handle to the log file.


## Metrics

The metrics generated share subsets of the following labels:

- `srv` &ndash; the field `storageservice.name` from the SRR, present on all `srr_*` metrics
- `ep` &ndash; the field `storageservice.storageendpoints[*].name`, i.e., the name of a storage endpoint, present on all `srr_endpoint_*` metrics
- `share` &ndash; the field `storageservice.storageshares[*].name`, i.e., the name of a storage share, present on all `srr_share_*` metrics

The following metrics are defined:

- `srr_service_info` has the value `1` and the following additional labels:
  - `srv_type` &ndash; the field `storageservice.implementation`
  - `srv_vers` &ndash; the field `storageservice.implementationversion`
  - `srv_qual` &ndash; the field `storageservice.qualitylevel`
- `srr_endpoint_info` has the value `1` and the following additional labels:
  - `base` &ndash; the field `storageservice.storageendpoints[*].endpointurl`
  - `ep_type` &ndash; the field `storageservice.storageendpoints[*].interfacetype`
  - `ep_qual` &ndash; the field `storageservice.storageendpoints[*].qualitylevel`
- `srr_share_path_info` has the value `1` and the following additional label:
  - `path` &ndash; the field `storageservice.storageshares[*].path[*]`
- `srr_share_capacity_bytes` gives a share's allocated capacity
- `srr_share_usage_bytes` gives a share's used capacity
- `srr_share_state` gives a share's `storageservice.storageshares[*].servingstate` as a numeric encoding:
  - `100` &ndash; `open`
  - `0` &ndash; any other value
- `srr_share_vo_info` has the value `1` and the following additional labels:
  - `vo_id` &ndash; the name of a VO, excluding role/group conditions
  - `vo_role` &ndash; a role within a VO, as defined by a `/Role=` attribute in `storageservice.storageshares[*].vos[*]`
  - `vo_group` &ndash; a group within a VO, as defined by a `/Group=` attribute in `storageservice.storageshares[*].vos[*]`
- `srr_share_endpoint_info` has the value `1` and the following additional label:
  - `ep`

The relationship between shares and endpoints is derived solely by the field `storageservice.storageshares[*].assignedendpoints`.
If it contains a single value `all`, the share is considered accesible through all endpoints given by `storageservice.storageendpoints[*].name`.
Otherwise, only the listed endpoints are considered.
Values in `storageservice.storageendpoints[*].assignedshares` are ignored.
(We assume that they express the same information as `assignedendpoints`.)
