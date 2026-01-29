# Static metrics

The script `ip-statics-exporter` generates Prometheus-compatible metrics from static intent, and includes ping RTTs.
It runs continously, but can be started safely with a cronjob, quitting if it's already running.


## Run-time dependencies

`ip-statics-exporter` requires `frozendict`, [Protocol Buffers](https://developers.google.com/protocol-buffers) and [Snappy compression](http://google.github.io/snappy/) for pushing to Prometheus, so try one of these:

```
sudo dnf install python3-snappy python3-protobuf python3-frozendict
```

```
sudo apt-get install python3-snappy python3-protobuf python3-frozendict
```


## Command-line arguments

The following options are accepted:

- `-h INT` &ndash; seconds of horizon, beyond which metrics are discarded; 120 is the default
- `-t PORT` &ndash; port number to bind to (HTTP/TCP); 9363 is the default
- `-T HOST` &ndash; hostname/IP address to bind to (HTTP/TCP); empty string is `INADDR_ANY`; `localhost` is default
- `-z` &ndash; Open `/dev/null` and duplicate it to `stdout` and `stderr`.
  Use this in a cronjob to obviate starting a separate shell to perform redirection.
- `--log=LEVEL` &ndash; Set the log level.
  `info` is good.
- `--log-file=FILE` &ndash; Append logging to a file.
- `-f FILE` &ndash; Add the file to the list scanned each time metrics are generated.
- `-M ENDPOINT` &ndash; Push metrics to a remote-write endpoint.

## Source format

Every minute, files specified with `-f` are read to describe what metrics to generate.
The source is expressed in YAML, and must contain a map with an entry `machines`.
Each key is then a machine or node name, and the value is a map describing it.
The node name appears as the label `node` on many metrics, including all the `machine_` ones, and `ip_metadata`.
The following keys are recognized:

- `building`, `room`, `rack`, `level` &ndash; These strings appear as labels in `machine_metadata`, and can be used to describe the location of the machine.
- `drive_layout` &ndash; This string appears as label `dloid` in a metric `machine_drive_layout`, and should be used to map drive device paths to physical ports/slots/sockets on the host.
- `interfaces` &ndash; This describes interfaces present on the node.
  IP addresses or DNS names are keys, and the values are maps with the following keys:
  - `device` &ndash; the internal device name, such as `eth0`
  - `network` &ndash; any form of network identifier, e.g., `public`, which can be cross-referenced with other interfaces to infer connectivity
  - `roles` &ndash; an arbitrary set of roles that the interface fulfils
  - `slaves` &ndash; a list of internal names of devices that are bonded to form this device
  `network` and `device` appear in the metric `ip_metadata`.
  The interface name appears as the label `iface` on almost all `ip_` metrics.
  Each role generates an `ip_role` metric.
  Additionally, a role of `xroot` identifies the device that XRootD uses to determine its full name.
- `enabled` &ndash; Assumed `true` if absent, this allows a node to be ignored from the configuration.
- `clusters` &ndash; This holds a dictionary whose keys are cluster identifiers that the machine plays a part in.
  Metrics generated from the `clusters` entry have a `cluster` label giving the cluster identifier, and this cross-references entries in the top-level `cluster` dictionary.
  The values of the `clusters` dictionary are themselves dictionaries with the following optional values:
  - `osds` &ndash; This specifies the number of Ceph OSDs that the node should be running, and appears as the value of the `machine_osd_drives` metric.
  - `roles` &ndash; This specifies an array of role names.
    A metric `machine_role` is generated for each one on that node.
  - `xroots` &ndash; This lists names of XRootD instances expected to be running on the node.
    Exactly one interface must be assigned the role `xroot`, and then all instances full names are formed from `INSTANCE-NAME@INTERFACE-NAME`, which appears as the label `xrdid` in a metric `xrootd_expect`, with the label `pgm` set to `xrootd`.
  - `cmses` &ndash; This lists names of CMSd instances expected to be running on the node.
    Exactly one interface must be assigned the role `xroot`, as described for `xroots`.
    The corresponding metric `xrootd_expect` has the label `pgm` set to `cmsd`.

All node names must be unique.
All interface names must be unique.

A `sites` top-level map entry may exist, with site names as keys.
Each value is also a map in which the sole key `domains` is recognized.
Its value is an array of domain names associated with the site.


A `site_groups` top-level map entry may exist, with site groups' names as keys.
Each value is an array of site names or group names belonging to the site.


A `drive_paths` top-level map entry may exist to define drive path patterns and layouts.


A `clusters` top-level map entry may exist, whose keys are cluster identifiers, and whose values are maps with the following entries:

- `name` &ndash; This specifies the display name for the cluster.
  A metric `cluster_meta` is generated, with label `cluster` being the cluster identifier, and `cluster_name` being the value of this entry.
  The value is `1`.
- `ceph` &ndash; This boolean indicates whether a Ceph instance is expected in the cluster.
  A metric `cluster_expect_ceph` with the `cluster` label is generated with the value `1`.
- `vos` &ndash; This map defines a set of named VOs (the keys).
  Each entry may contain:
  - `name` &ndash; the display name of the VO;
  - `dns` &ndash; a list of certificate DNs known to identify the VO;
  - `jobs` &ndash; a map of:
    - `users` &ndash; a list of compute usernames used by the VO;
    - `accounts` &ndash; a list of accounts used by the VO;
  - `transfers` &ndash; a map of:
    - `users` &ndash; a list of file-transfer usernames used by the VO.
  The display name is used to generate a metric `vo_meta` with `cluster` as the cluster id, `vo_id` as the VO identifier and `vo_name` as the display name.
  The other entries generate the metric `vo_affiliation` with `cluster` and `vo_id` as before, `affiliation` as `job_user`, `job_account`, `transfer_user` or `dn`, and `affiliate` as the user name or DN.
  The value of these metrics is always `1`.

Interfaces are pinged every minute.
If a response is obtained, an `ip_ping_milliseconds` gauge is generated, and the `ip_up` gauge has the value `1`.
Otherwise, `ip_up` is `0`, and no `ip_ping_milliseconds` metric is generated.

If `-M` is specified, all metrics are written to this endpoint as they have all been generated.
Meanwhile, the HTTP server serves no metrics, only metric documentation.
Without `-M`, no remote-write occurs, and all metrics are served through the HTTP server.

All pushed metrics include the label `job="statics"`.

## Drive layouts

The `drive_paths` top-level map entry contains `patterns` and `layouts` map elements.
`patterns` defines device paths for drives, so they can be mapped to more meaningful label sets.
For example, the following defines a pattern called `gen1_hotplugs`, and matches strings such as `pci-0000:18:00.0-scsi-0:0:14:0` slot 14, row 2, column 4:

```
drive_paths:
  patterns:
    gen1_hotplugs:
      path: "pci-0000:18:00.0-scsi-0:0:{x}:0"
      fields:
        - name: x
          min: 0
          max: 23
      labels:
        drive_bank: hotplug
      computed_labels:
        drive_slot: 'x'
        drive_row: 'x // 6'
        drive_column: 'x % 6'
      formats:
        drive_slot: '%02d'
```

`computed_labels` values are limited Python expressions, referring to the variables named in `fields`.
`labels` defines only static values.
Elements of `formats` override the default `%s` used to format the label value.

The `layouts` map defines layouts are unions of patterns.
For example, the following defines that layout `gen1` is the single set of mappings from `gen1_hotplugs` (defined above):

```
drive_paths:
  layouts:
    gen1:
      - gen1_hotplugs
```

Together, they generate metrics such as:

```
dlo_meta{dloid="gen1",
         path="pci-0000:18:00.0-scsi-0:0:14:0",
         drive_bank="hotplug",
         drive_slot="14",
         drive_row="2",
         drive_column="2"} 1
```

The `dloid` label matches that of `machine_drive_layout`, and `path` matches that of `cephhealth_disk_fitting`, so a metric with `node` and `path` can first be augmented with `dloid` using `machine_drive_layout` on `node`, and then with these additional metrics using `dlo_meta` on `dloid` and `path`:

```
my_expr * on(node) group_left(dloid) machine_drive_layout
        * on(dloid, path) group_left(drive_bank, drive_slot) dlo_meta
```
