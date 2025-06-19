# HammerCloud notifications

The script `hammercloud-events` derives metrics from HammerCloud email notifications.
It looks for a subject line containing the likes of:

```
[TYPE] Auto-excluded PANDA-QUEUE-NAME
```

It also uses the date provided by the `Date:` header field, and remote-writes a metric point into Prometheus, specified by `-M`.
Run it as a Procmail rule, such as:

```
:0
| /usr/local/share/gridmon/hammercloud-events -M "http://localhost:9090/api/v1/write"
```

(You should probably also match on some other header fields like `List-Id:`, but if the subject doesn't match, nothing happens.)

You can also run it manually to fake an event:

```
/usr/local/share/gridmon/hammercloud-events -M "http://localhost:9090/api/v1/write" -q PANDA-QUEUE-NAME -t TYPE -x
```

`-x` means an exclusion.
`-r` means a reset.

You might need to permit Procmail and its child processes to connect to the remote-write interface:

```
sudo semanage permissive -a procmail_t
```

The metric generated is `hammercloud_state`, for example:

```
hammercloud_state{queue="PANDA-QUEUE-NAME",queue_type="TYPE"} 1
```

The value is `1` if the subject contains `Auto-excluded`, and `0` otherwise.
You might want to pick out exclusions of your own queue as an annotation, using an expression such as this:

```
last_over_time(hammercloud_state{queue=~"MYQUEUE_.*"}[10d]) > 0
```

As future work, the script might run persistently, maybe listen for new notifications on a Unix-domain socket, and periodically repeat non-zero metrics, so that events don't simply expire.

