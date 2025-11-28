# VO mapping

A VO mapping is a YAML file containing a dictionary whose keys are VO identifiers.
Each value is a dictionary with the following optional fields:

- `name` &ndash; a human-readable name
- `dns` &ndash; a list of Distingushed Names to be associated with the VO
- `token_issuers` &ndash; a list of URLs that issue tokens for the VO
- `jobs`:
  - `users` &ndash; a list of compute usernames used by the VO
  - `accounts` &ndash; a list of accounts used by the VO
- `transfers`:
  - `users` &ndash; a list of file-transfer usernames used by the VO
  - `paths` &ndash; a list of virtual paths associated with the VO

Currently, only the [XRootD monitor](xrootd.md) uses such a file, and only uses relevant parts.
(For example, `jobs` is meaningless to it.)
The program periodically checks the timestamp of the file, so that changes can be loaded in without a restart.

Configuration for the [static exporter](static.md) includes an embedded form of this file, but will later be made to read from a separate file.
