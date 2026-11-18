# roble-cli

`roble`, a command line tool for [roble](https://github.com/merlimat/roble)
databases: it puts, gets, scans and deletes entries, shows how a database's
tables are spread across its levels, and reads single Pebble files, sstables
and MANIFESTs.

```sh
cargo install roble-cli
```

## Example

```console
$ roble -d /var/tmp/fruit put apple red
$ roble -d /var/tmp/fruit put banana yellow
$ roble -d /var/tmp/fruit get banana
yellow
$ roble -d /var/tmp/fruit scan
apple	red
banana	yellow
$ roble -d /var/tmp/fruit delete banana
$ roble -d /var/tmp/fruit get banana
roble: key not found
```

`-d` names the database's directory, the current one by default. Keys and
values are bytes: `\xNN` in an argument stands for the byte with hex value
`NN`.

## Commands

| Command | What it does |
|---|---|
| `put`, `delete`, `delete-range` | Write to the database, taking its lock |
| `get`, `scan` | Read entries, without a lock, beside a process writing to it |
| `stats`, `tables` | Show the database's levels and every table in them |
| `sst`, `manifest` | Read a single table file or MANIFEST |
| `completions` | Print a completion script for a shell |

`roble help <command>` describes each one. Exit status is 0 on success, 1 when
`get` finds no such key, and 2 on errors.

## License

Apache-2.0, as [LICENSE](LICENSE) states.
