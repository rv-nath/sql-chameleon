#  Schema Converter
A command line tool for converting any bunch of sql (mostly DDL)
to a specified target dialect.  For example, convert from MySQL to Oracle or vice versa.

Philosopy:
- The program reads in an sql file of a given dialect from cmdline.
- Parses the content into a dialect agnostic AST.
- Emits the target sql statements.

## Install

### Download a binary (Linux x86_64)

Download the binary, verify it, and put it on your PATH:

```sh
VERSION=v0.2.3
BASE=https://github.com/rv-nath/sql-chameleon/releases/download/$VERSION
curl -LO $BASE/schema-conv-$VERSION-x86_64-linux
curl -LO $BASE/schema-conv-$VERSION-x86_64-linux.sha256
sha256sum -c schema-conv-$VERSION-x86_64-linux.sha256
chmod +x schema-conv-$VERSION-x86_64-linux
sudo mv schema-conv-$VERSION-x86_64-linux /usr/local/bin/schema-conv
```

The binary is statically linked against musl, so it needs no glibc and runs on
any Linux distribution, however old.

Check the [releases page](https://github.com/rv-nath/sql-chameleon/releases) for
the current version and bump `VERSION` to match.

### Build from source

Needed on macOS and Windows, since only a Linux binary is published. Any recent
stable Rust toolchain will do:

```sh
git clone https://github.com/rv-nath/sql-chameleon.git
cd sql-chameleon
cargo build --release      # binary lands in target/release/schema-conv
cargo test                 # optional
```

## Usage

```
Usage: schema-conv [OPTIONS] <SOURCE_FILE>

Arguments:
  <SOURCE_FILE>  Path to the source SQL file

Options:
  -f, --from <FROM>      Source dialect [default: mysql]
  -t, --to <TO>          Target dialect [default: mysql]
  -o, --output <OUTPUT>  Output file (if not specified, prints to console)
  -h, --help             Print help
```

Convert a MySQL schema to Oracle:

```sh
schema-conv input.sql -f mysql -t oracle -o output.sql
```

Leave off `-o` to print to stdout, which is handy for a quick look:

```sh
schema-conv input.sql -f mysql -t oracle | less
```

Round-trip MySQL to MySQL. Useful for normalising a schema, and for checking
what the parser actually understood about a file:

```sh
schema-conv input.sql -f mysql -t mysql -o normalised.sql
```

### Converting a whole tree

`utils/convert_all.sh` walks a source directory and mirrors its structure into
the target directory, converting every `.sql` file it finds:

```sh
utils/convert_all.sh <source_dir> <target_dir> -f mysql -t oracle
```

It reports per-file success or failure and a total at the end. It uses
`target/release/schema-conv` when present, otherwise `target/debug`, and builds
one if neither exists.

### Dialects

`-f` and `-t` accept `mysql`, `oracle`, `postgresql` (also `postgres`, `pg`),
`sqlite`, and `sqlserver` (also `mssql`).

Only some of those pairings are implemented so far:

| Direction | Status |
| --- | --- |
| `-f mysql -t oracle` | Supported — the main path |
| `-f mysql -t mysql` | Supported — round-trip / normalise |
| anything else | Names are accepted, but conversion fails with `Unsupported dialect: <name>` |

Distant Roadmap:
- The program can have an API that serves up data to a web UI, that can render the tables and their links in a graphical manner.

- GUI may have options for editing, saving, conversion of schemas from and to any dialect.
