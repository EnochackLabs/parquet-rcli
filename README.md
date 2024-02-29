# Parquet Inspector

A Rust version of Apache Parquet command-line tools.

## Build

```bash
cargo build --release
```

## Usage

```bash
./target/release/parquet-rcli
Usage: parquet-rcli <COMMAND>

Commands:
  cat          Prints the content of a Parquet file. The output contains only the data, no metadata is displayed
  row-count    Prints the count of rows in the Parquet file
  schema       Prints the schema of the Parquet file
  meta         Prints the metadata of the Parquet file
  size         Prints the size of the Parquet file
  column-size  Prints out the size in bytes and ratio of column(s) in the Parquet file
  rewrite      Rewrite one or more Parquet files to a new Parquet file
  help         Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```
