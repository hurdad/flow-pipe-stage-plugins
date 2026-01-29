# Flow examples

This directory contains sample flow definitions for the stage plugins in this
repository. These flows are written in YAML and can be executed with the
`flow_runtime` binary from the Flow-Pipe runtime.

## Available flows

| File | Description |
| --- | --- |
| `csv_to_parquet_arrow.yaml` | Read a CSV file with `csv_arrow_source` and write a Parquet file with `parquet_arrow_sink`. |

## Running a flow

```sh
flow_runtime flows/csv_to_parquet_arrow.yaml
```

Ensure the runtime can find the plugins built from this repository (for example,
by installing them to `/opt/flow-pipe/plugins`).
