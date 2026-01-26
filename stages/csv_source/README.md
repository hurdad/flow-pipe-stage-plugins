# csv_source

## Configuration

```yaml
path: "/data/input.csv"
delimiter: ","
has_header: true
trim_whitespace: true
skip_empty_lines: true
```

## Inputs

- None (source stage).

## Outputs

- Payload: JSON object encoded as UTF-8 bytes. Keys are column headers (from the header row when `has_header` is true, otherwise `column1`, `column2`, ...), and values are string values from the CSV row. Schema: `object<string, string>`.
