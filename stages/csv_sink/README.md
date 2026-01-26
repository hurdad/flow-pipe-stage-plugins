# csv_sink

## Configuration

```yaml
path: "/data/output.csv"
delimiter: ","
include_header: true
append: true
headers:
  - id
  - name
quote_all_fields: false
```

## Inputs

- Payload: JSON object encoded as UTF-8 bytes. Schema: `object<string, any>` where values are converted to strings (numbers/bools as text, nested objects/arrays as JSON strings) before writing CSV.

## Outputs

- None (sink stage).
