# redis_stream_source

## Configuration

```yaml
host: "127.0.0.1"
port: 6379
stream: "events"
username: ""
password: ""
database: 0
start_id: "$"
block_timeout_ms: 1000
count: 1
field_name: "data"
```

## Inputs

- None (source stage).

## Outputs

- Payload: raw bytes from the Redis stream entry field (defaults to the first field when `field_name` is empty). Schema: `bytes` (opaque binary data).
