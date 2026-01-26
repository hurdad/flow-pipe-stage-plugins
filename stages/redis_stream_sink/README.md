# redis_stream_sink

## Configuration

```yaml
host: "127.0.0.1"
port: 6379
stream: "events"
username: ""
password: ""
database: 0
field_name: "data"
maxlen: 10000
maxlen_approx: true
id: "*"
command_timeout_ms: 1000
```

## Inputs

- Payload: raw bytes stored in the Redis stream entry under `field_name` (defaults to `data`). Schema: `bytes` (opaque binary data).

## Outputs

- None (sink stage).
