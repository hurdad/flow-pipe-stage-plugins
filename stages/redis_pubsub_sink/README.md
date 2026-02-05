# redis_pubsub_sink

See `redis_pubsub_sink.proto` for full options.
## Configuration

```yaml
host: "127.0.0.1"
port: 6379
channel: "events"
username: ""
password: ""
database: 0
command_timeout_ms: 1000
```

## Inputs

- Payload: raw bytes published to the Redis Pub/Sub channel. Schema: `bytes` (opaque binary data).

## Outputs

- None (sink stage).
