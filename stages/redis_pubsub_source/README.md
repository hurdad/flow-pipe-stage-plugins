# redis_pubsub_source

## Configuration

```yaml
host: "127.0.0.1"
port: 6379
channel: "events"
username: ""
password: ""
database: 0
poll_timeout_ms: 1000
```

## Inputs

- None (source stage).

## Outputs

- Payload: Redis Pub/Sub message payload as raw bytes. Schema: `bytes` (opaque binary data).
