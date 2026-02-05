# kafka_source

See `kafka_source.proto` for full options.
## Configuration

```yaml
brokers: "localhost:9092"
topic: "events"
group_id: "flowpipe"
client_id: "flowpipe-source"
auto_offset_reset: "earliest"
poll_timeout_ms: 1000
partition: -1
consumer_config:
  enable.auto.commit: "true"
```

## Inputs

- None (source stage).

## Outputs

- Payload: Kafka message value as raw bytes. Schema: `bytes` (opaque binary data).
