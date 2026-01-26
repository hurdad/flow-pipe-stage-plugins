# kafka_sink

## Configuration

```yaml
brokers: "localhost:9092"
topic: "events"
client_id: "flowpipe-sink"
partition: -1
producer_config:
  acks: "all"
```

## Inputs

- Payload: raw bytes sent as the Kafka message value. Schema: `bytes` (opaque binary data).

## Outputs

- None (sink stage).
