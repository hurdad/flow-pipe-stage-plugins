# sqs_sink

## Configuration

```yaml
queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/events"
region: "us-east-1"
endpoint: ""
access_key_id: ""
secret_access_key: ""
session_token: ""
message_group_id: "group-1"
message_deduplication_id: "dedupe-1"
```

## Inputs

- Payload: raw bytes sent as the SQS message body. Schema: `bytes` (opaque binary data).

## Outputs

- None (sink stage).
