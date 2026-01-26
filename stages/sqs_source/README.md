# sqs_source

## Configuration

```yaml
queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/events"
region: "us-east-1"
endpoint: ""
access_key_id: ""
secret_access_key: ""
session_token: ""
max_number_of_messages: 5
wait_time_seconds: 10
visibility_timeout_seconds: 30
delete_after_read: true
```

## Inputs

- None (source stage).

## Outputs

- Payload: SQS message body as raw bytes. Schema: `bytes` (opaque binary data).
