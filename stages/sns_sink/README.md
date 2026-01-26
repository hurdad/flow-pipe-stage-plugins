# sns_sink

## Configuration

```yaml
topic_arn: "arn:aws:sns:us-east-1:123456789012:events"
region: "us-east-1"
endpoint: ""
access_key_id: ""
secret_access_key: ""
session_token: ""
subject: "Flowpipe Events"
message_structure: "json"
```

## Inputs

- Payload: raw bytes used as the SNS message body. Schema: `bytes` (often UTF-8 text or JSON when `message_structure` is set).

## Outputs

- None (sink stage).
