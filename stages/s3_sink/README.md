# s3_sink

## Configuration

```yaml
bucket: "my-bucket"
key: "path/to/object.json"
region: "us-east-1"
endpoint: ""
use_path_style: false
access_key_id: ""
secret_access_key: ""
session_token: ""
content_type: "application/json"
```

## Inputs

- Payload: raw bytes uploaded as the S3 object body. Schema: `bytes` (opaque binary data).

## Outputs

- None (sink stage).
