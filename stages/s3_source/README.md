# s3_source

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
```

## Inputs

- None (source stage).

## Outputs

- Payload: raw bytes of the S3 object body. Schema: `bytes` (opaque binary data).
