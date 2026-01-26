# http_sink

## Configuration

```yaml
url: "https://example.com/ingest"
method: METHOD_POST
content_type: "application/json"
timeout_ms: 5000
headers:
  - name: "X-Request-Id"
    value: "flowpipe"
```

## Inputs

- Payload: raw bytes used as the HTTP request body. Schema: `bytes` (opaque binary data; often UTF-8 JSON when using the default content type).

## Outputs

- None (sink stage).
