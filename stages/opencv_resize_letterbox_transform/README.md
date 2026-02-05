# opencv_resize_letterbox_transform

See `opencv_resize_letterbox_transform.proto` for full options.
## Configuration

```yaml
output_width: 640
output_height: 640
resize_mode: "letterbox" # "letterbox" (default) or "stretch"
interpolation: INTERPOLATION_LINEAR
pad_color: [0, 0, 0]
output_format: ".jpg" # e.g. ".png"
output_params: []
```

## Inputs

- Payload: encoded image bytes (JPEG/PNG/etc).

## Outputs

- Payload: encoded image bytes after resizing and padding.
