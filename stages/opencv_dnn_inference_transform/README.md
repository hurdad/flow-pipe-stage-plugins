# opencv_dnn_inference_transform

## Configuration

```yaml
model_path: "/models/resnet50.onnx"
config_path: ""
framework: ""
input_width: 224
input_height: 224
scale: 0.0039215686
mean_values: [0.485, 0.456, 0.406]
swap_rb: true
crop: false
input_name: ""
output_names: []
```

## Inputs

- Payload: encoded image bytes (JPEG/PNG/etc).

## Outputs

- Payload: UTF-8 JSON string containing output layer names, shapes, and flattened
  float data.
