#!/bin/bash
set -e
echo "Generating Pydantic models from OpenAPI spec..."
datamodel-codegen \
  --input openapi/openapi.yaml \
  --input-file-type openapi \
  --output src/models/generated.py \
  --output-model-type pydantic_v2.BaseModel
echo "Done: src/models/generated.py"
