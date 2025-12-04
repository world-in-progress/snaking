#!/bin/bash
set -e

# Remove internal/proto folder
if [ -d "internal/proto" ]; then
    rm -rf internal/proto
    echo "Removed internal/proto"
fi

# Create internal directory if it doesn't exist
mkdir -p internal

# Generate Go code into internal/proto
protoc --go_out=./internal --go_opt=paths=source_relative \
       --go-grpc_out=./internal --go-grpc_opt=paths=source_relative \
       proto/snaking.proto

echo "Done: Generated Go code in internal/proto"

# --- Python Generation ---

PYTHON_PROTO_DIR="python/snaking/src/snaking/proto"

# 1. Remove python proto folder
if [ -d "$PYTHON_PROTO_DIR" ]; then
    rm -rf "$PYTHON_PROTO_DIR"
    echo "Removed $PYTHON_PROTO_DIR"
fi

# 2. Create python proto directory
mkdir -p "$PYTHON_PROTO_DIR"

# 3. Activate Python environment and generate code
# We use a subshell to avoid affecting the current shell environment with 'source'
(
    source python/snaking/.venv/bin/activate
    
    # Use python -m grpc_tools.protoc to ensure we use the tools from the venv
    # We use -Iproto and snaking.proto (instead of -I. and proto/snaking.proto)
    # to generate files directly in the output directory without the 'proto/' subdirectory.
    # Added --pyi_out to generate type stubs for IDE support
    python -m grpc_tools.protoc -Iproto \
           --python_out="$PYTHON_PROTO_DIR" \
           --grpc_python_out="$PYTHON_PROTO_DIR" \
           --pyi_out="$PYTHON_PROTO_DIR" \
           snaking.proto

    # Fix import in generated grpc file to use relative import
    sed -i 's/import snaking_pb2 as snaking__pb2/from . import snaking_pb2 as snaking__pb2/' "$PYTHON_PROTO_DIR/snaking_pb2_grpc.py"
)

echo "Done: Generated Python code in $PYTHON_PROTO_DIR"
