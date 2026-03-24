#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Source emsdk environment
source "$ROOT_DIR/emsdk/emsdk_env.sh" 2>/dev/null

OUT_DIR="$ROOT_DIR/tests/harness"

echo "Building Emscripten FS harness..."
emcc "$SCRIPT_DIR/harness.c" \
  -o "$OUT_DIR/emscripten_fs.mjs" \
  -s MODULARIZE=1 \
  -s EXPORT_NAME=createModule \
  -s EXPORTED_RUNTIME_METHODS='["FS","PATH","ERRNO_CODES"]' \
  -s ENVIRONMENT=node \
  -s FORCE_FILESYSTEM=1 \
  -s ALLOW_MEMORY_GROWTH=1 \
  -s NO_EXIT_RUNTIME=1 \
  -s INVOKE_RUN=0 \
  -O0

echo "Built: $OUT_DIR/emscripten_fs.mjs"
echo "Built: $OUT_DIR/emscripten_fs.wasm"
