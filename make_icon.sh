#!/bin/bash

# Fail immediately if any command fails
set -e

ICON_SRC="assets/groovekraft_icon.png"
ICONSET_DIR="assets/groovekraft.iconset"
ICNS_FILE="assets/groovekraft.icns"

# Remove previous iconset if it exists
rm -rf "$ICONSET_DIR"

# Create new iconset folder
mkdir -p "$ICONSET_DIR"

# Generate required icon sizes
sips -z 16 16     "$ICON_SRC" --out "$ICONSET_DIR/icon_16x16.png"
sips -z 32 32     "$ICON_SRC" --out "$ICONSET_DIR/icon_16x16@2x.png"
sips -z 32 32     "$ICON_SRC" --out "$ICONSET_DIR/icon_32x32.png"
sips -z 64 64     "$ICON_SRC" --out "$ICONSET_DIR/icon_32x32@2x.png"
sips -z 128 128   "$ICON_SRC" --out "$ICONSET_DIR/icon_128x128.png"
sips -z 256 256   "$ICON_SRC" --out "$ICONSET_DIR/icon_128x128@2x.png"
sips -z 256 256   "$ICON_SRC" --out "$ICONSET_DIR/icon_256x256.png"
sips -z 512 512   "$ICON_SRC" --out "$ICONSET_DIR/icon_256x256@2x.png"
sips -z 512 512   "$ICON_SRC" --out "$ICONSET_DIR/icon_512x512.png"
cp "$ICON_SRC" "$ICONSET_DIR/icon_512x512@2x.png"

# Convert to .icns
iconutil -c icns "$ICONSET_DIR" -o "$ICNS_FILE"

echo "✅ Icon built successfully at $ICNS_FILE"
