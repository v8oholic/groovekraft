#!/bin/bash

# Fail immediately if any command fails
set -e

ICON_SRC="assets/groovekraft_icon.png"
ICONSET_DIR="assets/groovekraft.iconset"
ICNS_FILE="assets/groovekraft.icns"
ICNS_BACKUP=""

cleanup() {
  if [ -n "$ICNS_BACKUP" ] && [ -f "$ICNS_BACKUP" ]; then
    rm -f "$ICNS_BACKUP"
  fi
}

trap cleanup EXIT

if [ -f "$ICNS_FILE" ]; then
  ICNS_BACKUP="$(mktemp /tmp/groovekraft.icns.XXXXXX)"
  cp "$ICNS_FILE" "$ICNS_BACKUP"
fi

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
sips -z 1024 1024 "$ICON_SRC" --out "$ICONSET_DIR/icon_512x512@2x.png"

# Convert to .icns
if iconutil -c icns "$ICONSET_DIR" -o "$ICNS_FILE"; then
  echo "✅ Icon built successfully at $ICNS_FILE"
elif [ -n "$ICNS_BACKUP" ] && [ -f "$ICNS_BACKUP" ]; then
  cp "$ICNS_BACKUP" "$ICNS_FILE"
  echo "⚠️ iconutil could not rebuild $ICNS_FILE on this macOS version; using the existing .icns instead."
else
  echo "❌ iconutil failed and no existing $ICNS_FILE is available to reuse."
  exit 1
fi
