#!/bin/bash

# Fail on any error
set -e

echo "🛠 Cleaning previous builds..."
rm -rf build/ dist/ GrooveKraft.zip dist/GrooveKraft.dmg

echo "🎨 Rebuilding .icns icon..."
./make_icon.sh

echo "🚀 Building GrooveKraft.app..."
pyinstaller GrooveKraft.spec

echo "📦 Creating DMG installer..."

# Move into dist/ where GrooveKraft.app was created
cd dist

# Create compressed DMG (native Mac tool)
hdiutil create -volname "GrooveKraft" -srcfolder "GrooveKraft.app" -ov -format UDZO "GrooveKraft.dmg"

cd ..

echo "✅ Done! Find GrooveKraft.dmg in the dist/ folder."
