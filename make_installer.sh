#!/bin/bash

# Fail on any error
set -e

APP_PYTHON="/opt/homebrew/Caskroom/miniforge/base/envs/groovekraft/bin/python"

echo "🛠 Cleaning previous builds..."
rm -rf build/ dist/ GrooveKraft.zip dist/GrooveKraft.dmg

echo "🎨 Rebuilding .icns icon..."
./make_icon.sh

if [ -x "$APP_PYTHON" ]; then
  PYINSTALLER_CMD=("$APP_PYTHON" -m PyInstaller)
elif command -v pyinstaller >/dev/null 2>&1; then
  PYINSTALLER_CMD=(pyinstaller)
else
  echo "❌ Could not find PyInstaller. Expected $APP_PYTHON or a pyinstaller executable on PATH."
  exit 1
fi

echo "🚀 Building GrooveKraft.app..."
"${PYINSTALLER_CMD[@]}" GrooveKraft.spec

echo "📦 Creating DMG installer..."

# Move into dist/ where GrooveKraft.app was created
cd dist

# Create compressed DMG (native Mac tool)
hdiutil create -volname "GrooveKraft" -srcfolder "GrooveKraft.app" -ov -format UDZO "GrooveKraft.dmg"

cd ..

echo "✅ Done! Find GrooveKraft.dmg in the dist/ folder."
