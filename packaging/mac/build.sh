#!/usr/bin/env bash
set -euo pipefail

APP_NAME="XiaoCaiChat"
ICON_PNG="icons/ui/xiaocaichat.png"
BUILD_DIR="packaging/mac/build"

mkdir -p "$BUILD_DIR"

if [ ! -f "$ICON_PNG" ]; then
  echo "Icon PNG not found at $ICON_PNG" >&2
  exit 1
fi

# Generate .icns from PNG using macOS sips + iconutil
ICONSET="$BUILD_DIR/xiaocaichat.iconset"
rm -rf "$ICONSET"
mkdir -p "$ICONSET"

for size in 16 32 64 128 256 512; do
  sips -z $size $size "$ICON_PNG" --out "$ICONSET/icon_${size}x${size}.png" >/dev/null
  sips -z $((size*2)) $((size*2)) "$ICON_PNG" --out "$ICONSET/icon_${size}x${size}@2x.png" >/dev/null
done

iconutil -c icns "$ICONSET" -o "$BUILD_DIR/xiaocaichat.icns"

# Ensure PyInstaller is available
python -m pip show pyinstaller >/dev/null 2>&1 || python -m pip install -U pyinstaller

# Build app
ADD_DATA_ARGS=(--add-data "icons:icons" --add-data "themes:themes")
if [ -f client_config.json ]; then
  ADD_DATA_ARGS+=(--add-data "client_config.json:.")
fi

pyinstaller \
  --noconfirm \
  --windowed \
  --name "$APP_NAME" \
  --icon "$BUILD_DIR/xiaocaichat.icns" \
  "${ADD_DATA_ARGS[@]}" \
  qt_chat_client.py

echo "App built at dist/$APP_NAME.app"

# Ad-hoc sign the app to satisfy macOS Gatekeeper in some environments
if command -v codesign >/dev/null 2>&1; then
  echo "Signing app (ad-hoc)..."
  codesign --force --deep --sign - "dist/$APP_NAME.app" || true
fi

# Remove quarantine attribute if present (common for apps built from downloaded toolchains)
if command -v xattr >/dev/null 2>&1; then
  echo "Clearing quarantine attribute (if any)..."
  xattr -dr com.apple.quarantine "dist/$APP_NAME.app" || true
fi

echo "Done. Try: open dist/$APP_NAME.app"
