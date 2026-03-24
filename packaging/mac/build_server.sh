#!/usr/bin/env bash
set -euo pipefail

APP_NAME="XiaoCaiChatServer"
ICON_PNG="icons/ui/server.png"
BUILD_DIR="packaging/mac/build_server"
ENV_NAME="${ENV_NAME:-xiaocaichat}"

mkdir -p "$BUILD_DIR"

if [ ! -f "$ICON_PNG" ]; then
  echo "Icon PNG not found at $ICON_PNG" >&2
  exit 1
fi

ICONSET="$BUILD_DIR/server.iconset"
rm -rf "$ICONSET"
mkdir -p "$ICONSET"

for size in 16 32 64 128 256 512; do
  sips -z $size $size "$ICON_PNG" --out "$ICONSET/icon_${size}x${size}.png" >/dev/null
  sips -z $((size*2)) $((size*2)) "$ICON_PNG" --out "$ICONSET/icon_${size}x${size}@2x.png" >/dev/null
done

iconutil -c icns "$ICONSET" -o "$BUILD_DIR/server.icns"

PY_CMD=(python)
if command -v conda >/dev/null 2>&1; then
  if conda run -n "$ENV_NAME" python -V >/dev/null 2>&1; then
    PY_CMD=(conda run -n "$ENV_NAME" python)
  fi
fi

"${PY_CMD[@]}" -m pip install pyobjc-core pyobjc-framework-Cocoa >/dev/null 2>&1 || true
"${PY_CMD[@]}" -m pip show pyinstaller >/dev/null 2>&1 || "${PY_CMD[@]}" -m pip install pyinstaller

ADD_DATA_ARGS=(--add-data "icons:icons")
"${PY_CMD[@]}" -m PyInstaller \
  --noconfirm \
  --windowed \
  --name "$APP_NAME" \
  --icon "$BUILD_DIR/server.icns" \
  "${ADD_DATA_ARGS[@]}" \
  chat_server.py

: # make app agent (hide Dock)
PLIST="dist/$APP_NAME.app/Contents/Info.plist"
if [ -f "$PLIST" ]; then
  /usr/libexec/PlistBuddy -c "Add :LSUIElement bool true" "$PLIST" 2>/dev/null || /usr/libexec/PlistBuddy -c "Set :LSUIElement true" "$PLIST" || true
  
  # Extract version from chat_server.py
  VERSION=$(grep 'SERVER_VERSION =' chat_server.py | cut -d '"' -f 2)
  if [ -z "$VERSION" ]; then
    VERSION="1.0.2"
  fi
  
  # Set version
  /usr/libexec/PlistBuddy -c "Set :CFBundleShortVersionString $VERSION" "$PLIST" || true
  /usr/libexec/PlistBuddy -c "Set :CFBundleVersion $VERSION" "$PLIST" || true
fi

if command -v codesign >/dev/null 2>&1; then
  codesign --force --deep --sign - "dist/$APP_NAME.app" || true
fi

if command -v xattr >/dev/null 2>&1; then
  xattr -dr com.apple.quarantine "dist/$APP_NAME.app" || true
fi

echo "Built: dist/$APP_NAME.app"
