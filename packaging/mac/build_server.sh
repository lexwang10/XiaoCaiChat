#!/usr/bin/env bash
set -euo pipefail

APP_NAME="XiaoCaiChatServer"
ICON_PNG="icons/ui/server.png"
BUILD_DIR="packaging/mac/build_server"

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

conda install -n xiaocai -c conda-forge -y pyobjc-core pyobjc-framework-Cocoa >/dev/null 2>&1 || true

PYENV_PY="/opt/anaconda3/envs/xiaocai/bin/python"
"$PYENV_PY" -m pip show pyinstaller >/dev/null 2>&1 || conda install -n xiaocai -c conda-forge -y pyinstaller

ADD_DATA_ARGS=(--add-data "icons:icons")
"$PYENV_PY" -m PyInstaller \
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
fi

if command -v codesign >/dev/null 2>&1; then
  codesign --force --deep --sign - "dist/$APP_NAME.app" || true
fi

if command -v xattr >/dev/null 2>&1; then
  xattr -dr com.apple.quarantine "dist/$APP_NAME.app" || true
fi

echo "Built: dist/$APP_NAME.app"
