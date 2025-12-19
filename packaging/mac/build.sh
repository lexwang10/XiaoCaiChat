#!/usr/bin/env bash
set -euo pipefail

APP_NAME="XiaoCaiChat"
ICON_PNG="icons/ui/xiaocaichat.png"
BUILD_DIR="packaging/mac/build"
ENV_NAME="${ENV_NAME:-xiaocaichat}"

mkdir -p "$BUILD_DIR"

# Extract app version from source to keep bundle version in sync
# Fallback to 0.0.0 if not found
APP_VERSION_STR="$(awk -F '\"' '/^[[:space:]]*APP_VERSION[[:space:]]*=/ {print $2; exit}' qt_chat_client.py)"
if [ -z \"$APP_VERSION_STR\" ]; then APP_VERSION_STR=\"0.0.0\"; fi
export APP_VERSION="$APP_VERSION_STR"

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

conda run -n "$ENV_NAME" python -m pip install -r requirements.txt >/dev/null 2>&1 || true

# Build app
ADD_DATA_ARGS=(--add-data "icons:icons" --add-data "themes:themes")
if [ -f client_config.json ]; then
  ADD_DATA_ARGS+=(--add-data "client_config.json:.")
fi

conda run -n "$ENV_NAME" python -m PyInstaller \
  --noconfirm \
  --windowed \
  --name "$APP_NAME" \
  --icon "$BUILD_DIR/xiaocaichat.icns" \
  --osx-bundle-identifier "com.xiaocai.chat" \
  --hidden-import "Cocoa" \
  --hidden-import "UserNotifications" \
  --hidden-import "PySide6" \
  --hidden-import "PySide6.QtCore" \
  --hidden-import "PySide6.QtGui" \
  --hidden-import "PySide6.QtWidgets" \
  --hidden-import "shiboken6" \
  "${ADD_DATA_ARGS[@]}" \
  qt_chat_client.py

echo "App built at dist/$APP_NAME.app"
echo "Embedding version into Info.plist: $APP_VERSION"
if [ -f "dist/$APP_NAME.app/Contents/Info.plist" ]; then
  /usr/bin/plutil -replace CFBundleShortVersionString -string "$APP_VERSION" "dist/$APP_NAME.app/Contents/Info.plist" || true
  /usr/bin/plutil -replace CFBundleVersion -string "$APP_VERSION" "dist/$APP_NAME.app/Contents/Info.plist" || true
else
  echo "Warning: Info.plist not found at dist/$APP_NAME.app/Contents/Info.plist" >&2
fi

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
