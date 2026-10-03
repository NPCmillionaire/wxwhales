#!/bin/zsh
# Builds wxwhales.app in ~/Applications. Double-click it to start the dashboard.
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
PY="$(command -v python3)"
APP="$HOME/Applications/wxwhales.app"
[ -n "$PY" ] || { echo "python3 not found"; exit 1; }

rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources" "$DIR/data"

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>CFBundleName</key><string>wxwhales</string>
  <key>CFBundleDisplayName</key><string>wxwhales</string>
  <key>CFBundleIdentifier</key><string>com.cloudsculpting.wxwhales</string>
  <key>CFBundleVersion</key><string>1.0</string>
  <key>CFBundleShortVersionString</key><string>1.0</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleExecutable</key><string>wxwhales</string>
  <key>CFBundleIconFile</key><string>wxwhales</string>
  <key>LSUIElement</key><true/>
</dict></plist>
PLIST

# Launcher: start the server detached (or just open the browser if it's already up), then exit.
cat > "$APP/Contents/MacOS/wxwhales" <<LAUNCH
#!/bin/zsh
cd "$DIR"
if curl -s -m 2 http://127.0.0.1:8765/api/status >/dev/null; then
  open http://127.0.0.1:8765/
else
  nohup "$PY" server.py >> "$DIR/data/server.log" 2>&1 &
fi
LAUNCH
chmod +x "$APP/Contents/MacOS/wxwhales"

# Icon
ICONSET="$(mktemp -d)/wxwhales.iconset"; mkdir -p "$ICONSET"
for s in 16 32 128 256 512; do
  sips -z $s $s "$DIR/app/icon.png" --out "$ICONSET/icon_${s}x${s}.png" >/dev/null
  sips -z $((s*2)) $((s*2)) "$DIR/app/icon.png" --out "$ICONSET/icon_${s}x${s}@2x.png" >/dev/null
done
iconutil -c icns "$ICONSET" -o "$APP/Contents/Resources/wxwhales.icns"
touch "$APP"
echo "Built $APP (python: $PY)"
