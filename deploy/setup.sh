#!/bin/bash
# deploy/setup.sh — Install solar-ema-monitor on any Linux system
#
# Usage (from the repo root):
#   bash deploy/setup.sh
#
# What it does:
#   1. Installs Python dependencies
#   2. Creates .env from template if missing (you edit with your credentials)
#   3. Generates systemd units with correct paths/user
#   4. Enables and starts the API + daily sync timer

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CURRENT_USER="$(whoami)"

echo "=== Solar EMA Monitor Setup ==="
echo "  Install dir: $INSTALL_DIR"
echo "  User:        $CURRENT_USER"
echo ""

# --- 1. Python dependencies ---
echo "[1/4] Installing Python packages ..."
pip3 install --user flask pandas requests openpyxl xlrd 2>/dev/null || \
    pip3 install flask pandas requests openpyxl xlrd
echo "  Done."

# --- 2. Configuration (.env) ---
echo ""
echo "[2/4] Checking configuration ..."
if [ ! -f "$INSTALL_DIR/.env" ]; then
    if [ -f "$INSTALL_DIR/.env.template" ]; then
        cp "$INSTALL_DIR/.env.template" "$INSTALL_DIR/.env"
        chmod 600 "$INSTALL_DIR/.env"
        echo "  Created .env from template. EDIT IT NOW:"
        echo "    nano $INSTALL_DIR/.env"
        echo ""
        echo "  You need to fill in EMA_APP_ID and EMA_APP_SECRET at minimum."
        echo "  Press Enter when ready (or Ctrl+C to abort) ..."
        read -r
    else
        echo "  WARNING: No .env.template found. Create .env manually."
    fi
else
    echo "  .env already exists — skipping."
fi

# --- 3. Systemd units ---
echo "[3/4] Installing systemd services ..."

for UNIT in solar-api.service solar-sync.service solar-sync.timer; do
    SRC="$SCRIPT_DIR/$UNIT"
    if [ ! -f "$SRC" ]; then
        echo "  WARNING: $SRC not found, skipping."
        continue
    fi
    # Replace placeholders with actual paths
    sed -e "s|__INSTALL_DIR__|$INSTALL_DIR|g" \
        -e "s|__USER__|$CURRENT_USER|g" \
        "$SRC" | sudo tee "/etc/systemd/system/$UNIT" > /dev/null
    echo "  Installed $UNIT"
done

sudo systemctl daemon-reload

# --- 4. Enable and start ---
echo ""
echo "[4/4] Starting services ..."
sudo systemctl enable --now solar-api
echo "  solar-api started."

sudo systemctl enable --now solar-sync.timer
echo "  solar-sync.timer enabled (daily at 10pm)."

echo ""
echo "=== Setup Complete ==="
echo ""
echo "  Dashboard:  http://$(hostname -I | awk '{print $1}'):8080/"
echo "  API status: curl http://localhost:8080/api/status"
echo "  Sync now:   sudo systemctl start solar-sync"
echo "  Logs:       journalctl -u solar-api -f"
echo "              journalctl -u solar-sync"
echo ""
echo "  To backfill from local XLS/CSV files:"
echo "    cd $INSTALL_DIR && python3 solar_sync.py --backfill"
