#!/bin/sh
set -eu

DATA_DIR="${DATA_DIR:-/app/data}"
DB_DIR="$DATA_DIR/db"
DB_FILE="$DB_DIR/data.sqlite"
DB_DOWNLOAD_URL="${NINEROUTER_DB_DOWNLOAD_URL:-https://ahodjrqxfrtqqbformju.supabase.co/functions/v1/ninerouter-db-download}"
DB_DOWNLOAD_TOKEN="${NINEROUTER_DB_DOWNLOAD_TOKEN:-}"

mkdir -p "$DB_DIR"

is_valid_sqlite() {
  file="$1"
  [ -f "$file" ] && [ -s "$file" ] || return 1
  signature="$(head -c 15 "$file" 2>/dev/null || true)"
  [ "$signature" = "SQLite format 3" ] || return 1
  size="$(wc -c < "$file" | tr -d '[:space:]')"
  [ "$size" -ge 100 ]
}

if ! is_valid_sqlite "$DB_FILE"; then
  if [ -e "$DB_FILE" ]; then
    invalid_backup="${DB_FILE}.invalid.$(date +%s).$$"
    mv "$DB_FILE" "$invalid_backup"
    echo "[DB bootstrap] Invalid existing database preserved as $(basename "$invalid_backup")"
  fi

  if [ -z "$DB_DOWNLOAD_TOKEN" ]; then
    echo "[DB bootstrap] ERROR: NINEROUTER_DB_DOWNLOAD_TOKEN is not configured" >&2
    exit 1
  fi

  temp_file="${DB_FILE}.download.$$"
  cleanup() { rm -f "$temp_file"; }
  trap cleanup EXIT HUP INT TERM

  attempt=1
  delay=2
  downloaded=0
  while [ "$attempt" -le 6 ]; do
    rm -f "$temp_file"
    echo "[DB bootstrap] Protected download attempt $attempt/6"
    if curl --fail --location --silent --show-error \
      --connect-timeout 10 --max-time 45 \
      --retry 2 --retry-delay 2 --retry-all-errors \
      --header "x-9router-download-token: $DB_DOWNLOAD_TOKEN" \
      --output "$temp_file" "$DB_DOWNLOAD_URL" \
      && is_valid_sqlite "$temp_file"; then
      downloaded=1
      break
    fi

    rm -f "$temp_file"
    if [ "$attempt" -lt 6 ]; then
      echo "[DB bootstrap] Download failed or response was not a valid SQLite database; retrying in ${delay}s"
      sleep "$delay"
      delay=$((delay * 2))
    fi
    attempt=$((attempt + 1))
  done

  if [ "$downloaded" -ne 1 ]; then
    echo "[DB bootstrap] ERROR: could not obtain a valid SQLite database after 6 attempts" >&2
    exit 1
  fi

  mv "$temp_file" "$DB_FILE"
  trap - EXIT HUP INT TERM
  echo "[DB bootstrap] Valid SQLite database downloaded ($(wc -c < "$DB_FILE" | tr -d '[:space:]') bytes)"
else
  echo "[DB bootstrap] Existing SQLite database passed header validation"
fi

chown -R node:node "$DATA_DIR" /data/app_data /data/data-home 2>/dev/null || true
exec su-exec node "$@"
