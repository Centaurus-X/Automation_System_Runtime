#!/usr/bin/env bash
set -euo pipefail

# dirstruct.sh – vollständige Verzeichnisstruktur inkl. Dateien (mit Endungen)
# Usage:
#   ./dirstruct.sh [ROOT] [--all] [--max-depth N] [--out FILE]
#
# Beispiele:
#   ./dirstruct.sh .
#   ./dirstruct.sh . --all
#   ./dirstruct.sh . --max-depth 6
#   ./dirstruct.sh . --out verzeichnisstruktur.txt

root="."
show_all="0"
max_depth=""
out_file=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --all)
      show_all="1"
      shift
      ;;
    --max-depth)
      shift
      max_depth="${1:-}"
      if [ -z "$max_depth" ]; then
        echo "ERROR: --max-depth benötigt eine Zahl" >&2
        exit 2
      fi
      shift
      ;;
    --out)
      shift
      out_file="${1:-}"
      if [ -z "$out_file" ]; then
        echo "ERROR: --out benötigt einen Dateinamen" >&2
        exit 2
      fi
      shift
      ;;
    -*)
      echo "ERROR: Unbekannte Option: $1" >&2
      exit 2
      ;;
    *)
      root="$1"
      shift
      ;;
  esac
done

if [ ! -d "$root" ]; then
  echo "ERROR: ROOT ist kein Verzeichnis: $root" >&2
  exit 2
fi

# Find-Filter: standardmäßig versteckte Pfade ausschließen (.*), mit --all erlauben.
# Zusätzlich: typische Noise-Dirs ausschließen (optional, lässt sich leicht anpassen).
exclude_expr='
  ( -path "*/.git/*" -o -path "*/.venv/*" -o -path "*/__pycache__/*" -o -path "*/node_modules/*" )
'

base_find=(find "$root" -mindepth 1)

if [ -n "$max_depth" ]; then
  base_find+=(-maxdepth "$max_depth")
fi

if [ "$show_all" = "0" ]; then
  # Keine versteckten Pfade
  base_find+=(-not -path "*/.*")
fi

# Noise-Dirs rausfiltern
base_find+=(-not \( $exclude_expr \))

# Ausgabe: Sortiert, mit Root-Header
{
  printf "%s\n" "${root%/}/"
  "${base_find[@]}" -print | LC_ALL=C sort | while IFS= read -r p; do
    # Prefix entfernen, damit es wie ein Baum wirkt
    rel="${p#"$root"/}"

    # Einrückung über Slash-Anzahl (Depth)
    # Depth = Anzahl "/" in rel
    depth="${rel//[^\/]/}"
    indent_len="${#depth}"

    indent=""
    i=0
    while [ "$i" -lt "$indent_len" ]; do
      indent="${indent}│   "
      i=$((i+1))
    done

    name="${rel##*/}"
    if [ -d "$p" ]; then
      printf "%s├─ %s/\n" "$indent" "$name"
    else
      printf "%s├─ %s\n" "$indent" "$name"
    fi
  done
} | {
  if [ -n "$out_file" ]; then
    cat > "$out_file"
    echo "OK: geschrieben nach $out_file" >&2
  else
    cat
  fi
}
