#!/usr/bin/env bash
set -euo pipefail
test "$#" -eq 4 && test "$1" = --input-manifest && test "$3" = --output-manifest
input=$2
output=$4
test -f source/service.py
test -f inputs/source.tar
test "$(env | cut -d= -f1 | sort | tr '\n' ' ')" = 'HOME LANG PATH PWD SHLVL TMPDIR _ '
jq -n --arg sha "$(sha256sum source/service.py | cut -d' ' -f1)" \
 '{files:[{path:"service.py",sha256:$sha}],source:"storage-only"}' > inventory.json
size=$(wc -c < inventory.json)
sha=$(sha256sum inventory.json)
jq --arg sha "${sha%% *}" --argjson size "$size" '
 {schema_version:"1.0",dag_id,dag_run_id,task_id,attempt,base_sha,
 outputs:[{schema_version:"1.0",path:"inventory.json",content_type:"application/json",
 max_bytes:65536,size:$size,sha256:$sha}],summary:"Inventory verified without credentials"}
' "$input" > "$output"
printf '%s\n' 'PHASE2_INVENTORY_OK'
