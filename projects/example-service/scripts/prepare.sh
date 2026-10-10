#!/usr/bin/env bash
set -euo pipefail
test "$#" -eq 4 && test "$1" = --input-manifest && test "$3" = --output-manifest
input=$2
output=$4
tar --format=ustar -cf source.tar -C source service.py
size=$(wc -c < source.tar)
sha=$(sha256sum source.tar)
jq --arg sha "${sha%% *}" --argjson size "$size" '
 {schema_version:"1.0",dag_id,dag_run_id,task_id,attempt,base_sha,
 outputs:[{schema_version:"1.0",path:"source.tar",content_type:"application/x-tar",
 max_bytes:1048576,size:$size,sha256:$sha}],summary:"Prepared synthetic source snapshot"}
' "$input" > "$output"
printf '%s\n' 'PHASE2_PREPARE_OK'
