#!/usr/bin/env bash
# Regression fixture: process_flink_properties from the Apache Flink 2.2.0 image.
# The sink captures parser arguments; envsubst is limited to the test placeholder.
set_config_options() {
  while [[ $# -gt 0 ]]; do printf '%s=%s\n' "$1" "$2"; shift 2; done
}
envsubst() {
  local input
  IFS= read -r input || true
  printf '%s\n' "${input//\$PLACEHOLDER/expanded}"
}
process_flink_properties() {
    local flink_properties_content=$1
    local config_options=()
    local OLD_IFS="$IFS"
    IFS=$'\n'
    for prop in $flink_properties_content; do
        prop=$(echo $prop | tr -d '[:space:]')
        if [ -z "$prop" ]; then continue; fi
        IFS=':' read -r key value <<< "$prop"
        value=$(echo $value | envsubst)
        config_options+=("$key" "$value")
    done
    IFS="$OLD_IFS"
    if [ ${#config_options[@]} -ne 0 ]; then set_config_options "${config_options[@]}"; fi
}
process_flink_properties "$1"
