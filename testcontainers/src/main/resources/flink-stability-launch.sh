#!/usr/bin/env bash
# Explicit custom-config image contract: bash plus the standard /opt/flink layout,
# standard YAML loader and unredacted GlobalConfiguration startup observations.
# This launcher never evaluates FLINK_PROPERTIES or runs an image-specific entrypoint.
set -euo pipefail
export FLINK_HOME=/opt/flink
export FLINK_CONF_DIR=/opt/flink/conf
case "${1:-}" in
  jobmanager|taskmanager) role=$1 ;;
  *) echo 'Unsupported Flink process role' >&2; exit 1 ;;
esac
[[ -f "$FLINK_CONF_DIR/config.yaml" && -x "$FLINK_HOME/bin/$role.sh" ]] || {
  echo 'Custom Flink configuration requires standard config.yaml and executable Flink process scripts' >&2
  exit 1
}

# Preserve the official image's allocator selection without invoking its config parser.
if [[ ${DISABLE_JEMALLOC:-false} == false ]]; then
  allocator="/usr/lib/$(uname -m)-linux-gnu/libjemalloc.so"
  fallback=/usr/lib/x86_64-linux-gnu/libjemalloc.so
  if [[ -f "$allocator" ]]; then
    export LD_PRELOAD="${LD_PRELOAD:-}:$allocator"
  elif [[ -f "$fallback" ]]; then
    export LD_PRELOAD="${LD_PRELOAD:-}:$fallback"
  else
    echo 'jemalloc unavailable; using glibc allocator' >&2
  fi
fi

# Retain built-in plugin setup; the harness's synthetic token plugin is copied separately.
if [[ -n ${ENABLE_BUILT_IN_PLUGINS:-} ]]; then
  IFS=';' read -r -a plugins <<< "$ENABLE_BUILT_IN_PLUGINS"
  for plugin in "${plugins[@]}"; do
    [[ $plugin =~ ^[A-Za-z0-9][A-Za-z0-9._-]*\.jar$ && -f "$FLINK_HOME/opt/$plugin" ]] || {
      echo 'Required built-in Flink plugin is unavailable or has an invalid name' >&2
      exit 1
    }
    mkdir -p "$FLINK_HOME/plugins/${plugin%.jar}"
    ln -fs "$FLINK_HOME/opt/$plugin" "$FLINK_HOME/plugins/${plugin%.jar}/$plugin"
  done
fi
exec "$FLINK_HOME/bin/$role.sh" start-foreground
