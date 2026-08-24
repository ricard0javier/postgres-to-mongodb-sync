#!/bin/sh
set -eu

register() {
  name="$1"
  config="/connectors/$1.json"

  printf 'Registering %s\n' "$name"
  curl --fail-with-body --silent --show-error \
    --request PUT "http://debezium:8083/connectors/$name/config" \
    --header "Content-Type: application/json" \
    --data "@$config"
}

register postgres-source
register mongodb-sink
