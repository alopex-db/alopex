#!/bin/sh
# Download a fixed public release asset; publish only verified bytes.
set -eu
[ "$#" -eq 4 ] || exit 64
url=$1
expected_sha=$2
expected_size=$3
output=$4
case "$url" in https://api.github.com/repos/*/releases/assets/*) ;; *) exit 64 ;; esac
case "$expected_sha" in *[!0-9a-f]*|'') exit 64 ;; esac
[ "${#expected_sha}" -eq 64 ] || exit 64
case "$expected_size" in *[!0-9]*|'') exit 64 ;; esac
[ "$expected_size" -gt 0 ] && [ "$expected_size" -le 16777216 ] || exit 64
temporary=$(mktemp "${output}.part.XXXXXX")
trap 'rm -f "$temporary"' EXIT HUP INT TERM
curl --disable --fail --silent --show-error --location --proto '=https' --proto-redir '=https' \
    --retry 2 --retry-max-time 60 --connect-timeout 10 --max-time 45 \
    --max-filesize "$expected_size" \
    -H 'Accept: application/octet-stream' -H 'X-GitHub-Api-Version: 2022-11-28' \
    "$url" -o "$temporary"
[ "$(wc -c < "$temporary")" -eq "$expected_size" ]
printf '%s  %s\n' "$expected_sha" "$temporary" | sha256sum -c -
mv "$temporary" "$output"
