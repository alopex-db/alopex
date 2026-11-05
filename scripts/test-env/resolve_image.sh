#!/bin/sh
# Runs only in the pinned base's disposable acquisition container.
set -eu
umask 077
export LC_ALL=C
. /etc/os-release
test "$ID:$VERSION_ID" = ubuntu:24.04
out=/work/results
mkdir -p "$out/metadata" /resolution/archives/partial
dpkg-query -W -f='${Package}\t${Version}\n' > "$out/base.tsv"

# The base supplies Ubuntu's signing keys and architecture-specific sources.
# TLS uses the invoking controller's CA trust; no credentials are copied.
sed 's|http://|https://|g' /etc/apt/sources.list.d/ubuntu.sources > /resolution/ubuntu.sources
cat /resolution/ubuntu.sources > /resolution/sources.sources
cat >> /resolution/sources.sources <<'EOF'

Types: deb
URIs: https://apt-archive.postgresql.org/pub/repos/apt
Suites: noble-pgdg-archive
Components: main
Signed-By: /resolution/pgdg.asc
EOF
cat > /resolution/apt.conf <<'EOF'
Dir::Etc::sourcelist "/resolution/sources.sources";
Dir::Etc::sourceparts "-";
Dir::Cache::archives "/resolution/archives";
Acquire::Languages "none";
Acquire::Retries "0";
Acquire::https::Timeout "30";
Acquire::https::CaInfo "/resolution/ca.pem";
Acquire::https::AllowRedirect "false";
Acquire::AllowInsecureRepositories "false";
Acquire::AllowDowngradeToInsecureRepositories "false";
APT::Get::AllowUnauthenticated "false";
APT::Update::Error-Mode "any";
EOF
# No cached index or cached .deb may hide a missing repository or dependency.
rm -rf /var/lib/apt/lists/*
apt-get -c /resolution/apt.conf update
apt-get -c /resolution/apt.conf --no-install-recommends --no-remove -y --print-uris install \
    python3.12 python3.12-venv python3-pip \
    postgresql-16=16.14-1.pgdg24.04+1 \
    postgresql-client-16=16.14-1.pgdg24.04+1 libpq5=16.14-1.pgdg24.04+1 \
    sudo ca-certificates procps util-linux iproute2 > "$out/uris.txt"
awk 'substr($0,1,1)=="\047" {print $2}' "$out/uris.txt" | while read -r filename; do
    package=${filename%%_*}
    printf '%s\n' "$package" | grep -Eq '^[a-z0-9][a-z0-9+.-]*$'
    apt-cache -c /resolution/apt.conf show "$package" > "$out/metadata/$package.control"
done
cp /resolution/sources.sources "$out/sources.sources"
cp /var/lib/apt/lists/*InRelease "$out/metadata/"
printf '%s\n' '{"status":"passed","cases":[],"scope":"image_dependency_resolution"}' > "$out/result.json"
