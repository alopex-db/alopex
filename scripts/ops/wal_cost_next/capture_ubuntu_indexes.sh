#!/bin/sh
# Run only inside the admitted, disposable, exact-digest Ubuntu resolver.
# The controller must enforce the selected #804 profile and storage limits.
set -eu
umask 077
export LC_ALL=C

out=/resolution/results
sources=/etc/apt/sources.list.d/ubuntu.sources
keyring=/usr/share/keyrings/ubuntu-archive-keyring.gpg
test "$(dpkg --print-architecture)" = amd64
. /etc/os-release
test "$ID:$VERSION_ID" = ubuntu:24.04
test -r "$sources" && test -r "$keyring"
test -s /resolution/ca.pem
test ! -e /etc/apt/sources.list || test ! -s /etc/apt/sources.list
# A second enabled source could silently alter APT's dependency solution.
test "$(find /etc/apt/sources.list.d -maxdepth 1 -type f \( -name '*.list' -o -name '*.sources' \) | wc -l | tr -d ' ')" = 1
test "$(find /etc/apt/sources.list.d -maxdepth 1 -type f \( -name '*.list' -o -name '*.sources' \) -print)" = "$sources"
awk '
  /^URIs: / { if ($0 !~ /^URIs: https?:\/\/(archive|security)\.ubuntu\.com\/ubuntu$/) exit 1; u++ }
  /^Signed-By: / { if ($0 != "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg") exit 1; k++ }
  /^Types: / { if ($0 != "Types: deb") exit 1; t++ }
  /^Suites: / { if ($0 !~ /^Suites: noble(-updates|-security)?( noble(-updates|-security)?)*$/) exit 1; s++ }
  END { if (!u || u != k || u != t || u != s) exit 1 }
' "$sources"
test ! -e "$out"
mkdir -p "$out/lists/partial" "$out/archives/partial"
chmod 755 "$out" "$out/lists" "$out/lists/partial" "$out/archives" "$out/archives/partial"
chown _apt:root "$out/lists/partial" "$out/archives/partial"
sed 's|http://|https://|g' "$sources" > "$out/ubuntu.sources"
chmod 644 "$out/ubuntu.sources"
cat > "$out/apt.conf" <<EOF
Dir::Etc::sourcelist "$out/ubuntu.sources";
Dir::Etc::sourceparts "-";
Dir::State::lists "$out/lists";
Dir::Cache::archives "$out/archives";
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
chmod 644 "$out/apt.conf"
sha256sum "$0" > "$out/resolver.sha256"
dpkg-query -W -f='${Package}\t${Version}\n' > "$out/base.tsv"
cp "$keyring" "$out/trusted-keyring.gpg"
sha256sum "$sources" "$keyring" > "$out/source-keyring.sha256"
apt-get -c "$out/apt.conf" update > "$out/apt-update.log" 2>&1
apt-get -c "$out/apt.conf" indextargets > "$out/indextargets.raw"
mkdir "$out/packages"
awk '/^Filename: / {print substr($0, 11)}' "$out/indextargets.raw" | sort -u | while IFS= read -r target; do
    case "$target" in "$out/lists/"*) ;; *) exit 1 ;; esac
    test -f "$target"
    name=${target##*/}
    case "$name" in *[!A-Za-z0-9_.-]*|'') exit 1 ;; esac
    /usr/lib/apt/apt-helper cat-file "$target" > "$out/packages/$name"
done
apt-get -c "$out/apt.conf" --no-install-recommends --no-remove -y --print-uris install \
    gcc make gawk libc6-dev linux-libc-dev binutils > "$out/apt-raw.txt" 2>&1
awk 'substr($0,1,1)=="\047" {print}' "$out/apt-raw.txt" > "$out/apt-uris.txt"
test -s "$out/apt-uris.txt"
# Preserve every original index and signature. Decompression and proof creation
# are separate offline steps; no .deb is fetched by this script.
find "$out/lists" -maxdepth 1 -name '*InRelease' -type f -exec sh -c '
  for f do gpgv --status-fd 1 --keyring /usr/share/keyrings/ubuntu-archive-keyring.gpg "$f" > "$f.gpgv-status" 2> "$f.gpgv-stderr" || exit 1; done
' sh {} +
test "$(find "$out/lists" -maxdepth 1 -name '*InRelease' -type f | wc -l | tr -d ' ')" -gt 0
printf '%s\n' capture-complete > "$out/capture.status"
