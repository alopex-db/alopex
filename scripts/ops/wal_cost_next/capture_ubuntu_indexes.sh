#!/bin/sh
# Run only inside the admitted, disposable, exact-digest Ubuntu resolver.
# The controller must enforce the selected #804 profile and storage limits.
set -eu
umask 077
export LC_ALL=C

out=/resolution/results
legacy=/etc/apt/sources.list
sources=/etc/apt/sources.list.d/ubuntu.sources
keyring=/usr/share/keyrings/ubuntu-archive-keyring.gpg
test "$(dpkg --print-architecture)" = amd64
. /etc/os-release
test "$ID:$VERSION_ID" = ubuntu:24.04
test -r "$legacy" && test -r "$sources" && test -r "$keyring"
test -s /resolution/ca.pem
test "$(sha256sum "$legacy" | awk '{print $1}')" = 8ce6407d5487bed332b5fcb4aab26dacf3f516d811c26792efb0c67f06f1958b
test "$(sha256sum "$sources" | awk '{print $1}')" = 9a4af756e974a7a6b0a447705d1d12366cfa4d860d8722b652d69d8e1b44e34f
# The pinned image keeps a comment-only migration notice in sources.list.
awk '/^[[:space:]]*($|#)/ { next } { exit 1 }' "$legacy"
# A second enabled source could silently alter APT's dependency solution.
test "$(find /etc/apt/sources.list.d -maxdepth 1 -type f \( -name '*.list' -o -name '*.sources' \) | wc -l | tr -d ' ')" = 1
test "$(find /etc/apt/sources.list.d -maxdepth 1 -type f \( -name '*.list' -o -name '*.sources' \) -print)" = "$sources"
test ! -e "$out"
mkdir -p "$out/lists/partial" "$out/archives/partial"
chmod 755 "$out" "$out/lists" "$out/lists/partial" "$out/archives" "$out/archives/partial"
chown _apt:root "$out/lists/partial" "$out/archives/partial"
cat > "$out/ubuntu.sources" <<'EOF'
Types: deb
URIs: https://archive.ubuntu.com/ubuntu
Suites: noble noble-updates
Components: main universe restricted multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg

Types: deb
URIs: https://security.ubuntu.com/ubuntu
Suites: noble-security
Components: main universe restricted multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
EOF
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
sha256sum "$legacy" "$sources" "$keyring" > "$out/source-keyring.sha256"
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
