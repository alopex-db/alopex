# SQL test environment configuration

## Dedicated VM construction

Choose the guest architecture from the source-bound server/wheel ABI and supply
it as a private VM parameter. Record the runtime versions, guest image identity
and effective configuration alongside the execution profile. The tested
provisioning tools are Colima 0.10.3 and Lima 2.2.1. A QEMU backend also requires
QEMU and Lima's matching additional guest-agent package.

| Setting | Required value / parameter | Verification |
| --- | --- | --- |
| Runtime state, Docker CLI configuration, download cache | Separate private directories, owner-only access; `COLIMA_HOME`, `DOCKER_CONFIG`, `XDG_CACHE_HOME` | Directory modes 0700, configuration files 0600; include all in host storage inventory |
| VM backend and architecture | `VM_TYPE`, `VM_ARCH`; match the product ABI | Effective Lima `vmType`/`arch`, guest executable ELF signature against server/parser |
| VM CPU, memory, data/root disk sizes | `VM_CPUS`, `VM_MEMORY_GIB`, `VM_DATA_DISK_GIB`, `VM_ROOT_DISK_GIB` | Effective VM configuration; include allocated disk bytes in host budget |
| Host filesystem shares | None; `--mount=none` | Effective Lima `mounts` is empty |
| Agent forwarding and global SSH configuration | Disabled | `ssh.forwardAgent=false`; `--ssh-config=false` |
| Application port forwarding and reachable guest address | Disabled | Ignore application port ranges; only private management Unix sockets may be forwarded |
| Guest networking | User-mode outbound networking for acquisition | No bridged interface or published application port; acceptance containers use `network=none` |
| Nested virtualization, binfmt and Rosetta | Disabled | Effective VM configuration and provision script |
| Docker context activation | Disabled | Existing active context remains unchanged |
| Guest sudo and process I/O | Guest-local sudo succeeds; `/proc/self/io` is readable | `sudo -n id -u` returns 0; E02 additionally proves accounting on the DB volume |
| Guest security | Enforcing standard LSM and runtime seccomp | Guest policy state, Docker security options and E01 |

Use new private directories and a new VM profile. Keep runtime paths short
enough for Unix-domain management sockets. Set the variables below from the
private VM configuration, measure the host's complete working storage, and
record a bounded creation allowance and cleanup plan before starting. Include
new emulator installations, package downloads and VM disks in that inventory.

```sh
export COLIMA_HOME DOCKER_CONFIG XDG_CACHE_HOME
umask 077
mkdir -m 700 "$COLIMA_HOME" "$DOCKER_CONFIG" "$XDG_CACHE_HOME"
# Anonymous public-registry acquisition; do not reuse global credentials.
(set -C; printf '%s\n' '{"auths":{"https://index.docker.io/v1/":{}}}' > "$DOCKER_CONFIG/config.json")
colima start "$VM_PROFILE" \
  --runtime=docker --vm-type="$VM_TYPE" --arch="$VM_ARCH" \
  --cpus="$VM_CPUS" --memory="$VM_MEMORY_GIB" \
  --disk="$VM_DATA_DISK_GIB" --root-disk="$VM_ROOT_DISK_GIB" \
  --template=false --mount=none --ssh-agent=false --ssh-config=false \
  --activate=false --port-forwarder=none --network-address=false \
  --nested-virtualization=false --binfmt=false --vz-rosetta=false
colima ssh --profile "$VM_PROFILE" -- sudo -n id -u
colima ssh --profile "$VM_PROFILE" -- test -r /proc/self/io
docker --context "colima-$VM_PROFILE" info --format '{{json .SecurityOptions}}'
```

Check every effective boundary above before creating acquisition or acceptance
containers. Set the execution profile's Docker context to the new context and
its runtime-storage roots to the VM's backing-storage directory. Keep the VM
disk, acquisition cache and container volumes within the same physical-host
budget. Stop an idle validation VM before starting another on the same host.
Select `execution_mode=emulated` if the enclosing VM emulates the product ABI;
only native execution may supply release-performance qualification.

## Ownership and filesystem operations

| Target | Owner and access | Applied by | Verification |
| --- | --- | --- | --- |
| Input volume | root:alopex; directories 0750, files 0640, server 0550; runtime mount read-only | `stage.py`, `Docker.arguments` | E01, E04; input digests before/after |
| Tool code | root-owned, not group/other writable | Dockerfile | E01 and image inspection |
| Sudoers | root:root 0440; alopex may run root commands without a password | Dockerfile, `visudo -cf` | E01 |
| DB data and WAL | alopex, directories 0700/files 0600; create, rename, fsync, delete | `entrypoint.py`, umask 0077 | E04, S01–S03 |
| HOME, TMPDIR, cache, venv | private work volume; alopex | Dockerfile environment, `prepare` | E04, offline pip installation |
| PostgreSQL socket | alopex directory 0700, socket mode 0700 | `postgres` | E05 |
| Private host profiles/results | invoking user; directories 0700/files 0600 | `common.py` | P01, permission regressions |
| Process counters | observer reads same-UID server tree; PID/start ticks/executable identities | `observe.py` | E02, E07 and SQL backend membership |

E04 checks input/tool/work ownership and modes, the root-owned setuid sudo
binary and sudoers, and effective mount flags from `proc/self/mountinfo`.
Input is read-only and executable; work is writable and executable; the mount
containing sudo permits setuid and execution. `/dev/shm` is writable tmpfs with
nosuid/nodev/noexec. HOME, TMPDIR and caches use the private work volume.
The probe reads every input file, executes a tiny work-volume script, and
verifies that another unprivileged UID with no supplementary groups cannot
read the private fixture. A failed sudo invocation cannot pass that denial test.

The main container uses only two named volumes. Staging temporarily mounts the
input volume writable in a separately labelled helper, without host bind mounts.
Both helpers and the main container have explicit memory/PID bounds and no
external network. Data is retained after failure until collection is verified.
DB data, WAL and sockets live under `/work/databases`; logs, configuration and
observations live under `/work/results`. Collection copies the latter. Sparse
or preallocated DB files remain on the owned work volume until verified result
collection permits its deletion, avoiding expansion of WAL capacity in a copy.

## Privilege and isolation settings

`Docker.arguments` drops all capabilities, then adds CHOWN, DAC_OVERRIDE, FOWNER,
SETUID, SETGID and AUDIT_WRITE. The worker is `alopex`; sudo uses the container's
setuid binary. No-new-privileges is unset. The live probe checks effective and
bounding capability masks, seccomp mode, and the actual LSM label.

The private profile selects an existing seccomp file by digest and an existing
enforcing AppArmor profile or full SELinux context. For SELinux, the context is
translated into Docker's user/role/type/level options. The VM and container use
their standard protection policies throughout construction and acceptance.

Before image qualification, record the selected runtime policy version, the
required syscall/access rules, its applied identity, and successful/denied probe
results in private evidence. Seccomp mode alone is not policy qualification.
Host identity, connection details and filesystem roots stay out of public output.

## Network and database authentication

| Source | Destination | Rule / configuration |
| --- | --- | --- |
| Fetch CLI | HTTPS URLs and redirects in the resolved lock | Exact hashes/sizes; redirect hosts must be allowlisted; system CA validation |
| Lock resolver | Ubuntu archive/security/ports mirrors and PGDG archive, HTTPS 443; runtime bridge DNS | Disposable pinned-base container, no host mounts/socket/ports; standard seccomp/LSM, bounded memory/PIDs/time/storage. APT redirects disabled; signature/TLS validation required. |
| Lock resolver CLI | `www.postgresql.org`, `pypi.org`, HTTPS 443; system DNS | SHA256-pinned public PGDG key, exact wheel-version metadata; system CA trust. No credentials passed into the helper. |
| Docker pull | Registry/auth/CDN endpoints for the locked base | Existing Docker context credentials; record dependency destinations with the lock |
| Build | None | Pre-fetched base and packages, `--network=none`, `--pull=false` |
| Runtime and staging | None externally | `--network=none`, no port publication or host networking |
| Sudo hostname lookup | Container loopback | Fixed container alias `alopex-test` in `/etc/hosts`; no DNS lookup for the container's own name |
| SQL client | Alopex IPv4 loopback HTTP; supervisor also checks gRPC/admin listeners | Container-local ports 8080/9090/8081; auth type none; no TLS |
| PostgreSQL bootstrap | Unix socket, postgres DB, alopex role | `local postgres alopex peer` followed by reject rules |
| PostgreSQL acceptance | Unix socket, acceptance DB, alopex role | Replace HBA with `local acceptance alopex peer`, reject the rest, restart |

For public-registry acquisition with no credentials, use a dedicated Docker CLI
configuration with `auths: {"https://index.docker.io/v1/": {}}` and no credential
store or helper. This prevents the CLI from automatically discovering an OS
credential helper. Keep existing Docker configuration unchanged. Private
registries require a separately supplied credential configuration outside the
container.

Offline package installation uses APT's local-file dependency ordering with
empty source and index directories; `--network=none` also applies to the build.
APT may copy local package files into its archive cache. This satisfies
Pre-Depends before unpacking packages, which a single `dpkg -i` glob cannot
guarantee. Pin libpq to the same PostgreSQL version as the server and client.

PostgreSQL has `listen_addresses=''`, `fsync=on`, and
`synchronous_commit=on`. The tiny qualification fixtures disable autovacuum and
parallel query workers to keep the observed process set stable. These settings
belong to functional acceptance, not performance comparisons.

## Resource limits and observation

CPU sets, memory/swap, PID and time limits come from the private profile. Docker
limits and affinity are checked against actual container/process settings.
The runtime uses cgroup v2. E06 reads `memory.max`, `memory.swap.max`,
`pids.max` and `cpuset.cpus.effective`, rejects unlimited or mismatched values,
and checks both the supervisor affinity and separately started server/client
affinity probes. These checks allocate no memory pressure and exhaust no PIDs.
Filesystem roots in the storage inventory must cover the workspace, linked
worktrees, task caches, Docker storage (including VM disk allocation), logs and
collection output. Count each physical host separately. The controller checks
allocated bytes before every growing operation and periodically during it;
unknown usage is a failure. The reserve must cover growth between polls and
termination/collection. This is a watchdog threshold, not a filesystem quota.

The collector records each server process separately, including the PostgreSQL
backend identified by `pg_backend_pid()`. CPU and I/O are interval differences;
RSS is a boundary observation, not an operation peak or an additive total.
PID allocation during the interval, changed tree membership, PID replacement or
reaped-child accounting invalidates a sample. The PID-namespace allocation
counter closes the gap where a short-lived child is absent at both snapshots.
No ptrace capability or cgroup delegation is requested.

E02 writes one 4096-byte fixture to the DB's work volume, fsyncs, and reads its
counter from another live process. Filesystem/accounting incompatibility remains
a failed prerequisite; it is not repaired by substituting another filesystem.

## Run state and evidence

`preparing → running → stopped → collected → deleted` is persisted in `run.json`.
Collection verifies a manifest of copied file hashes before changing state.
Cleanup revalidates that manifest, resource identities, ownership labels and
stopped state. A failed copy or changed resource retains the originals.
The original failure retains its error code when cancellation or reaping also
fails. Secondary failures are recorded separately in private diagnostics and
`cleanup-errors.json`; they remain part of the collected evidence. An incomplete
stop keeps the run recoverable rather than marking it stopped or deleted.
R01 creates another container and named volumes, validates both runs, and binds
the result to both collection manifests. Unit-contract cases are labelled
`contract_regression`; live environment acceptance requires the live probes and
all six SQL cells as well.

## Image lock and qualification

`resolve-lock` fills the selected platform entry in a private copy of the initial
image lock. The pinned base supplies Ubuntu's archive signing keys; the PGDG
public key is additionally pinned by SHA256. APT rejects partial index-update
failures, unsigned repositories and package removals. It resolves against the
base inventory without changing the VM's package state. The host validates
each URI against its signed-index package name, filename, size, version and
SHA256, then freezes the complete installed inventory and dependency file list.
APT's printed MD5 is never used as the lock digest. Resolution runs are owned,
stopped, collected and deleted through the same lifecycle as acceptance runs.

Live image construction, LSM/namespace checks, I/O probes and SQL cells must be
recorded against the eventual image and input identities. Local unit tests do
not establish these results.

References: [Docker volumes](https://docs.docker.com/engine/storage/volumes/),
[Lima VM execution modes](https://lima-vm.io/docs/config/multi-arch/),
[PGDG repository configuration](https://wiki.postgresql.org/wiki/Apt),
[APT archive authentication](https://manpages.debian.org/bookworm/apt/apt-secure.8.en.html),
[Docker resource controls](https://docs.docker.com/engine/containers/resource_constraints/),
[Docker seccomp](https://docs.docker.com/engine/security/seccomp/),
[PostgreSQL initdb](https://www.postgresql.org/docs/16/app-initdb.html),
[PostgreSQL authentication](https://www.postgresql.org/docs/16/auth-pg-hba-conf.html),
[Linux process I/O](https://man7.org/linux/man-pages/man5/proc_pid_io.5.html).
Mount flags follow [proc_pid_mountinfo](https://man7.org/linux/man-pages/man5/proc_pid_mountinfo.5.html);
kernel limit files follow [cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html).
