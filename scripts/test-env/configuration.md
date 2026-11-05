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
Mount requirements apply to the container paths below, independently of the
VM's backing-directory names. `suid`/`dev` mean the absence of `nosuid`/`nodev`.

| Area | Read/write and execution | Setuid and devices | Configuration / check |
| --- | --- | --- | --- |
| Input volume | Read-only, exec | Setuid and device access are not used; either suid/nosuid and dev/nodev are acceptable. Only regular files/directories with the modes above are staged. | `Docker.arguments`: `readonly,volume-nocopy`; E04 records mount flags and rejects writes even through sudo. |
| Work volume, including HOME, TMPDIR, cache, venv, DB and results | Read-write, exec for venv and work scripts | No setuid files or device nodes; either suid/nosuid and dev/nodev are acceptable. Generated-tree inspection rejects special files other than private sockets. | `Docker.arguments`: `volume-nocopy`; E04 plus generated-permission receipts. |
| Root image layer, including sudo, shared libraries, `/tmp` and `/run` | Runtime writable layer, exec; libraries require read access and executable mappings | suid required for sudo. Runtime standard devices only; no device passthrough. | Dockerfile, `Docker.arguments`; E01 sudo, E04 root mount, `validate_container` rejects `Devices`. Application temporary files use TMPDIR on work. |
| Private shared memory | Read-write tmpfs, noexec | nosuid, nodev | `--ipc=private`; E04 requires tmpfs and all four flags on `/dev/shm`. SQL startup verifies DB shared-memory use. |
| Staging helper input/work volumes | Read-write during ownership setup and asset copy | Same named-volume defaults; no device access or setuid execution needed | `envctl.stage`, root helper with the same six-capability bound; runtime input is subsequently mounted read-only. |

No additional tmpfs or host bind mount is created. Named-volume defaults in the
qualified run were suid/dev; these flags grant no extra device attachment or
MKNOD capability. The accepted optional nosuid/nodev restrictions above do not
apply to the root layer, where sudo needs setuid. Docker mount records and
`proc/self/mountinfo` are retained privately, including filesystem type.
The probe reads every input file, executes a tiny work-volume script, and
verifies that another unprivileged UID with no supplementary groups cannot
read the private fixture. A failed sudo invocation cannot pass that denial test.
Extracted harness and venv trees are configured before use: directories and
executables 0700, ordinary files 0600. Venv links may stay within that tree or
bind its bin entry to the image's Python interpreter. SQL cases audit generated
DB and result trees after normal shutdown without rewriting their modes; the
PostgreSQL socket is checked while running. Metadata receipts remain private.

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

The qualified Docker 29.5.2 runtime uses Moby profiles `seccomp/v0.2.3`, SHA256
`536529b665dd0972c37bfb569f5d4ac8a53592e7b00752bc39ff063ca9864c74`, and the
runtime-generated `docker-default` AppArmor profile in enforce mode. The seccomp
default is ERRNO; its permitted file, mapping, UID/GID, signal and wait calls
cover sudo, DB access, observation and child reaping. SYS_ADMIN, SYS_PTRACE and
MKNOD are absent from the capability bound. E01 checks the applied profile and
privileges; E04 checks read-only input and other-user denial; F01/F02 check
signal/wait and recovery. A profile identity or digest mismatch fails POLICY.

Process observation requires `CONFIG_TASK_IO_ACCOUNTING` support and readable
`io`, `stat`, `status`, `exe` and `task/*/children` for the same-UID DB tree in the
private PID namespace. The io/exe access checks use PTRACE_MODE_READ_FSCREDS:
matching credentials, a dumpable target and LSM permission are required without
SYS_PTRACE. DBs and observers run as alopex, with no setuid transition in the DB
tree; sudo is used separately. E02 verifies another live process's actual I/O
on the DB volume, and all SQL cells verify the real DB tree. Missing counters,
hidden proc entries or denied reads fail the prerequisite/measurement; no
proc remount, dumpability override or LSM relaxation is performed.

## Network and database authentication

| Source | Destination | Rule / configuration |
| --- | --- | --- |
| Fetch CLI | `archive.ubuntu.com`, `security.ubuntu.com`, `ports.ubuntu.com`, `apt-archive.postgresql.org`, `files.pythonhosted.org`; HTTPS TCP 443 | Use only the selected lock entry's `allowed_hosts` and exact file URLs; exact hashes/sizes; redirect hosts must be allowlisted; system CA validation. |
| Lock resolver | `archive.ubuntu.com`, `security.ubuntu.com` or `ports.ubuntu.com` from the pinned base's sources; `apt-archive.postgresql.org`; HTTPS TCP 443 | Disposable pinned-base container, no host mounts/socket/ports; standard seccomp/LSM, bounded memory/PIDs/time/storage. APT redirects disabled; signature/TLS validation required. |
| Lock resolver CLI | `www.postgresql.org`, `pypi.org`, HTTPS 443; system DNS | SHA256-pinned public PGDG key, exact wheel-version metadata; system CA trust. No credentials passed into the helper. |
| Docker pull | `registry-1.docker.io`, `auth.docker.io`, `production.cloudfront.docker.com`; HTTPS TCP 443 | Public locked Ubuntu base, anonymous pull token; dedicated Docker configuration, daemon CA trust. Record actual blob redirect host with the base digest. |
| Operation CLI | Private Docker context Unix socket | Invoking user must be able to connect and create/build/inspect/stop/copy/remove owned resources; `Docker.prerequisites` rejects non-Unix endpoints. No TCP API or socket passed into containers. |
| Build | None | Pre-fetched base and packages, `--network=none`, `--pull=false` |
| Runtime and staging | None externally | `--network=none`, no port publication or host networking |
| Sudo hostname lookup | Container loopback | Fixed container alias `alopex-test` in `/etc/hosts`; no DNS lookup for the container's own name |
| SQL client | Alopex IPv4 loopback HTTP; supervisor also checks gRPC/admin listeners | Container-local ports 8080/9090/8081; auth type none; no TLS |
| PostgreSQL bootstrap | Unix socket, postgres DB, alopex role | `local postgres alopex peer` followed by reject rules |
| PostgreSQL acceptance | Unix socket, acceptance DB, alopex role | Replace HBA with `local acceptance alopex peer`, reject the rest, restart |

The resolved dependency lock selects only the hosts needed by its platform;
the table is the supported source set, not an instruction to contact every
host. The qualified base's blob request returned HTTP 307 to the listed CDN;
only its hostname and artifact digests are retained, never signed redirect URLs
or bearer tokens. External services are unreachable during build or acceptance: offline
installation and E05 run with network none.

| Access setting | Applied value / location | Verification |
| --- | --- | --- |
| CLI DNS | Operating-system resolver for the HTTPS destinations above | Successful lock/fetch with TLS verification; retain resolver configuration privately. |
| Resolver-container DNS | Runtime-provided `/etc/resolv.conf` on the default bridge; UDP/TCP 53 to its configured resolver | APT index retrieval succeeds; no `--dns` override or published port. Resolver addresses are private parameters. |
| Runtime DNS | No external route with network none; own hostname resolves through `/etc/hosts` | Docker inspection, E05 failed external connection, successful local listeners. |
| CLI CA and proxy | Python default CA trust and `urllib.request.getproxies()`; direct access when no proxy is configured | PGDG key hash, signed APT indexes and download hashes. No TLS-verification bypass. Keep proxy URLs and credentials private. |
| Resolver CA and proxy | Copy public certificates from `ssl.create_default_context()` into its temporary CA bundle; APT `Acquire::https::CaInfo` uses it. No proxy credentials or proxy environment passed into the helper. | `resolve_image.py`, `resolve_image.sh`; successful authenticated indexes. |
| Docker pull CA and proxy | Daemon trust/proxy configuration, separate from Python CLI settings | Successful pinned-base pull. Dedicated Docker CLI config has no automatic container proxy settings. |
| Acceptance credentials | No cloud/registry/proxy credentials; Alopex loopback has no auth/TLS, PostgreSQL uses peer | Request/asset schema and environment construction; E05 accepts only the intended PostgreSQL DB/role and rejects TCP. |

Direct outbound HTTPS is required for the disposable APT resolver. If the
network requires an authenticated proxy, its configuration is an unresolved
acquisition prerequisite; credentials must not be silently added to image
layers, build arguments or acceptance containers.

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
E06 also runs two independent active-watchdog probes on the work volume. One
publishes a complete, fsynced 16 KiB fixture into a directory with a deliberately
small monitoring threshold. The other removes its empty accounting root after
the child starts. They require CAPACITY/MEASUREMENT respectively, wait for the
terminated child, verify retained data and remove their tiny fixtures. This
tests monitoring failure handling rather than exhausting physical storage.
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
Access checks follow [ptrace permissions](https://man7.org/linux/man-pages/man2/ptrace.2.html).
Acquisition destinations and trust use [Docker registry endpoints](https://docs.docker.com/desktop/enterprise/allow-list/),
[daemon proxies](https://docs.docker.com/engine/daemon/proxy/) and
[container DNS](https://docs.docker.com/engine/network/).
Policy provenance: [Moby seccomp v0.2.3](https://github.com/moby/profiles/blob/seccomp/v0.2.3/seccomp/default.json),
[Docker AppArmor](https://docs.docker.com/engine/security/apparmor/).
Mount flags follow [proc_pid_mountinfo](https://man7.org/linux/man-pages/man5/proc_pid_mountinfo.5.html);
kernel limit files follow [cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html).
