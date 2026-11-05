# SQL test environment configuration

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

The main container uses only two named volumes. Staging temporarily mounts the
input volume writable in a separately labelled helper, without host bind mounts.
Both helpers and the main container have explicit memory/PID bounds and no
external network. Data is retained after failure until collection is verified.

## Privilege and isolation settings

`Docker.arguments` drops all capabilities, then adds CHOWN, DAC_OVERRIDE, FOWNER,
SETUID, SETGID and AUDIT_WRITE. The worker is `alopex`; sudo uses the container's
setuid binary. No-new-privileges is unset. The live probe checks effective and
bounding capability masks, seccomp mode, and the actual LSM label.

The private profile selects an existing seccomp file by digest and an existing
enforcing AppArmor profile or full SELinux context. For SELinux, the context is
translated into Docker's user/role/type/level options. No CLI command changes a
VM policy, installs an LSM policy, grants host privileges, or starts a VM.

Before image qualification, record the selected runtime policy version, the
required syscall/access rules, its applied identity, and successful/denied probe
results in private evidence. Seccomp mode alone is not policy qualification.
Host identity, connection details and filesystem roots stay out of public output.

## Network and database authentication

| Source | Destination | Rule / configuration |
| --- | --- | --- |
| Fetch CLI | HTTPS URLs and redirects in the resolved lock | Exact hashes/sizes; redirect hosts must be allowlisted; system CA validation |
| Docker pull | Registry/auth/CDN endpoints for the locked base | Existing Docker context credentials; record dependency destinations with the lock |
| Build | None | Pre-fetched base and packages, `--network=none`, `--pull=false` |
| Runtime and staging | None externally | `--network=none`, no port publication or host networking |
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
`--no-download`; `--network=none` also applies to the build. This satisfies
Pre-Depends before unpacking packages, which a single `dpkg -i` glob cannot
guarantee. Pin libpq to the same PostgreSQL version as the server and client.

PostgreSQL has `listen_addresses=''`, `fsync=on`, and
`synchronous_commit=on`. The tiny qualification fixtures disable autovacuum and
parallel query workers to keep the observed process set stable. These settings
belong to functional acceptance, not performance comparisons.

## Resource limits and observation

CPU sets, memory/swap, PID and time limits come from the private profile. Docker
limits and affinity are checked against actual container/process settings.
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
R01 creates another container and named volumes, validates both runs, and binds
the result to both collection manifests. Unit-contract cases are labelled
`contract_regression`; live environment acceptance requires the live probes and
all six SQL cells as well.

## Qualification still required

The initial image lock fixes the base index but has no resolved platform entry.
`doctor` and `build` reject it with PREREQUISITE until the dependency closure has
been resolved and its URLs, package versions, sizes and hashes recorded. This
prevents an unverified image from being presented as accepted.

Live image construction, LSM/namespace checks, I/O probes and SQL cells must be
recorded against the eventual image and input identities. Local unit tests do
not establish these results.

References: [Docker volumes](https://docs.docker.com/engine/storage/volumes/),
[Docker resource controls](https://docs.docker.com/engine/containers/resource_constraints/),
[Docker seccomp](https://docs.docker.com/engine/security/seccomp/),
[PostgreSQL initdb](https://www.postgresql.org/docs/16/app-initdb.html),
[PostgreSQL authentication](https://www.postgresql.org/docs/16/auth-pg-hba-conf.html),
[Linux process I/O](https://man7.org/linux/man-pages/man5/proc_pid_io.5.html).
