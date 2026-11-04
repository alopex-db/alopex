# Primary Python qualification storage

Primary Linux, Apple Silicon, Windows and sdist qualification use the upstream
[maturin action](https://github.com/PyO3/maturin-action/tree/e83996d129638aa358a18fbd1dfb82f0b0fb5d3b)
at a fixed commit, with maturin 1.14.1. `bounded_maturin.py` checks that checkout
and its entrypoint, then runs its bundled Node 24 entrypoint under the shared
storage watchdog. The existing manylinux2014 Linux container, target selection,
macOS dylib repair and wheel/sdist arguments are retained. Stable publication
continues to promote the qualified bytes without rebuilding them.

`release_storage.py` adds Rust toolchains and the Node, maturin and Python tool
caches to the normal workspace/worktree/cache inventory. On Linux it also
measures allocated bytes in the host Docker and containerd stores using
privileged `du`, including container writable layers. All data in these stores
is counted conservatively; an unfamiliar layout, permission failure, incomplete
measurement or failed measurement aborts verification. Container-local free
space is not used as the physical host budget.

Wheel action execution declares at most 20 GB additional storage. Sdist,
parser staging and isolated wheel installation declare 2 GB each. The shared
watchdog refuses a command whose declared allowance cannot fit below 40 GB and
stops the owned process tree at the allowance or host threshold. Cargo uses one
workspace target, two compilation jobs and no incremental compilation. Primary
jobs have a 90-minute deadline (sdist 30 minutes).

The Linux action uses a run/attempt-specific, labelled container. The wrapper
and an always-run recovery step remove only that container and any manylinux
image absent before this run. Existing images are preserved. Target permissions
are repaired only inside the owned workspace outputs. Cleanup is withheld if
container recovery fails; it must not remove a live build's mounted files.
There is no global Docker prune. Interrupted pulls can leave daemon-managed
cache data: the final host inventory still counts it until this isolated hosted
runner is discarded.

After artifact upload, target directories, parser downloads, tool checkouts
and owned wheel virtual environments are removed. Qualified `dist` bytes and
source/action/image identities, native exit status and storage receipts remain
available. Existing vector/streaming scenarios, macOS SQL tests, archive
inspection and strict parser identity checks remain required. Every native
subprocess exit is checked, including Windows installation and smoke failures.

Tiny process/container fixtures validate orchestration on the existing host
contract matrix. They do not qualify a real wheel, certify an image pull, or
prove a native product build. Those results still belong to the immutable RC
workflow, and both Core and Python resource changes require candidate CI and
integration before that workflow is dispatched.
