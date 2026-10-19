# Temporary macOS candidate runners

For `integration/v0.8.16-candidate` push runs only, the repository variable
`ALOPEX_V0816_MACOS_RUNNER=self-hosted` routes the macOS CI test/build jobs and
Python test matrix to `alopex-v0816-macos`. Pull requests and other branches
continue to use GitHub-hosted runners. Remove the variable or set it to `hosted`
to restore hosted routing for new runs. Already queued jobs retain their route.

Before enabling this switch, the operator must provision one ephemeral runner
per host, with only the dedicated label, and an admission hook outside the
checkout. The hook must reject any repository, event, branch, workflow, job or
exact commit outside the approved candidate. It writes the admitted commit to
`ALOPEX_RUNNER_ADMITTED_SHA` and the host's single shared Cargo target path to
`ALOPEX_RUNNER_SHARED_TARGET` through `GITHUB_ENV`. It must also provide `ALOPEX_RUNNER_PYTHON`, an absolute
path to a relocatable Python 3.11 executable in the task-owned directory.
The preparation step checks its version and adds its bin directory to
`GITHUB_PATH`; hosted jobs retain `actions/setup-python`.

The operator must inventory all Alopex storage on each physical host, reserve
the expected peak output, and run a watchdog that stops the runner and its
children at 40 GB or on measurement failure. The absolute limit is 50 GB per
host. Serialize builds per host, use two Cargo jobs, disable incremental and
dev/test debug information, and do not restore hosted Cargo caches. The runner
uses a task-owned home/tool cache, not the operator's normal tool environment.

Keep the admission hook, supervisor and logs outside the runner installation
and checkout. After each job, ephemeral registration expires; preserve compact
job/commit evidence and remove obsolete job outputs before registering another
one. After the four checks, restore hosted routing, remove registrations and
temporary tooling, and remove generated build outputs owned by this validation.

This fallback validates native Apple Silicon macOS jobs. Intel macOS release
assets retain their separate `macos-15-intel` workflow and are not qualified by
these ARM jobs. RC publication still requires its separate approval.
