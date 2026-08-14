# Historical patch release workflow

Use this path when a supported older release line needs a patch after `main`
has advanced to a newer minor version. Do not merge or reset `main` back to the
old line, and do not tag an unpushed local commit.

## 1. Create an isolated release branch

Choose the last published Rust tag in the target line explicitly. For example:

```bash
git fetch origin --tags
git worktree add ../alopex-worktrees/v077-release \
  -b release/v0.7.7 v0.7.6
```

The branch name must exactly match `release/vX.Y.Z`. Backport only the fixes and
release-process changes required by that patch. Update every product crate,
`Cargo.lock`, and `CHANGELOG.md` to `X.Y.Z`.

Pin toolchains and external dependencies to exact released versions. A sibling
checkout or an absolute/relative host path is not a release dependency.

## 2. Run the target-version gate locally

Run the gate belonging to the target release line, not the current `main` gate.
For v0.7.x:

```bash
scripts/check-rust-cache-budget.sh --check
bash scripts/release/v07_gate.sh
scripts/cleanup-generated-artifacts.sh --force
scripts/check-rust-cache-budget.sh --check
```

The target gate must cover all product crates, CLI, cluster/distributed reads,
DataFrame, and the installed Python interface. Pseudo-terminal TUI integration
tests stay in their dedicated CI lane instead of being mixed into a headless
release job. Record unavailable OS or physical environment checks explicitly;
they remain required in the tag workflow.

## 3. Publish the exact release candidate branch

Commit the complete release candidate and push it before invoking CI/CD:

```bash
git push -u origin release/v0.7.7
bash scripts/release/safe-tag.sh v0.7.7 --maintenance-base v0.7.6
```

`safe-tag.sh` rejects the operation unless all of these are true:

- the tree is clean and versions match the tag;
- the branch is exactly `release/vX.Y.Z`;
- local HEAD equals `origin/release/vX.Y.Z`;
- the explicit base is a lower patch in the same major/minor line;
- that base is both the nearest same-line release ancestor and the same commit
  as the remote tag;
- the target tag does not already exist locally or remotely.

The local `safe-tag.sh` invocation is a non-publishing preflight. Do not create
the tag locally.

Before dispatching, a repository administrator must allow the exact maintenance
branch in both the `testpypi` and `pypi` GitHub Environments. Keep the existing
`alopex-py-v*` tag policy and add `release/v0.7.7` as a branch policy. Do not add
a broad `release/v*` production-deployment policy: a typo or an unreviewed future
maintenance branch would then inherit publishing authority. Verify the two
exact policies before starting the workflow:

```bash
gh api repos/alopex-db/alopex/environments/testpypi/deployment-branch-policies
gh api repos/alopex-db/alopex/environments/pypi/deployment-branch-policies
```

Dispatch the `Release` workflow on `release/v0.7.7` with:

- `version`: `0.7.7`
- `maintenance_base`: `v0.7.6`
- `publish_python`: `true`

The GitHub Actions workflow repeats the source checks, runs the target-version
gate, and builds all supported OS artifacts against one recorded commit SHA.
Only after those jobs pass does it revalidate that SHA, create and push the
annotated Rust tag, create the GitHub Release, and publish Rust crates in
dependency order. A tag pushed with the workflow token does not recursively
start another release run.

## 4. Publish Python through the chained workflow

After crates.io publication succeeds, the Rust workflow dispatches
`alopex-py-release.yml` against the same maintenance branch. That workflow
repeats the branch/base/SHA checks, builds wheels for every declared platform,
and installs and exercises each wheel. Only then does it create the annotated
`alopex-py-vX.Y.Z` tag, verify TestPyPI, and publish to PyPI. The independent tag
trigger remains available for normal releases, but historical patches use this
chained CI/CD route.

If an Environment rejects the maintenance branch, the workflow must stop before
the corresponding registry publish. Add only the exact `release/vX.Y.Z` branch
policy, then use **Re-run failed jobs** on the same Actions run so the already
validated artifacts and source SHA remain the release inputs. Do not publish
locally as a workaround.

## 5. Verify published state directly

Do not infer completion from a green workflow alone. Confirm:

- both remote tag commit SHAs equal the release candidate commit;
- GitHub Release assets exist for the supported platforms;
- every Rust crate reports exactly `X.Y.Z` on crates.io;
- PyPI reports exactly `X.Y.Z` and a clean installation passes the installed API
  smoke test;
- milestone issues are closed only after their public artifacts are verified.

After all direct checks pass, remove the temporary exact maintenance-branch
policies from the `testpypi` and `pypi` Environments. The immutable tag policies
remain in place, and the next historical patch must explicitly authorize its own
exact branch before CI/CD starts.

Registry versions and tags are immutable. If publication partially succeeds,
fix the workflow and release the next patch version; never move or overwrite a
published tag/version.
