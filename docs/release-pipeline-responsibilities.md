# Commit and tag identity release pipeline

## Target invariant and source of truth

The release identity is an exact commit SHA, never branch membership. Development
CI and Extended Verification produce machine-readable evidence for that SHA. An
immutable `vX.Y.Z-rc.N` tag binds the SHA to qualified bytes listed in
`candidate-manifest.json`. A stable `vX.Y.Z` tag may only promote those same
bytes when it peels to the approved RC SHA.

Human-readable Markdown and GitHub summaries are views. They never authorize a
transition. Stable Delivery never rebuilds, retests, or reinterprets software
correctness.

## Failure ownership

| Failure | Owner | Required response |
|---|---|---|
| behavior, execution path, fast resource/performance policy | Development CI | change the commit |
| broad compatibility, stress, durability, statistical performance | Extended Verification | change the commit or versioned policy |
| archive/package/install/manifest/digest qualification | RC Qualification | create a new immutable RC after a fix |
| publication or public reachability | Stable Delivery | retry or repair-forward the same stable release |

## Workflow job inventory

Every job has exactly one responsibility. A workflow trigger does not change the
job's owner.

| Job | Responsibility | Canonical evidence or output |
|---|---|---|
| `alopex-cli.yml:cli-check` | Development CI | exact-SHA CLI compile/lint result |
| `alopex-cli.yml:functional-tests` | Development CI | exact-SHA CLI behavior result |
| `alopex-cli.yml:streaming-fallback-test` | Development CI | exact-SHA execution-path result |
| `alopex-cli.yml:signal-handling-test` | Development CI | exact-SHA process-lifecycle result |
| `alopex-cli.yml:s3-compatibility-test` | Development CI | exact-SHA S3 behavior result |
| `alopex-cli.yml:cli-success` | Development CI | status-only owner join |
| `alopex-performance.yml:measure` | Extended Verification | exact-SHA metrics JSON |
| `alopex-performance.yml:persist` | Extended Verification | immutable metrics history view |
| `alopex-py-release.yml:linux` | RC Qualification | qualified Linux wheels |
| `alopex-py-release.yml:macos` | RC Qualification | qualified macOS wheels |
| `alopex-py-release.yml:windows` | RC Qualification | qualified Windows wheels |
| `alopex-py-release.yml:sdist` | RC Qualification | qualified source distribution |
| `alopex-py-release.yml:publish-testpypi` | RC Qualification | isolated package-index staging |
| `alopex-py-release.yml:verify-testpypi` | RC Qualification | installed candidate smoke result |
| `alopex-py-release.yml:publish-pypi` | Stable Delivery | exact wheel/sdist publication |
| `alopex-py-release.yml:github-release` | Stable Delivery | exact wheel/sdist promotion |
| `alopex-py-release.yml:final-release-join` | Stable Delivery | publication identity join |
| `alopex-py-release.yml:verify-public-release` | Stable Delivery | public package reachability result |
| `alopex-py-release.yml:post-release-hnsw` | Stable Delivery | version snapshot dispatch only |
| `alopex-py.yml:rust-check` | Development CI | exact-SHA binding lint result |
| `alopex-py.yml:test` | Development CI | exact-SHA binding behavior result |
| `alopex-py.yml:polars-test` | Development CI | exact-SHA Polars compatibility result |
| `alopex-py.yml:server-e2e` | Development CI | exact-SHA client/server behavior result |
| `alopex-py.yml:typecheck` | Development CI | exact-SHA public typing result |
| `alopex-py.yml:benchmarks` | Development CI | fast exact-SHA benchmark policy result |
| `alopex-py.yml:ci-success` | Development CI | status-only owner join |
| `ci.yml:scope` | Development CI | exact-SHA change classification |
| `ci.yml:fmt` | Development CI | exact-SHA formatting result |
| `ci.yml:formal` | Development CI | exact-SHA bounded model result |
| `ci.yml:clippy` | Development CI | exact-SHA lint result |
| `ci.yml:test` | Development CI | exact-SHA behavior result |
| `ci.yml:coverage` | Development CI | exact-SHA coverage result |
| `ci.yml:security-audit` | Development CI | exact-SHA dependency policy result |
| `ci.yml:build` | Development CI | exact-SHA cross-platform build result |
| `ci.yml:v08-release-gate` | Development CI | exact-SHA implementation-surface result |
| `ci.yml:ci-success` | Development CI | required status-only join |
| `compatibility.yml:historical-parser` | Extended Verification | historical parser matrix result |
| `compatibility.yml:historical-contract` | Extended Verification | historical API matrix result |
| `compatibility.yml:current-windows-full` | Extended Verification | Windows full-workspace result |
| `compatibility.yml:native` | Development CI | exact-SHA native compatibility result |
| `compatibility.yml:wasm` | Development CI | exact-SHA WASM compatibility result |
| `parity-harness.yml:contract` | Development CI | benchmark harness contract result |
| `parity-performance.yml:performance` | Extended Verification | exact-SHA reference metrics JSON |
| `post-release-hnsw.yml:diagnostic` | Stable Delivery | public-version snapshot only; no release gate |
| `post-release-hnsw.yml:publish` | Stable Delivery | generated version report publication |
| `post-release-hnsw.yml:notify-failure` | Stable Delivery | advisory report failure notification |
| `public-release-verification.yml:verify` | Stable Delivery | package reachability and minimal import |
| `public-release-verification.yml:publish` | Stable Delivery | immutable public run report |
| `public-release-verification.yml:notify-scheduled-failure` | Stable Delivery | scheduled reachability notification |
| `release-process.yml:contract` | Development CI | workflow contract tests |
| `release-process.yml:state-model` | Development CI | legal transition model result |
| `release.yml:ci-gate` | Stable Delivery | approved RC identity check |
| `release.yml:build-release` | RC Qualification | moved to `release-candidate.yml` |
| `release.yml:create-release` | Stable Delivery | exact candidate asset promotion |
| `release.yml:publish-crate` | Stable Delivery | exact crate-byte publication |
| `release.yml:dispatch-python-release` | Stable Delivery | exact Python artifact publication dispatch |
| `stress-tests.yml:stress-tests` | Extended Verification | exact-SHA stress result |
| `stress-tests.yml:sanitizer-lane` | Extended Verification | exact-SHA sanitizer result |
| `stress-tests.yml:fuzz-lane` | Extended Verification | exact-SHA fuzz result |
| `stress-tests.yml:perf-lane` | Extended Verification | exact-SHA resource/performance result |

Rows naming current `release.yml` build responsibilities describe migration
ownership. Those rows disappear when the jobs move to the RC workflow; the
contract test then requires their new workflow-qualified names.

## File and artifact lifecycle

| Path or artifact | Current role | Target role | Action | Delete when | Proof |
|---|---|---|---|---|---|
| `.github/workflows/ci.yml` | named-branch Development CI | branch-independent exact-SHA Development CI | replace | named branch filters are absent | workflow contract test |
| `.github/workflows/release.yml` | build, qualify, and publish stable tag | Stable Delivery only | shrink | build/test commands are absent | workflow contract test and stable dry-run |
| `.github/workflows/release-candidate.yml` | absent | RC artifact build and qualification | replace | stable workflow consumes its manifest | RC dry-run and manifest test |
| `.github/workflows/alopex-py-release.yml` | build and publish together | RC build plus stable promotion jobs | move | stable jobs consume candidate bytes | wheel digest comparison |
| `scripts/release/safe-tag.sh` | main/worktree gate | commit-object and immutable-tag gate | replace | branch checks are absent | `test_safe_tag.sh` |
| `candidate-manifest.json` | absent | canonical qualified artifact inventory | replace | never; retention is release evidence | schema and digest replay tests |
| post-publication functionality demos | first-run correctness evidence | package reachability/minimal import only | shrink | every removed check has a commit-level owner | contract test and dead-path search |

## Operations and rollback

- A failed Development or Extended check requires a new commit.
- A failed RC tag is never moved or recreated; the corrected candidate uses a
  new RC number.
- A stable tag is not created until an approved RC manifest exists for the same
  peeled SHA.
- Stable partial publication uses repair-forward only. No source rebuild or
  qualification rerun may replace already-published bytes.
- Candidate views can be regenerated from `candidate-manifest.json`; Markdown is
  never an input.
