# Optional Intel delivery and qualification

Issue #686 requires Linux x86_64, Windows x86_64, and Apple Silicon to complete
v0.8.17 delivery without waiting for Intel Mac. Intel remains a supported target.
The parser vendor manifest now permits those three required targets alone or
all four supported targets. It still rejects each missing required target,
duplicate/unsorted targets, unsupported targets, and invalid asset identities.
The target record schema, source/input checks, native library hashes, sizes,
and canonical archive requirements are unchanged.

`parser_asset_manifest.py assemble-manifest`, `verify-manifest`, and
`release-envelope` accept the primary set. Rust `build_support.rs` applies the
same membership/order policy before selecting the target's asset. An Intel
record, when included, is checked exactly like every other supported record.
No existing published crate, tag, or manifest is modified by this source change.

## Strict resolver boundary

An explicit `NIM_SQL_PARSER_LIB_DIR` remains bound to the crate's pinned vendor
manifest. An Intel directory alone fails when the primary manifest omits Intel.
The additional path requires `PARSER_SUPPLEMENT.json` in that directory and an
explicit `ALOPEX_NIM_PARSER_SUPPLEMENT_SHA256` pin obtained from a reviewed
release record. The resolver checks that pin, the primary manifest digest, the
matching stable tag and source SHA format, target, parser contract, common
source/dependency/compiler inputs, and both native library hashes and sizes.
Existing primary assets always take precedence; a supplement cannot override
them. A missing supplement, wrong pin, or corrupt asset fails closed.
The development-only local-build opt-in remains separate from release asset
qualification. No implicit network download or fallback build is introduced.

The current tests use small synthetic dynamic/static payloads to prove identity
validation. They do not claim an executable Intel parser has been built or run.
Actual native smoke and crate compilation belong to package qualification.

## Explicit offline installation

`scripts/release/parser_supplement.py` provides `create` and `install` commands.
It performs no network access. The publication workflow must first qualify the
native Intel build from the stable tag's peeled commit and record its evidence.
The envelope pin authenticates the reviewed source declaration; the utility
does not query GitHub to prove that the tag currently points to that commit.
Neither these utilities nor the resolver interpret a declaration as proof of
native execution.

During qualification, create a separate supplement from the frozen primary
manifest, its pinned release envelope, and the Intel target record/archive:

```sh
python3 scripts/release/parser_supplement.py create \
  --primary primary.json --primary-sha256 "$PRIMARY_SHA256" \
  --envelope release-envelope.json --envelope-sha256 "$ENVELOPE_SHA256" \
  --record intel-target.json --archive intel-parser.tar.gz \
  --output PARSER_SUPPLEMENT.json
```

The primary digest must match the one compiled into the crate's resolver. The
envelope must bind those primary bytes and the same stable release identity.
The Intel target record must match the primary source and dependency identities.
Archive verification reuses the canonical packing, member allowlist, bounded
decompression, build identity, hash and size checks used by primary assets.
Creation refuses an existing output. Publish the resulting supplement digest
with the reviewed native qualification evidence, separately from the frozen
primary manifest.

After obtaining the archive and supplement, use the reviewed supplement digest
to install into a fresh directory:

```sh
python3 scripts/release/parser_supplement.py install \
  --primary primary.json --primary-sha256 "$PRIMARY_SHA256" \
  --supplement PARSER_SUPPLEMENT.json --supplement-sha256 "$SUPPLEMENT_SHA256" \
  --archive intel-parser.tar.gz --destination ./intel-parser
NIM_SQL_PARSER_LIB_DIR="$PWD/intel-parser" \
ALOPEX_NIM_PARSER_SUPPLEMENT_SHA256="$SUPPLEMENT_SHA256" \
  cargo build --locked
```

Obtain the trusted pins from the release record, independently of the files
being checked. Computing a pin from an untrusted download only demonstrates
self-consistency. Keep `ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD` unset for this path.
The installer verifies a bounded snapshot of the archive, writes only its three
declared members plus contract/checksum sidecars, and copies the supplement last.
It exclusively creates the destination, refuses existing directories and
symlinks, and removes its own partial destination on a copy failure. The resolver
rejects partial installations. Retry with a fresh destination; do not replace a
published crate's primary manifest or any installed primary assets.

This path is available only in crates that contain the supplemental resolver.
It does not retrofit older published crates. Automated acquisition, real Intel
consumer qualification and independent publication remain required below.

## Independent Intel run

Starting with v0.8.17, the Core RC build matrix and primary Python qualification
contain Linux x86_64, Windows x86_64, and Apple Silicon only. All three remain
mandatory, including their parser records and Python wheels plus the sdist.
The Core/Python public join accepts the primary parser inventory while preserving
the four-target rule for older versions. An Intel job is never a dependency of
these primary jobs.

Intel delivery runs separately in the existing `alopex-py-release.yml`, using
`mode=intel` and the same immutable stable tag as Core. This preserves the
existing PyPI trusted-publisher workflow/environment. For example, after Core
assets are available:

```sh
gh workflow run alopex-py-release.yml --ref v0.8.17 \
  -f mode=intel -f core_tag=v0.8.17 -f core_run_id=CORE_RUN_ID
```

Replace `CORE_RUN_ID` with the actual Core delivery run. Dispatch from the stable
tag itself: the workflow rejects a checkout/workflow SHA that differs from the
annotated tag. It checks the frozen candidate manifest, the primary parser
manifest and RC envelope, and confirms that RC and stable tags peel to the same
commit. The RC envelope stays byte-identical. A separate local envelope supplies
the stable source identity when creating the supplemental record.

The Intel runner builds the parser, CLI and wheel from that source, then executes
four bounded checks: native exported parser contract, CLI version, installed wheel
vector/streaming APIs, and a public crates.io SQL consumer that parses a small SQL
statement through the strict supplemental resolver. Source builds and the public
consumer share one Cargo target directory and execute serially under the storage
watchdog. No load/performance checks are added to ordinary CI.

Only after these checks succeed does `optional-intel-qualified` contain the
sealed `intel-release-vX.Y.Z.json`, parser target record/archive, parser supplement,
CLI, wheel and four smoke logs. The manifest binds the primary and candidate
manifest digests, exact source SHA, qualification run ID, and artifact hashes and
sizes. Artifact retention is 90 days; successful publication preserves the
manifest and logs on the Core release.

Publication rechecks that the native job succeeded at the expected source, then
compares existing assets on Core GitHub Releases, Python GitHub Releases and
PyPI. An absent artifact can be added; an identical artifact is reused; the same
name with a different size/hash aborts. GitHub uploads never use `--clobber` and
PyPI never silently skips an unchecked existing file. The frozen primary assets
and candidate manifest are not replaced. A final check verifies the new GitHub
assets and the published PyPI wheel digest.

## Availability and retries

| Intel state | Primary release | Intel delivery |
|---|---|---|
| Queued or running | May finish once all three primary platforms pass | No availability claim yet |
| Qualification failed/cancelled | Unchanged | No publication; investigate the failed bounded check |
| Successful while primary Python delivery is finishing | Unchanged | Qualification is retained; publication requires both Core/Python stable surfaces |
| Successful later | Already published primary bytes remain fixed | Add qualified missing Intel artifacts |
| Publication interrupted or primary Python not yet available | Unchanged | Reuse qualification artifact; do not rebuild previously published bytes |
| Retry encounters same-name/different-byte asset | Unchanged | Stop; no overwrite or deletion |

Retry the publication portion with the original successful Intel qualification
run, even if that overall run failed in its later publication job:

```sh
gh workflow run alopex-py-release.yml --ref v0.8.17 \
  -f mode=intel -f core_tag=v0.8.17 -f core_run_id=CORE_RUN_ID \
  -f intel_artifact_run_id=INTEL_QUALIFICATION_RUN_ID
```

The retry skips the native build, downloads that exact run's artifact, and checks
its manifest run ID against the requested run. If the retained artifact has
expired, stop and recover the qualified bytes/evidence rather than assuming a
rebuild is identical. A source fix requires a new release version once the stable
tag is public. The publication job serializes attempts for the same Core tag;
a concurrent asset conflict still fails safely and can be retried after review.

Before Intel is published, Intel CLI/wheel/parser assets are unavailable even
though the three-platform Core release is complete. After publication, use the
versioned Intel manifest and its native evidence to obtain the supplemental pin
and the matching assets. Crate consumers use the explicit offline installation
above; there is no implicit Cargo network download. Primary release manifests
continue to describe only the frozen primary assets.

## Acceptance still required

The implementation and fixture contract tests are development evidence. Before
closing #686, reconcile the candidate's Core/Python inventory checks (including
#688's latest-release check), verify candidate CI and the three-platform release
path, and retain a real Intel run's source/run/native/package/publication evidence.
The tests simulate byte identity and workflow dependency outcomes; they do not
prove that the GitHub Intel runner or PyPI publication has executed successfully.
