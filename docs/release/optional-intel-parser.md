# Optional Intel parser delivery: verification foundation

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

## Remaining #686 delivery work

This foundation alone does **not** enable a three-platform release. Complete
the following connected work before accepting #686:

1. Remove Intel from the Core/Python required job dependencies and qualification
   inventory, including the public Core/Python join and latest CLI check. Keep
   all three primary targets mandatory. Record Intel availability separately.
2. Add an independent Intel build/qualification/publication workflow using the
   same immutable tag, commit, parser contract, source, and dependency inputs.
3. Bind the supplemental manifest to the frozen primary manifest and release
   identity. Validate native execution, CLI/parser/wheel hashes and sizes. Keep
   the primary candidate/vendor manifest immutable.
4. Connect the digest-pinned offline installer and strict resolver to explicit
   acquisition of the qualified supplemental assets. Verify the trusted source
   and digest handoff end to end with a published three-target crate; the local
   fixture tests alone do not satisfy this condition.
5. Permit only missing qualified artifacts to be added to GitHub Releases/PyPI.
   Treat an existing identical artifact as an idempotent retry and reject a
   same-name/different-hash artifact. Do not rebuild or overwrite core assets.
6. Test queued/running/failed/late-success Intel states, primary-target failure,
   simultaneous success, retries, and an actual Intel crate consumer. Preserve
   source/run/artifact evidence in #690 and document availability and recovery.

These are release requirements, not optional follow-up enhancements. The
four-target workflows remain in place until their replacement satisfies the
full independent-publication contract. #686 remains open until that work and
the required native/package evidence are complete.
