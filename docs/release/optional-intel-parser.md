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
manifest. Providing an Intel directory when the manifest contains only primary
targets must fail; it must not turn into a self-asserted hash/sidecar acceptance.
The development-only local-build opt-in remains separate from release asset
qualification. No implicit network download or fallback build is introduced.

The current tests use small synthetic dynamic/static payloads to prove identity
validation. They do not claim an executable Intel parser has been built or run.
Actual native smoke and crate compilation belong to package qualification.

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
4. Implement an explicit authenticated acquisition/verification route for an
   already-published three-target crate to use later Intel assets. Preserve the
   strict resolver's trust boundary; copying files or replacing the pinned
   primary manifest is insufficient. Document the trusted supplemental identity
   and its binding before enabling that route.
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
