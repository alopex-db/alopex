# Reference compatibility development policy

## Purpose

This policy applies whenever Alopex claims compatibility, parity, conformance, or
ported behavior, or when an Alopex API is designed from another project's API,
implementation, specification, or tests. A compatible name or signature is not
sufficient evidence: Alopex must trace the implementation and tests to a pinned
reference and must make every intentional extension or divergence explicit.

Pure Alopex functionality with no external semantic basis may use
`alopex-native`, but the inventory must state why no reference contract applies.

## Mandatory development gate

Before implementation starts, the owning requirements and design must:

1. Identify every compatibility-sensitive public API and observable behavior.
2. Select the canonical reference project for each contract instead of naming one
   project as the oracle for unrelated behaviors.
3. Pin the reference version and commit. Mutable `main`, `master`, `current`,
   `stable`, or `latest` links are navigation aids, not pins.
4. Record the upstream implementation location and upstream test file/case that
   were inspected.
5. Classify the relationship as `ported`, `extended`, `diverged`,
   `not-yet-implemented`, `unsupported`, or `alopex-native`.
6. Record licensing/provenance constraints before copying test data or code.

The implementer must inspect both the reference implementation and its tests. A
design based only on API documentation, memory, or a handwritten expected value
does not satisfy this gate. When reference behavior conflicts across projects, the
design must select one contract per behavior and document the alternatives.

## Mandatory implementation procedure

The implementer must keep a machine-readable compatibility inventory that maps:

`Alopex API/behavior -> reference project -> pinned revision -> implementation source -> upstream test -> Alopex test evidence`

The implementation must follow the selected reference contract unless the design
records an intentional Alopex extension or divergence. The implementation must not
silently preserve a same-named API with different defaults, result types, ordering,
null behavior, error timing, resource bounds, or lifecycle semantics.

Bug fixes must first search every sibling API that shares the reference contract.
The implementer must fix the shared owner when the same divergence can affect more
than one surface.

## Mandatory test provenance

Every compatibility test must carry provenance in its fixture or in a linked
machine-readable sidecar. An inline prose comment alone is insufficient. The
minimum record is:

```json
{
  "reference": {
    "project": "project-name",
    "version": "exact-version",
    "commit": "full-commit-sha",
    "source": "path/to/upstream/implementation",
    "test": "path/to/upstream/test#case-id",
    "relationship": "ported"
  },
  "generation": {
    "command": "reproducible command or runner identifier",
    "result_format": "documented normalization format"
  }
}
```

When Alopex extends a reference case, the fixture must use
`relationship: "extended"` and add:

```json
{
  "extension": {
    "base_case": "path/to/upstream/test#case-id",
    "change": "the exact additional input or behavior",
    "reason": "why Alopex adds it",
    "expected_difference": "the intentional result or error difference"
  }
}
```

The same rule applies when an Alopex test generalizes, combines, parameterizes, or
adds boundaries to an upstream case. A fixture must not use `verified_with` unless
the repository contains a reproducible command and evidence for that verification.
Handwritten expected values may remain as local regression tests, but they must use
`handwritten-regression` and must not count as conformance evidence.

## Required verification

The test owner must prefer, in order:

1. Live differential execution against the pinned reference.
2. A ported upstream case with pinned source and reproducible expected-output
   generation.
3. A specification-derived fixture when the reference cannot run in CI, with the
   limitation and an owner for stronger evidence recorded.

Tests must compare every relevant public contract: signature and defaults, return
type, values, schema/type, ordering, null/NaN behavior, exceptions and validation
timing, mutation/lifecycle behavior, persistence, and documented resource limits.
Approximate or nondeterministic algorithms must use an exact oracle plus documented
quality invariants instead of brittle full-output equality.

Required CI must fail when a compatibility inventory row is missing, a
`ported`/`extended` row lacks provenance or test evidence, a pinned reference is
replaced by a mutable revision, or a required differential test is skipped. Full
upstream corpora may run in a scheduled lane, but the curated cases for a changed
contract must run in the pull-request gate.

## Review and release gate

Reviewers must treat missing reference research, missing upstream-test provenance,
and undocumented extensions/divergences as release-blocking findings. Release
evidence must distinguish external conformance, intentional Alopex extensions, and
local regression coverage; these categories must not be combined into one
compatibility percentage.

