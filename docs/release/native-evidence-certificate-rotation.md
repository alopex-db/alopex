# Native evidence certificate rotation

The checked-in `.github/ops/native-evidence-cert.pem` is the public encryption
certificate for private CMS evidence produced by ops and qualification jobs.
Its current validity ends on **2027-02-20 07:20:43 UTC**. Plan a replacement
before that date. A certificate replacement is an evidence trust change and
requires a reviewed source commit and an owner for the off-repository private
key. Never commit or print a private key, execution profile, or plaintext
evidence.

## Prepare and verify privately

1. Name the private-key custodian and an owner-only storage location. Keep the
   old private key for decrypting archived CMS artifacts after rotation; a new
   certificate does not decrypt old ciphertext.
2. Create the replacement key and self-signed public certificate in that private
   location with a documented validity window. Require restrictive permissions
   for the key. Record only the public certificate's SHA-256 and validity in the
   review record.
3. Verify the certificate dates and that its public key matches the private
   key. Encrypt a synthetic, non-sensitive receipt with the new public
   certificate and decrypt it with the new private key; require exact byte
   equality. Repeat decryption of an archived test ciphertext with the retained
   old key. Keep these checks and their detailed paths private.

## Change and qualify the public source

1. Replace only `.github/ops/native-evidence-cert.pem` with the reviewed public
   certificate. In the same commit, update `CERT_SHA256` in
   `scripts/ops/s04_inspect_seal.py` and `scripts/ops/four_case_job.py`;
   `s04_capture_seal.py` imports the S04 pin. Replacing the PEM alone causes
   pinned paths to reject the certificate. The former Stage-2 CMS finalizer is
   absent from the current source and must not be restored for this rotation.
2. Review every certificate consumer: `scripts/ops/evidence.py`, both S04
   sealers, `same_job_four_case.py`, `hosted_test_env_qualification.py`,
   `hosted_test_env_qualification_1951.py`, `hosted_test_env_inventory.py`,
   `diagnostic_entrypoint.py`, and `wal_cost_next/seal_wrapper.py`. Confirm
   that no workflow embeds a stale certificate digest or private-key location.
3. Run the focused S04/ops sealer tests and one bounded qualification smoke on
   the exact source commit. Verify a new CMS receipt and its public ciphertext
   digest, then decrypt with the new key and check the original receipt bytes.
   Keep capacity, isolation, cleanup and source identity gates in force.
4. Record the new certificate identity, smoke run, retained old-key custodian,
   and next expiry date in the private release ledger. Only then use the new
   source for selected evidence runs. Preserve existing ciphertext and old keys
   until their retention period ends; never rewrite historical receipts.

A failed expiry check blocks admission for new collection; keep encryption and
the approved evidence route in force. If the key/certificate pair or smoke fails,
stop the rotation and retain the existing source and ciphertext.

## 2026-11-24 candidate source status

The candidate-facing PEM in this source has SHA-256
`e8a39adb8f76185a87f805f6d083bdbe2bd7ea5d3bc05607f1a7a2e0deb8b775`
and expires on 2027-02-20 07:20:43 UTC. Its matching private key and the
retained old key are held outside Git; their custody and local synthetic
checks are recorded privately. The frozen WAL comparator at `490f002d` still
uses the older recipient. Its separate workflow expiry check must stop new
collection when that recipient has less than 24 hours remaining. This source
status does not establish selected-runner CMS smoke, frozen-controller cutover,
private artifact retention, or notification delivery.

## 2026-11-20 source audit and approval boundary (Issue #690)

Audited candidate: `6b0435b8507d5d8399b20afc8d5086b526618d60`.
The November 19 [tracking comment](https://github.com/alopex-db/alopex/issues/690#issuecomment-7000535600)
leaves rotation open. The procedure above already exists (introduced by
`c330e0f8`); procedure availability does not establish that rotation happened.

Public identity at that audit date (retained for old evidence):

- Subject and issuer: `CN=alopex-private-evidence` (self-issued; no external CA
  or human custodian identity is established by this name).
- Validity: 2026-11-05 15:00:49 UTC through 2026-12-05 15:00:49 UTC;
  expiry is **2026-12-06 00:00:49 JST**.
- PEM file SHA-256 (the S04 pin):
  `d33a612dab2f76986e0dc6ba0ba45be15265ceccd2d56b07c14f9f696259f6b2`.
- DER certificate SHA-256 fingerprint:
  `d5ffc77f38a99990870ba412e3ca4f1952a820adc40ac8bdf0ddace1befed8ec`.
  DER fingerprint and PEM byte digest are different identifiers.

### Encryption and signature verification are separate contracts

This certificate supplies an RSA recipient public key to `openssl cms -encrypt`
(AES-256-CBC CMS EnvelopedData). These paths do not create a CMS signature, and
no production private key is needed on the runner. Ciphertext SHA-256 checks
identify collected bytes; they do not independently authenticate their author.
`evidence.py`'s `verified` field means size/hash checks succeeded, not a signature
or a private-key decryption check. Expiry alone does not make existing CMS
ciphertext undecryptable. OpenSSL CMS encryption itself is not the validity gate.

The signed Ubuntu indexes are a separate chain: Ubuntu publishes InRelease;
`capture_ubuntu_indexes.sh` uses the pinned base image's
`/usr/share/keyrings/ubuntu-archive-keyring.gpg` and `gpgv`;
`verify_captured_indexes.py` requires gpgv exit zero, a single VALIDSIG fingerprint,
source/keyring/resolver hashes, signed package index hashes and resolved package
identity. `resolve_builder_lock.py` consumes that proof, not a new signature.
The copied keyring and its hash must remain bound to the reviewed base-image
identity; accepting an arbitrary replacement keyring alongside its own hash
would not establish trust. Rotating the CMS recipient changes none of these
Ubuntu signing keys or trust decisions.

### Consumers and retained evidence

| Path | Role and acceptance |
| --- | --- |
| `scripts/ops/evidence.py`, `live_wrapper.py` | Exact diagnostic allowlist, size/hash bounds, CMS output and private modes; plaintext archive retained on seal failure |
| `s04_inspect_seal.py`, `s04_capture_seal.py` | PEM byte pin, private ownership/modes, bounded receipt, fixed public outcome and ciphertext digest; existing cleanup semantics retained |
| `hosted_test_env_inventory.py` | Read-only host receipt encrypted before artifact upload |
| `hosted_test_env_qualification.py`, `hosted_test_env_qualification_1951.py` | Qualification receipts and candidate identity; capacity, source and cleanup gates |
| `same_job_four_case.py`, `diagnostic_entrypoint.py` | Same-run/paired measurements and bounded private evidence |
| `wal_cost_next/seal_signed_indexes.py` | Encrypted signed-index capture and bounded failure fallback; public digest is not signature certification |
| `wal_cost_next/seal_wrapper.py` | Wrapper/failure receipt encryption and frozen cost evidence digest |
| `.github/workflows/alopex-performance.yml` | Inventory, qualification, WAL and S04 lanes; allowlisted encrypted artifacts/public status; **7-day artifact retention** |
| Frozen `490f002dfb2c7f509a2444891808d9704fe6b001`, `scripts/ops/hosted_wal_cost.py` | Separate checkout uses the older PEM, then removes its work directory after successful encryption; its recipient now differs from the candidate-facing PEM |

Seven-day Actions retention is shorter than a release audit's likely lifetime.
Before artifact expiry, the owner must approve a private archival destination
and retention end date, preserve CMS bytes and original public manifests, and
record artifact/run IDs, immutable source SHA, ciphertext hash, recipient serial
and issuer, PEM/DER identities, and receipt schema. Record key custody and backup
locations only in the private ledger. Do not upload decrypted receipts or keys.
At this audit date the custodian, destination and retention policy were
**unconfirmed**; do not infer them from the certificate CN. The later limited
custody decision and remaining unverified operations are separated above.

### Implemented preparation and its limits

`check_evidence_certificate.py` is a public-only admission tool. It verifies the
reviewed PEM hash, rejects symlinks, multiple certificates/private-key material,
invalid PEM, not-yet-valid and expired certificates, and insufficient remaining
validity. It emits public identities or fixed error codes, never exception text
or private input paths. The S04 constant is read as data via AST, not imported.

The performance workflow runs this check before inventory, qualification,
WAL measurement and S04 inspection, with **24 hours** remaining required (the
longest job timeout is 6 hours). WAL checks both the current and frozen checkout;
the frozen pin is explicit because that checkout predates the S04 pin source.
This is an admission gate for these workflow lanes, not a global replacement of
all standalone sealer APIs. Direct/manual execution must run the check on the
same immutable checkout before starting, and immediately before encryption if
execution can exceed the admission window. Existing failure cleanup/export
steps retain their behavior. Previously pinned external controllers/workflows
continue to use their old code until explicitly reviewed and updated.

This 2026-11-20 preparation changed no certificate, private key, secret,
external setting, frozen controller ref or measurement source digest. The
current candidate-facing PEM update is recorded above. CI runs synthetic
rotation tests; it neither generates nor installs a production recipient.

### Concrete cutover, requiring explicit owner approval

1. Propose a custodian, private storage/backup destination, retention end date,
   validity duration (proposal: 90 days), renewal lead time (proposal: 14 days),
   and bounded smoke scope. Obtain approval **before production key generation,
   issuance, PEM replacement or external configuration changes**. Recommend a
   fresh key pair; renewal with the same key is a distinct decision and does not
   remove the need to retain old certificate metadata.
2. Custodian creates the key/certificate privately, verifies public-key match,
   permission restrictions, backup restoration and synthetic encrypt/decrypt
   byte equality. Share only public identity, validity and pass/fail receipts.
   The repository cannot establish whether the actual old private key is still
   accessible; verify this privately before accepting the cutover.
3. Prepare a reviewed source change for the active PEM and S04 `CERT_SHA256`.
   Review new issuer/serial, algorithm, public key, validity and intended use.
   Preserve the old public certificate and key-to-artifact mapping in the private
   ledger. A correct hash for an unreviewed recipient is not authorization.
4. **Handle the frozen WAL checkout explicitly.** Updating only the current PEM
   leaves `490f002d` encrypting with the old recipient. Prepare a new reviewed
   frozen controller revision with the recipient change, update the workflow
   ref and its explicit PEM hash, and assess all source/config pins checked by
   `same_job_wrapper.py`. Requalify the exact new controller and comparator
   together. Do not overwrite the frozen checkout ad hoc or silently weaken
   source identity checks. Until that is approved, the new admission gate will
   intentionally block this lane when the old certificate has under 24 hours.
5. Run preflight with a 7-day planning horizon, focused tests below and a bounded
   smoke on the exact reviewed commit. Record all selected controller refs,
   artifacts/digests, public cert identity, outcome, private decryption equality,
   capacity and cleanup. An expired/mismatched recipient must stop new collection;
   do not fall back to plaintext or a different unapproved recipient.
6. Reverify one retained legitimate old artifact with its original hash and old
   private key, and the new artifact with the new key. Preserve both original
   bytes and provenance. A new key must fail to decrypt old-key ciphertext.
   Expired recipient certificates remain usable as historical decryption
   metadata; this grants no permission for new encryption or new release claims.
7. Record acceptance in #690 only after review; do not close the overall release
   issue on this work alone. For rollback before old expiry, restore the complete
   approved source/pin/ref set only if its remaining window passes admission.
   After expiry, keep new work blocked and issue an approved valid replacement.
   Keep the new key too if it has already encrypted any evidence.

### Regression and release acceptance matrix

| Test | Required result | Coverage |
| --- | --- | --- |
| Current PEM versus S04 pin | Exact match | Automated |
| Before notBefore, at/after notAfter | Reject; exact notBefore accepted | Automated, historical public fixture |
| Exactly 24h left / 24h+1s left | Reject / accept | Automated |
| Wrong PEM pin, malformed PEM, symlink, multiple certificates | Reject with fixed error | Automated |
| Old/new synthetic recipients | Both round trips preserve exact receipt bytes | Automated; ephemeral test keys only |
| Old CMS after simulated recipient expiry | New encryption admission rejects; historical decryption still succeeds | Automated; expiry policy is separate from CMS decryption |
| New key against old CMS | Decryption fails | Automated |
| Altered InRelease / nonzero gpgv with VALIDSIG text / changed index or keyring | Reject proof | Existing signed-index regression suite; real retained signed-index recheck in private smoke |
| Existing old production CMS | Original digest, old-key decryption, receipt/schema/source and acceptance checks pass | Pending private custodian smoke |
| New production CMS | New-key equality, source/capacity/cleanup and acceptance checks pass | Pending approved issuance and smoke |
| Altered ciphertext | Reject against independently retained original hash before decryption | Private smoke; CMS CBC is not an author signature |
| Frozen comparator after current PEM rotation | Its own old-recipient expiry is still rejected | Workflow checks frozen PEM independently; ref migration pending approval |

Run locally without production secrets:

```sh
python3 -B scripts/ops/check_evidence_certificate.py --minimum-seconds 604800
python3 -B -m unittest discover -s scripts/ops/tests -p 'test_evidence_certificate.py'
python3 -B -m unittest discover -s scripts/ops/tests -p 'test_s04_inspect_workflow.py'
python3 -B -m unittest discover -s scripts/ops/wal_cost_next -p 'test_*.py'
```

The historical PEM fixture under `scripts/ops/tests/fixtures` is public test
input only, never a production trust source. Keep it fixed when rotating the
active certificate so expiry-boundary tests remain deterministic.


## Recurring operation and private acceptance tool

Proposed recurring policy and the explicit approval request are in
[the operator decision record](native-evidence-operation-approval-20261121.md).
They are not approved policy until the owner accepts that record.

`native-evidence-expiry.yml` checks the selected candidate and the frozen WAL
recipient daily at 00:17 UTC (09:17 JST). It derives the frozen ref and hash from
the candidate workflow, so changing the approved comparator does not leave a
second stale hard-coded monitor target. Either recipient with 14 days or less
remaining fails the job. Failure of the current check does not skip the frozen
check. Notification delivery must be tested under the owner's Actions
notification settings; an unsuccessful run alone is not proof of receipt.
GitHub schedules require the workflow to exist on the default branch. This
candidate-targeted draft does not activate monitoring. Activation requires a
separate reviewed default-branch deployment after the candidate tool is merged,
a manual normal/expiry notification drill, and explicit owner approval. Future
release branch changes require updating the monitor target with the release
handover; missing branches/scripts are failures, not silent success.

`verify_evidence_rotation.py` runs on the approved private Linux host, outside
Actions. It requires independent ledger hashes for the old/new public PEM,
old ciphertext and decrypted archive bytes. It checks key/certificate pairs,
rejects key reuse, enforces at least 14 days on the new certificate, verifies the
old archive hash before decryption, checks decrypted byte equality, confirms
that the new key cannot decrypt the old archive, and round-trips a synthetic
receipt with the new recipient. A successful result contains only fixed status
and public certificate identities. It does not publish, issue, install, select
an archive destination or alter a repository.

Input private key files must be regular files owned by the invoking user with
mode 0600. For an encrypted key vault, the custodian materializes a temporary
unencrypted PEM on the approved encrypted local filesystem and removes it after
verification; no passphrase is accepted via command line, environment or log.
The containing directory and TMPDIR must be owner-only on that filesystem.
Temporary decrypted bytes are removed on success and handled failure; removal
is not a secure-erasure guarantee and abrupt power loss requires custodian
cleanup. Never run this verifier with production keys on a hosted CI runner.
Restore each backup to a separate owner-only path and repeat the check before
recording backup recoverability as passed.

Example structure (replace paths privately; do not paste real paths/keys into
an Issue). Hash inputs must come from the approved private ledger, not be
computed from an untrusted archive and immediately accepted as their own proof:

```sh
python3 -B scripts/ops/verify_evidence_rotation.py \
  --old-certificate /private/old/cert.pem --old-key /private/old/key.pem \
  --new-certificate /private/new/cert.pem --new-key /private/new/key.pem \
  --archived-cms /private/archive/evidence.cms \
  --old-certificate-sha256 OLD_REVIEWED_PEM_SHA256 \
  --new-certificate-sha256 NEW_REVIEWED_PEM_SHA256 \
  --ciphertext-sha256 ORIGINAL_CIPHERTEXT_SHA256 \
  --plaintext-sha256 ORIGINAL_ARCHIVE_SHA256
```

If the historical plaintext digest was never recorded, do not invent one or
mark byte-equality verification complete. The custodian must instead approve a
receipt-level validation against the original run/source/schema and independent
acceptance records, preserving that limitation. This tool's strict digest mode
will remain blocked until an independently established expected digest exists.
