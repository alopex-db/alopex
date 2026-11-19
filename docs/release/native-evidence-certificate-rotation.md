# Native evidence certificate rotation

The checked-in `.github/ops/native-evidence-cert.pem` is the public encryption
certificate for private CMS evidence produced by ops and qualification jobs.
Its current validity ends on **2026-12-05 15:00:49 UTC**. Plan a replacement
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
   `scripts/ops/s04_inspect_seal.py`; `s04_capture_seal.py` imports that pin.
   Replacing the PEM alone causes both S04 paths to report `CERT_MISMATCH`.
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

An expiry check is an admission warning, not a reason to disable encryption or
substitute another evidence route. If the key/certificate pair or smoke fails,
stop the rotation and retain the existing source and ciphertext.
