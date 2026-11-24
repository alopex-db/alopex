"""Run the public final SQL comparison from independently admitted inputs.

The selected job owns acquisition, #804 construction and exact pin review.
This module joins private adapters; it never fetches or uploads an artifact.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from . import (admission, collection_attestor, controller, docs_harness,
               product_harness, profile_factory, public_upload,
               selected_profile, subject_binding)


class PreparedFailure(ValueError):
    """The public line is fixed; the underlying cause remains private."""

    def __init__(self, cause):
        super().__init__("PUBLIC_INCOMPLETE")
        self.cause = cause
        failure = (cause if isinstance(cause, controller.ControllerFailure) else
                   controller.ControllerFailure("PUBLIC_INCOMPLETE", 0, cause))
        self.public_line = public_upload.failure_log(failure)


def run(*, qualification_args, manifests, reviewed_pins, selected_image_digest,
        controller_digest, worker_path, worker_sha256, controller_path,
        controller_sha256, docs_checkout, product_checkout, docs_archive,
        harness_path, profile_dir, staging_root, public_dir, declared_seconds):
    """Return only the verified public file path after fresh private audits.

    ``manifests`` contains the two admitted public references and the
    current-job candidate manifest. All reviewed hashes are supplied
    independently of those files and the #804 qualification result.
    """
    public_admitted = False
    try:
        if not (type(qualification_args) is dict and
                set(qualification_args) == {"lock_path", "expected", "repo",
                                            "runner_temp", "environment"} and
                isinstance(worker_path, Path) and worker_path.is_absolute() and
                isinstance(controller_path, Path) and controller_path.is_absolute() and
                isinstance(public_dir, Path) and public_dir.is_absolute()):
            raise ValueError("MISSING_INPUT")
        public_upload._destination(public_dir, empty=True)
        public_admitted = True
        operators = {
            "measurement_worker": {"path": worker_path, "sha256": worker_sha256},
            "measurement_controller": {"path": controller_path,
                                       "sha256": controller_sha256},
        }
        for item in operators.values():
            path = item["path"]
            if (path.is_symlink() or not path.is_file() or
                    hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]):
                raise ValueError("OPERATOR_PIN")
        qualified_profile, qualified_profile_sha256, expected_backing_root = (
            selected_profile.from_live(qualification_args))
        # The binder reattests the current #804 job before any SQL staging.
        subjects, product_manifests, audit_pins = subject_binding.assemble(
            manifests=manifests, reviewed_pins=reviewed_pins,
            qualification_args=qualification_args,
            selected_image_digest=selected_image_digest,
            worker_sha256=worker_sha256,
            harness_sha256=docs_harness.FILES[
                "server-sql-isolated-v0816.py"][1],
            controller_digest=controller_digest)
        docs_harness.build(docs_checkout, docs_archive)
        product_root, product_files = product_harness.verify(product_checkout)
        profiles = profile_factory.ProfileFactory(
            qualified_profile, expected_profile_sha256=qualified_profile_sha256,
            profile_dir=profile_dir, staging_root=staging_root,
            source_pins=subjects)
        storage = admission.StorageAdmission(
            qualified_profile,
            expected_profile_sha256=qualified_profile_sha256,
            expected_backing_root=expected_backing_root,
            cell_profile_for=profiles.profile_for)

        def independent_evidence(_result):
            return collection_attestor.attest_collections(
                qualification_args=qualification_args,
                profile_for=profiles.profile_for,
                profile_hashes=profiles.profile_hashes,
                pins=audit_pins, harness_path=harness_path)

        result = controller.run(
            **qualification_args, subjects=subjects,
            product_manifests=product_manifests, audit_pins=audit_pins,
            docs_archive=docs_archive, product_root=product_root,
            product_files=product_files, harness_path=harness_path,
            operators=operators, destination_for=profiles.destination_for,
            profile_for=profiles.profile_for, admit_storage=storage.admit_storage,
            capacity=storage.capacity, independent_evidence=independent_evidence,
            declared_seconds=declared_seconds)
        public_upload.prepare_success(result, audit_pins, public_dir,
                                      independent_evidence)
        public_upload.verify(public_dir, pins=audit_pins, private_result=result,
                             independent_evidence=independent_evidence)
        return public_dir / "public.json"
    except Exception as error:
        if public_admitted:
            try:
                (public_dir / "public.json").unlink(missing_ok=True)
            except OSError:
                pass
        if isinstance(error, PreparedFailure):
            raise
        raise PreparedFailure(error) from error
