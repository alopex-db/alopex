"""Reject local data and changed measurements at the publication boundary."""

import copy
import hashlib
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_public as case
import issue681_public_pair as pair
import issue681_public_transport as transport
from test_issue681_public_pair import PINS, fixture


def public_fixture(finish="rollback"):
    pins = copy.deepcopy(PINS)
    reports = fixture(finish)
    for arm in pair.ARMS:
        fixed = transport.source.PINS[arm]
        pins[arm].update(source_commit=fixed["commit"],
                         source_tree=fixed["tree"],
                         example_sha256=fixed["example"],
                         cargo_lock_sha256=fixed["lock"],
                         rust_version="1.90.0")
    for report, (_, _, arm) in zip(reports, pair._order()):
        report["source_commit"] = pins[arm]["source_commit"]
    value = pair.assemble(reports, pins, finish)
    raw = case._canonical(value)
    return raw, pins


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


class PublicTransportTests(unittest.TestCase):
    def test_canonical_complete_pair_is_publishable_bytes(self):
        for finish in ("commit", "rollback"):
            with self.subTest(finish=finish):
                raw, pins = public_fixture(finish)
                self.assertEqual(transport.validate(
                    raw, expected_sha256=digest(raw),
                    expected_pins=pins, finish=finish), raw)

    def test_local_field_and_changed_summary_are_rejected(self):
        raw, pins = public_fixture()
        for change in ("local_field", "summary", "arm"):
            with self.subTest(change=change):
                value = json.loads(raw)
                if change == "local_field":
                    value["cases"][0]["report"]["local_path"] = "/private/host"
                elif change == "summary":
                    value["summary"][0]["candidate_minus_before"] = 0
                else:
                    value["cases"][0]["arm"] = "candidate"
                changed = case._canonical(value)
                with self.assertRaisesRegex(ValueError, "^PUBLIC_TRANSPORT$"):
                    transport.validate(changed, expected_sha256=digest(changed),
                                       expected_pins=pins, finish="rollback")

    def test_digest_source_and_duplicate_keys_are_rejected(self):
        raw, pins = public_fixture()
        with self.assertRaisesRegex(ValueError, "^PUBLIC_TRANSPORT$"):
            transport.validate(raw, expected_sha256="0" * 64,
                               expected_pins=pins, finish="rollback")
        wrong = copy.deepcopy(pins)
        wrong["candidate"]["source_commit"] = "0" * 40
        with self.assertRaisesRegex(ValueError, "^PUBLIC_TRANSPORT$"):
            transport.validate(raw, expected_sha256=digest(raw),
                               expected_pins=wrong, finish="rollback")
        duplicate = raw.replace(b'"status":"measured"',
                                b'"status":"measured","status":"measured"', 1)
        with self.assertRaisesRegex(ValueError, "^PUBLIC_TRANSPORT$"):
            transport.validate(duplicate, expected_sha256=digest(duplicate),
                               expected_pins=pins, finish="rollback")

    def test_oversize_and_wrong_finish_are_rejected(self):
        raw, pins = public_fixture()
        with self.assertRaisesRegex(ValueError, "^PUBLIC_TRANSPORT$"):
            transport.validate(b"x" * (transport.MAX_BYTES + 1),
                               expected_sha256=digest(raw),
                               expected_pins=pins, finish="rollback")
        with self.assertRaisesRegex(ValueError, "^PUBLIC_TRANSPORT$"):
            transport.validate(raw, expected_sha256=digest(raw),
                               expected_pins=pins, finish="commit")
