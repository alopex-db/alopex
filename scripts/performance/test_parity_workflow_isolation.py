"""Keep a hosted capability dispatch separate from parity measurement."""

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/parity-performance.yml"


def jobs(source: str) -> dict[str, str]:
    match = re.search(r"(?m)^jobs:\s*$", source)
    if match is None:
        raise AssertionError("workflow jobs missing")
    body = source[match.end():]
    boundaries = list(re.finditer(r"(?m)^  ([a-z][a-z0-9-]*):\s*$", body))
    return {
        current.group(1): body[current.end():next_match.start()]
        for current, next_match in zip(boundaries,
                                       [*boundaries[1:], re.compile(r"$").search(body, len(body))])
    }


class ParityWorkflowIsolationTest(unittest.TestCase):
    def test_capability_dispatch_cannot_construct_peer_or_upload_run_outcome(self):
        source = WORKFLOW.read_text(encoding="utf-8")
        selected = jobs(source)
        self.assertIn("hosted_capability:", source)
        self.assertIn("if: ${{ inputs.hosted_capability }}", selected["hosted-capability"])
        self.assertIn("runs-on: ubuntu-24.04", selected["hosted-capability"])
        self.assertNotIn("services:", selected["hosted-capability"])
        self.assertNotIn("upload-artifact", selected["hosted-capability"])
        self.assertIn("if: ${{ !inputs.hosted_capability }}", selected["performance"])
        self.assertIn("services:", selected["performance"])
        self.assertIn("if: ${{ always() && !inputs.hosted_capability }}",
                      selected["retain-run-outcome"])

    def test_capability_and_measurement_have_distinct_concurrency_groups(self):
        source = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(
            "group: parity-performance-${{ github.sha }}-${{ inputs.hosted_capability && 'capability' || 'measurement' }}",
            source,
        )


if __name__ == "__main__":
    unittest.main()
