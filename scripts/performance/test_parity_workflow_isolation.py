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
        self.assertIn("if: ${{ inputs.hosted_capability && !inputs.selected_inventory }}", selected["hosted-capability"])
        self.assertIn("runs-on: ubuntu-24.04", selected["hosted-capability"])
        self.assertNotIn("services:", selected["hosted-capability"])
        self.assertNotIn("upload-artifact", selected["hosted-capability"])
        self.assertIn("if: ${{ github.event.repository.private == true && !inputs.hosted_capability && !inputs.selected_inventory }}", selected["performance"])
        self.assertNotIn("services:", selected["performance"])
        self.assertIn("if: ${{ always() && github.event.repository.private == true && !inputs.hosted_capability && !inputs.selected_inventory }}",
                      selected["retain-run-outcome"])

    def test_selected_jobs_have_job_level_private_repository_guard(self):
        selected = jobs(WORKFLOW.read_text(encoding="utf-8"))
        for name in ("selected-inventory", "performance"):
            job = selected[name]
            first_line = job.strip().splitlines()[0]
            self.assertIn("github.event.repository.private == true", first_line)
            self.assertIn("runs-on: [self-hosted, Linux, X64, alopex-performance]", job)
        self.assertIn("github.event.repository.private == true",
                      selected["retain-run-outcome"].strip().splitlines()[2])

    def test_capability_and_measurement_have_distinct_concurrency_groups(self):
        source = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(
            "group: parity-performance-${{ github.sha }}-${{ inputs.selected_inventory && 'inventory' || inputs.hosted_capability && 'capability' || 'measurement' }}",
            source,
        )

    def test_selected_run_stops_before_any_construction_or_acquisition(self):
        source = WORKFLOW.read_text(encoding="utf-8")
        performance = jobs(source)["performance"]
        self.assertNotIn("services:", performance)
        checkout = performance.index("- uses: actions/checkout@v4")
        admission = performance.index("id: admission")
        reservation = performance.index("parity_storage_reservation.py")
        hold = performance.index("PARITY_ADMISSION_SECURITY_PEER_UNQUALIFIED")
        first_acquisition = performance.index("- uses: actions/setup-python@v5")
        self.assertLess(checkout, admission)
        self.assertLess(admission, reservation)
        self.assertLess(reservation, hold)
        self.assertLess(hold, first_acquisition)
        self.assertIn("PARITY_ADMISSION_PROFILE_MISSING", performance)
        self.assertIn("PARITY_SUITE:", performance[admission:first_acquisition])
        self.assertRegex(performance[hold:first_acquisition], r"exit 1\s*")


if __name__ == "__main__":
    unittest.main()
