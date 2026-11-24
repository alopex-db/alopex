"""The public final SQL mode stays separate from debug and legacy uploads."""
from pathlib import Path
import unittest


WORKFLOW = Path(__file__).resolve().parents[3] / ".github/workflows/alopex-performance.yml"


class FinalSqlWorkflowTests(unittest.TestCase):
    def test_final_mode_has_one_qualified_public_only_route(self):
        source = WORKFLOW.read_text()
        job = source.split("  qualification:\n", 1)[1].split("  wal_cost_682:\n", 1)[0]
        self.assertIn("      sql_final_mode:\n", source)
        self.assertIn("inputs.sql_final_mode && !inputs.sql_four_case_mode && !inputs.sql_diagnostic_mode", job)
        self.assertIn("if: always() && !inputs.sql_four_case_mode && !inputs.sql_final_mode", job)
        for phase in ("install", "run"):
            self.assertIn(f"scripts/ops/final_sql_job.py {phase}", job)
        self.assertLess(job.index("scripts/ops/final_sql_job.py install"),
                        job.index("hosted_test_env_qualification_four_case.py --admission-only"))
        self.assertLess(job.index("hosted_test_env_qualification_four_case.py --execute"),
                        job.index("scripts/ops/final_sql_job.py run"))
        self.assertLess(job.index("scripts/ops/final_sql_job.py admit-sources"),
                        job.index("Check out fixed public SQL docs source"))
        self.assertIn("ref: 5c3d068b9ce4d60fbc1608924882df13b0a27568", job)
        self.assertIn("ref: 426418099c4337e73f5ced4444215a640898ed5a", job)
        self.assertNotIn("ref: d7818774379b6d451ebdc2742778114f1e53c748", job)
        self.assertEqual(job.count("persist-credentials: false"), 2)
        final = job.split("      - name: Measure public final SQL comparison\n", 1)[1].split(
            "      - name: Capture selected CLI wire", 1)[0]
        self.assertIn("steps.final_source_budget.outcome == 'success'", final)
        self.assertIn("steps.final_sql.outcome == 'success'", final)
        self.assertIn("path: native-final-sql-export/public.json", final)
        self.assertNotIn(".cms", final)
        self.assertNotIn("native-final-sql-export/\n", final)


if __name__ == "__main__":
    unittest.main()
