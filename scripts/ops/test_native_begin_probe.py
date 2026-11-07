import ast
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import native_begin_probe as probe


class NativeBeginProbeTests(unittest.TestCase):
    def test_linux_lifetime_rss_converts_kib_to_bytes(self):
        with patch.object(probe.sys, "platform", "linux"), \
             patch.object(probe.resource, "getrusage", return_value=SimpleNamespace(ru_maxrss=123)):
            self.assertEqual(probe.peak_rss_bytes(), 123 * 1024)

    def test_macos_lifetime_rss_keeps_bytes(self):
        with patch.object(probe.sys, "platform", "darwin"), \
             patch.object(probe.resource, "getrusage", return_value=SimpleNamespace(ru_maxrss=123)):
            self.assertEqual(probe.peak_rss_bytes(), 123)

    def test_unknown_units_and_negative_counters_fail(self):
        for platform, value in (("unsupported", 123), ("linux", -1), ("darwin", -1)):
            with patch.object(probe.sys, "platform", platform), \
                 patch.object(probe.resource, "getrusage", return_value=SimpleNamespace(ru_maxrss=value)):
                with self.assertRaises(RuntimeError): probe.peak_rss_bytes()

    def test_workload_and_timing_boundaries_match_pinned_original(self):
        root = Path(os.environ.get("ALOPEX_PRODUCT_ROOT", Path(__file__).resolve().parents[2]))
        original = ast.parse((root / "scripts/performance/begin_acceptance.py").read_text())
        adapted = ast.parse(Path(probe.__file__).read_text())
        def functions(tree):
            return {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
        before, after = functions(original), functions(adapted)
        for name in ("snapshot", "validate_rows", "emit"):
            self.assertEqual(ast.dump(before[name]), ast.dump(after[name]))
        class NormalizeAllowedPlatformChanges(ast.NodeTransformer):
            def visit_If(self, node):
                # The top-level platform guard changes; workload branches do not.
                if (isinstance(node.test, ast.Compare) and
                        ast.dump(node.test.left) == ast.dump(ast.parse("sys.platform", mode="eval").body)):
                    return None
                return self.generic_visit(node)
            def visit_Call(self, node):
                if isinstance(node.func, ast.Name) and node.func.id == "peak_rss_bytes":
                    return ast.parse("resource.getrusage(resource.RUSAGE_SELF).ru_maxrss", mode="eval").body
                return self.generic_visit(node)
        normalize = NormalizeAllowedPlatformChanges()
        self.assertEqual(ast.dump(normalize.visit(before["main"])), ast.dump(normalize.visit(after["main"])))


if __name__ == "__main__":
    unittest.main()
