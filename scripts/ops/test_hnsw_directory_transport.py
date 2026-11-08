"""Parser transport may change; source repository and content checks may not."""
import ast
from pathlib import Path
import unittest

from hosted_hnsw_directory_assets import parser_request


class ParserRequestTests(unittest.TestCase):
    def test_public_api_requests_asset_bytes_without_credentials(self):
        url = "https://api.github.com/repos/alopex-db/alopex/releases/assets/123"
        request = parser_request(url)
        self.assertEqual(request.full_url, url)
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(dict(request.header_items()), {
            "Accept": "application/octet-stream", "X-github-api-version": "2022-11-28"})

    def test_original_release_routes_still_work(self):
        for tag in ("v0.8.16", "alopex-py-v0.8.16"):
            url = f"https://github.com/alopex-db/alopex/releases/download/{tag}/parser.tar.gz"
            self.assertEqual(parser_request(url).full_url, url)

    def test_rejects_untrusted_repository_host_scheme_and_route(self):
        urls = [
            "http://api.github.com/repos/alopex-db/alopex/releases/assets/123",
            "https://api.github.com/repos/other/alopex/releases/assets/123",
            "https://api.github.com.evil/repos/alopex-db/alopex/releases/assets/123",
            "https://user@api.github.com/repos/alopex-db/alopex/releases/assets/123",
            "https://api.github.com/repos/alopex-db/alopex/releases/assets/0",
            "https://api.github.com/repos/alopex-db/alopex/releases/assets/123?other=1",
            "https://api.github.com/repos/alopex-db/alopex/releases/assets/123#fragment",
            "https://github.com/alopex-db/alopex/releases/download/v0.8.16/../../other",
            "https://github.com/alopex-db/alopex/releases/download/v0.8.17/parser.tar.gz",
        ]
        for url in urls:
            with self.subTest(url=url), self.assertRaises(ValueError):
                parser_request(url)

    def test_fetch_retains_size_budget_and_hash_checks(self):
        module = ast.parse(Path(__file__).with_name("hosted_hnsw_directory_assets.py").read_text())
        main = next(n for n in module.body if isinstance(n, ast.FunctionDef) and n.name == "main")
        fetch = next(n for n in main.body if isinstance(n, ast.FunctionDef) and n.name == "fetch")
        namespace = {}
        exec(compile(ast.Module(body=[fetch], type_ignores=[]), "fetch", "exec"), namespace)
        import hashlib
        import io
        import tempfile
        from unittest.mock import Mock
        budget = Mock()
        def require(condition, code):
            if not condition:
                raise ValueError(code)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "asset"
            namespace.update(parser_request=parser_request, budget=budget, require=require,
                             digest=lambda p: hashlib.sha256(p.read_bytes()).hexdigest(),
                             urllib=Mock())
            for payload, expected, limit, failure in (
                    (b"abc", hashlib.sha256(b"abc").hexdigest(), 3, None),
                    (b"abcd", hashlib.sha256(b"abcd").hexdigest(), 3, "CAPACITY"),
                    (b"abc", "0" * 64, 3, "IDENTITY")):
                output.unlink(missing_ok=True)
                namespace["urllib"].request.urlopen.return_value = io.BytesIO(payload)
                if failure:
                    with self.assertRaisesRegex(ValueError, failure):
                        namespace["fetch"]("https://api.github.com/repos/alopex-db/alopex/releases/assets/123", output, expected, limit)
                else:
                    namespace["fetch"]("https://api.github.com/repos/alopex-db/alopex/releases/assets/123", output, expected, limit)
                    self.assertEqual(output.read_bytes(), payload)
            self.assertEqual(budget.before.call_count, 3)
            budget.check.assert_called()


if __name__ == "__main__":
    unittest.main()
