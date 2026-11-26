"""Offline guards for the UC #682 Windows Nim bootstrap."""
from __future__ import annotations

import contextlib
import hashlib
import io
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock
import zipfile

import windows_nim_toolchain as toolchain


def archive(path: Path, members: list[tuple[str, bytes, int | None]]) -> None:
    with zipfile.ZipFile(path, "w") as output:
        for name, contents, mode in members:
            # Preserve raw member names (including backslashes) on Windows where
            # zipfile.ZipInfo.__init__ normalizes os.sep to '/'.
            info = zipfile.ZipInfo("placeholder")
            info.filename = name
            if mode is not None:
                info.create_system = 3
                info.external_attr = mode << 16
            output.writestr(info, contents)


class NimToolchainTests(unittest.TestCase):
    def test_download_checks_exact_hash_and_compressed_limit(self):
        class Response(io.BytesIO):
            def geturl(self):
                return toolchain.URL

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "nim.zip"
            with (mock.patch.object(toolchain.urllib.request, "urlopen",
                                    return_value=Response(b"archive")),
                  mock.patch.object(toolchain, "SHA256", hashlib.sha256(b"archive").hexdigest())):
                toolchain.download(output)
            self.assertEqual(output.read_bytes(), b"archive")
            with mock.patch.object(toolchain.urllib.request, "urlopen",
                                   return_value=Response(b"wrong")):
                with self.assertRaises(toolchain.BootstrapError) as error:
                    toolchain.download(Path(directory) / "wrong.zip")
            self.assertEqual(error.exception.code, "NIM_DOWNLOAD_HASH")
            with (mock.patch.object(toolchain.urllib.request, "urlopen",
                                    return_value=Response(b"oversized")),
                  mock.patch.object(toolchain, "MAX_DOWNLOAD", 4)):
                with self.assertRaises(toolchain.BootstrapError) as error:
                    toolchain.download(Path(directory) / "large.zip")
            self.assertEqual(error.exception.code, "NIM_DOWNLOAD_SIZE")

    def test_extracts_expected_executable_and_rejects_unsafe_members(self):
        valid = [("nim-2.2.10/bin/nim.exe", b"nim", stat.S_IFREG | 0o755)]
        invalid = [
            [("../escape", b"x", None)],
            [("nim-2.2.10/../../escape", b"x", None)],
            [("/nim-2.2.10/bin/nim.exe", b"x", None)],
            [("nim-2.2.10\\bin\\nim.exe", b"x", None)],
            [("nim-2.2.10/bin/nim.exe", b"x", stat.S_IFLNK | 0o777)],
            valid + [("nim-2.2.10/bin/NIM.EXE", b"x", None)],
            [("nim-2.2.10/bin", b"x", None), *valid],
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "input.zip"
            archive(path, valid)
            self.assertEqual(toolchain.extract(path, root / "safe").read_bytes(), b"nim")
            for index, members in enumerate(invalid):
                archive(path, members)
                with self.subTest(index=index), self.assertRaises(toolchain.BootstrapError) as error:
                    toolchain.extract(path, root / f"unsafe-{index}")
                self.assertEqual(error.exception.code, "NIM_ARCHIVE")
            archive(path, valid)
            with mock.patch.object(toolchain, "MAX_EXTRACTED", 2):
                with self.assertRaises(toolchain.BootstrapError) as error:
                    toolchain.extract(path, root / "large")
            self.assertEqual(error.exception.code, "NIM_ARCHIVE")

    def test_version_check_is_exact_and_diagnostics_stay_private(self):
        with tempfile.TemporaryDirectory() as directory:
            nim = Path(directory) / "nim.exe"
            nim.touch()
            def version(command, **kwargs):
                kwargs["stdout"].write(b"Nim Compiler Version 2.2.10 [Windows: amd64]\n")
                return mock.Mock(returncode=0)
            with mock.patch.object(toolchain.subprocess, "run", side_effect=version):
                toolchain.verify_version(nim)
            def wrong_version(command, **kwargs):
                kwargs["stdout"].write(b"C:\\private\\path Nim Compiler Version 2.2.9\n")
                return mock.Mock(returncode=0)
            with mock.patch.object(toolchain.subprocess, "run", side_effect=wrong_version):
                with self.assertRaises(toolchain.BootstrapError) as error:
                    toolchain.verify_version(nim)
            self.assertEqual(error.exception.code, "NIM_VERSION")
            self.assertNotIn("private", str(error.exception))

    def test_bootstrap_uses_repo_setup_with_empty_overrides(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bash = root / "bash.exe"
            setup = root / "setup.sh"
            bash.touch()
            setup.touch()
            def fake_download(path):
                path.write_bytes(b"archive")
            def fake_extract(path, destination):
                nim = destination / toolchain.NIM_ROOT / "bin/nim.exe"
                nim.parent.mkdir(parents=True)
                nim.touch()
                return nim
            with (mock.patch.object(toolchain, "REPO", root),
                  mock.patch.object(toolchain, "SETUP", setup),
                  mock.patch.object(toolchain, "download", side_effect=fake_download),
                  mock.patch.object(toolchain, "extract", side_effect=fake_extract),
                  mock.patch.object(toolchain, "verify_version"),
                  mock.patch.object(toolchain.subprocess, "run",
                                    return_value=mock.Mock(returncode=0)) as run,
                  mock.patch.dict(toolchain.os.environ,
                                  {name: "forbidden" for name in toolchain.OVERRIDES})):
                toolchain.bootstrap(root, bash)
            args, kwargs = run.call_args
            self.assertEqual(args[0], [str(bash), "--noprofile", "--norc",
                                       setup.relative_to(root).as_posix()])
            self.assertEqual(kwargs["cwd"], root)
            self.assertEqual(kwargs["env"]["PATH"].split(toolchain.os.pathsep)[0],
                             str(root / toolchain.NIM_ROOT / "bin"))
            self.assertTrue(all(kwargs["env"][name] == "" for name in toolchain.OVERRIDES))
            self.assertTrue((root / toolchain.NIM_ROOT / "bin/nim.exe").is_file())

    def test_cli_exposes_only_fixed_code(self):
        with tempfile.TemporaryDirectory() as directory:
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                result = toolchain.main(["--work-dir", directory,
                                         "--bash", str(Path(directory) / "missing.exe")])
            self.assertEqual(result, 1)
            self.assertEqual(stderr.getvalue(), "NIM_INPUT\n")
            self.assertNotIn(directory, stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
