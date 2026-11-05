import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import Failure
from resolve_image import PG_VERSION, apt_variant, paragraphs, resolve, wheel_records


class AptResolution(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="alopex-resolve-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "metadata").mkdir()
        packages = {name: "1" for name in ("python3.12", "python3.12-venv", "python3-pip",
                    "sudo", "ca-certificates", "procps", "util-linux", "iproute2")}
        packages.update({name: PG_VERSION for name in ("postgresql-16", "postgresql-client-16")})
        (self.root / "base.tsv").write_text("".join(f"{key}\t{value}\n" for key, value in packages.items()))
        self.filename = f"libpq5_{PG_VERSION}_test.deb"
        self.path = "pool/main/p/postgresql-16/" + self.filename
        self.url = "https://apt-archive.postgresql.org/pub/repos/apt/" + self.path
        self.write_uri()
        self.metadata = (f"Package: libpq5\nVersion: {PG_VERSION}\nSize: 10\n"
                         f"Filename: {self.path}\nSHA256: {'a' * 64}\n"
                         "Description: test\n continuation: colon\n")
        (self.root / "metadata/libpq5.control").write_text(self.metadata)

    def write_uri(self, url=None):
        (self.root / "uris.txt").write_text(f"'{url or self.url}' {self.filename} 10 MD5Sum:ignored\n")

    def test_signed_index_sha256_replaces_apt_uri_md5(self):
        variant = apt_variant(self.root)
        self.assertEqual(variant["files"][0]["sha256"], "a" * 64)
        self.assertEqual(variant["packages"]["libpq5"], PG_VERSION)
        self.assertEqual(variant["packages"]["sudo"], "1")

    def test_uri_size_must_match_signed_index(self):
        (self.root / "metadata/libpq5.control").write_text(self.metadata.replace("Size: 10", "Size: 11"))
        with self.assertRaises(Failure):
            apt_variant(self.root)

    def test_download_path_must_match_signed_index(self):
        self.write_uri(self.url.replace("/pool/", "/different/"))
        with self.assertRaises(Failure):
            apt_variant(self.root)

    def test_missing_sha256_is_not_replaced_by_md5(self):
        (self.root / "metadata/libpq5.control").write_text(self.metadata.replace("SHA256:", "MD5sum:"))
        with self.assertRaises(Failure):
            apt_variant(self.root)

    def test_conflicting_metadata_is_rejected(self):
        (self.root / "metadata/libpq5.control").write_text(
            self.metadata + "\n" + self.metadata.replace("a" * 64, "b" * 64))
        with self.assertRaises(Failure):
            apt_variant(self.root)

    def test_postgresql_from_another_repository_is_rejected(self):
        self.write_uri(self.url.replace("apt-archive.postgresql.org", "archive.ubuntu.com"))
        with self.assertRaises(Failure):
            apt_variant(self.root)

    def test_duplicate_package_and_archive_are_rejected(self):
        path = self.root / "uris.txt"
        path.write_text(path.read_text() * 2)
        with self.assertRaises(Failure):
            apt_variant(self.root)

    def test_archive_cannot_traverse_collection_directory(self):
        self.filename = "../" + self.filename
        self.write_uri()
        with self.assertRaises(Failure):
            apt_variant(self.root)

    def test_missing_dependency_is_not_a_complete_inventory(self):
        path = self.root / "base.tsv"
        path.write_text(path.read_text().replace("python3-pip\t1\n", ""))
        with self.assertRaises(Failure):
            apt_variant(self.root)

    def test_continuations_do_not_inject_fields(self):
        self.assertEqual(paragraphs("Package: p\nDescription: x\n SHA256: forged\n"),
                         [{"Package": "p", "Description": "x\n SHA256: forged"}])
        with self.assertRaises(Failure):
            paragraphs("Package: p\nPackage: q\n")


class WheelResolution(unittest.TestCase):
    def metadata(self, url):
        name, version = url.split("/")[-3:-1]
        return {"info": {"name": name, "version": version}, "urls": [{
            "filename": f"{name}-{version}-py3-none-any.whl", "size": 10,
            "url": f"https://files.pythonhosted.org/{name}.whl", "yanked": False,
            "packagetype": "bdist_wheel", "digests": {"sha256": "b" * 64}}]}

    def test_exact_pure_python_wheels_are_pinned(self):
        rows = wheel_records("psycopg==3.2.10\ntyping_extensions==4.15.0", self.metadata)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["sha256"], "b" * 64)

    def test_unpinned_requirement_is_rejected(self):
        with self.assertRaises(Failure):
            wheel_records("psycopg>=3.2", self.metadata)

    def test_wrong_package_metadata_is_rejected(self):
        def changed(url):
            result = self.metadata(url)
            result["info"]["version"] = "0"
            return result
        with self.assertRaises(Failure):
            wheel_records("psycopg==3.2.10", changed)

    def test_yanked_wheel_is_rejected(self):
        def changed(url):
            result = self.metadata(url)
            result["urls"][0]["yanked"] = True
            return result
        with self.assertRaises(Failure):
            wheel_records("psycopg==3.2.10", changed)


class AcquisitionBoundary(unittest.TestCase):
    def test_failed_budget_prevents_docker_and_network_operations(self):
        budget, docker = Mock(), Mock()
        budget.before.side_effect = Failure("CAPACITY")
        with patch("resolve_image.acquire") as acquire:
            with self.assertRaises(Failure):
                resolve({}, docker, budget)
        self.assertEqual(docker.mock_calls, [])
        acquire.assert_not_called()


if __name__ == "__main__":
    unittest.main()
