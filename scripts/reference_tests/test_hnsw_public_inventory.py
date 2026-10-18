import unittest

from scripts.reference_tests.hnsw_public_inventory import python_hnsw_methods


class PythonHnswInventoryTests(unittest.TestCase):
    def test_only_pymethods_are_public(self):
        source = """
impl PyDatabase {
    #[cfg(test)]
    fn take_hnsw_detach_hook(&self) {}
}

#[pymethods]
impl PyDatabase {
    fn create_hnsw_index(&self) {}
    fn search_hnsw(&self) {}
}

#[cfg(test)]
mod tests {
    fn hnsw_management_releases_gil_during_native_waits() {}
}
"""
        self.assertEqual(
            python_hnsw_methods(source), {"create_hnsw_index", "search_hnsw"}
        )


if __name__ == "__main__":
    unittest.main()
