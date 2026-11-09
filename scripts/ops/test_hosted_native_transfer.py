"""Artifact name admission for source-bound SQL product transfer."""
import unittest

from hosted_native_transfer import product_artifact_name
from hosted_native_sql import EXPECTED_SECCOMP_SHA256, verify_boundary_inputs


class ProductArtifactNameTests(unittest.TestCase):
    def test_accepts_current_no_mount_product_artifact(self):
        self.assertEqual(
            product_artifact_name({'product_artifact_name': 'native-product-assets-no-mount'}),
            'native-product-assets-no-mount',
        )

    def test_preserves_legacy_name_and_rejects_arbitrary_artifact(self):
        self.assertEqual(product_artifact_name({}), 'native-product-assets')
        with self.assertRaisesRegex(ValueError, 'unqualified'):
            product_artifact_name({'product_artifact_name': 'unreviewed-assets'})

    def test_rejects_unreviewed_seccomp_policy_before_construction(self):
        verify_boundary_inputs({'seccomp_sha256': EXPECTED_SECCOMP_SHA256})
        with self.assertRaisesRegex(ValueError, 'unreviewed'):
            verify_boundary_inputs({'seccomp_sha256': '0' * 64})


if __name__ == '__main__':
    unittest.main()
