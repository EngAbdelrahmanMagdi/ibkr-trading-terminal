from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "infrastructure/ci"))
import delivery_manifest


class DeliveryManifestTest(unittest.TestCase):
    def test_runtime_critical_blocks_images_even_if_output_flag_is_wrong(self):
        evidence = {"scanners_complete": True, "findings": [{"severity": "CRITICAL", "artifact": "image:core"}]}
        self.assertFalse(delivery_manifest.image_delivery_allowed(evidence, {"policy_pass": True, "runtime_image_delivery_allowed": True}))
        with self.assertRaises(ValueError):
            delivery_manifest.image_delivery_allowed({**evidence, "scanners_complete": False}, {"policy_pass": True})
    def test_detected_packages_keep_exact_versions_without_license_claims(self):
        report = {"Metadata": {"ImageID": "sha256:example"}, "Results": [{"Type": "debian", "Packages": [
            {"Name": "example", "Version": "1.2"}, {"Name": "example", "Version": "1.2"}]}]}
        result = delivery_manifest.sbom(report)
        self.assertEqual(1, len(result["packages"]))
        self.assertEqual("1.2", result["packages"][0]["versionInfo"])
        self.assertEqual("NOASSERTION", result["packages"][0]["licenseDeclared"])

    def test_credential_media_and_local_records_are_excluded(self):
        for name in ["secrets/token", ".env", "docs/plan.md", "terminal.mp4", "certificate.pem"]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                output = Path(directory)
                path = output / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("test")
                with self.assertRaises(ValueError):
                    delivery_manifest.validate_files(output)


if __name__ == "__main__":
    unittest.main()
