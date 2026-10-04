import copy
import datetime as dt
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("security_policy", ROOT / "infrastructure/ci/security_policy.py")
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)


class SecurityPolicyTest(unittest.TestCase):
    def setUp(self):
        self.item = {"artifact": "image:postgres", "architecture": "linux/amd64",
                     "recipe_sha256": "a" * 64, "base_images": ["postgres@sha256:" + "b" * 64],
                     "advisory": "CVE-example", "severity": "CRITICAL", "component": "libxml2",
                     "version": "1.0", "target": "debian"}
        self.baseline = {"schema_version": 1, "exceptions": [{**self.item,
                         "expires": "2026-10-11", "owner": "repository owner",
                         "reason": "Trusted local MOCK only; unresolved"}]}
        self.today = dt.date(2026, 10, 4)

    def test_acceptance_does_not_allow_runtime_delivery(self):
        result = policy.evaluate([self.item], self.baseline, self.today)
        self.assertTrue(result["policy_pass"])
        self.assertFalse(result["runtime_image_delivery_allowed"])
        self.assertEqual(1, len(result["accepted_unresolved"]))

    def test_each_security_context_change_requires_review(self):
        for key in policy.IDENTITY:
            with self.subTest(key=key):
                changed = copy.deepcopy(self.item)
                changed[key] = ["changed"] if key == "base_images" else "changed"
                self.assertFalse(policy.evaluate([changed], self.baseline, self.today)["policy_pass"])

    def test_expired_exception_fails(self):
        self.assertFalse(policy.evaluate([self.item], self.baseline, dt.date(2026, 10, 12))["policy_pass"])

    def test_clean_scan_does_not_claim_exception_resolved(self):
        result = policy.evaluate([], self.baseline, self.today)
        self.assertTrue(result["policy_pass"])
        self.assertEqual([], result["accepted_unresolved"])

    def test_malformed_scanner_report_fails_closed(self):
        for report in ({}, {"SchemaVersion": 2, "Results": []}):
            with self.assertRaises(ValueError):
                policy.findings(report, {})

    def test_unbounded_exception_rejected(self):
        del self.baseline["exceptions"][0]["component"]
        with self.assertRaises(ValueError):
            policy.evaluate([self.item], self.baseline, self.today)

    def test_local_image_tag_is_not_security_context(self):
        report = {"SchemaVersion": 2, "Metadata": {"ImageID": "sha256:sample",
                  "OS": {"Name": "13.7"}}, "Results": [{"Target": "mutable:tag (debian 13.7)",
                  "Class": "os-pkgs", "Type": "debian", "Vulnerabilities": [{
                  "Severity": "HIGH", "VulnerabilityID": "CVE-example", "PkgName": "libxml2",
                  "InstalledVersion": "1.0"}]}]}
        original = policy.findings(report, {})
        report["Results"][0]["Target"] = "another:tag (debian 13.7)"
        self.assertEqual(original, policy.findings(report, {}))
        self.assertEqual("debian:13.7", original[0]["target"])


if __name__ == "__main__":
    unittest.main()
