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
                     "version": "1.0", "target": "debian", "fix_available": False}
        exception = {key: self.item[key] for key in (*policy.IDENTITY, "severity")}
        self.baseline = {"schema_version": 2, "exceptions": [{**exception,
                         "owner": "repository owner", "reviewed": "2026-10-01",
                         "review_by": "2026-10-11", "reason": "Trusted local MOCK only; unresolved"}]}
        self.today = dt.date(2026, 10, 4)

    def test_acceptance_does_not_allow_runtime_delivery(self):
        result = policy.evaluate([self.item], self.baseline, self.today)
        self.assertTrue(result["policy_pass"])
        self.assertFalse(result["runtime_image_delivery_allowed"])
        self.assertEqual(1, len(result["accepted_unresolved"]))

    def test_each_finding_identity_change_requires_review(self):
        for key in policy.IDENTITY:
            with self.subTest(key=key):
                changed = {**self.item, key: "changed"}
                result = policy.evaluate([changed], self.baseline, self.today)
                self.assertFalse(result["policy_pass"])
                self.assertEqual("unaccepted", result["rejected"][0]["policy_reason"])

    def test_rebuild_keeps_review(self):
        rebuilt = {**self.item, "recipe_sha256": "c" * 64, "base_images": ["postgres@sha256:" + "d" * 64]}
        self.assertTrue(policy.evaluate([rebuilt], self.baseline, self.today)["policy_pass"])

    def test_unaccepted_fixable_finding_fails_gate(self):
        new = {**self.item, "advisory": "CVE-new", "fix_available": True}
        result = policy.evaluate([new], self.baseline, self.today)
        self.assertFalse(result["policy_pass"])
        self.assertEqual("fix-available", result["rejected"][0]["policy_reason"])

    def test_review_triggers_flag_gate_and_fail_monitor(self):
        cases = {"review-due": (self.item, dt.date(2026, 10, 12)),
                 "fix-available": ({**self.item, "fix_available": True}, self.today),
                 "severity-changed": ({**self.item, "severity": "HIGH"}, self.today)}
        for reason, (item, today) in cases.items():
            with self.subTest(reason=reason):
                gate = policy.evaluate([item], self.baseline, today, "gate")
                self.assertTrue(gate["policy_pass"])
                self.assertEqual([reason], gate["needs_review"][0]["review_reasons"])
                self.assertFalse(policy.evaluate([item], self.baseline, today, "monitor")["policy_pass"])

    def test_monitor_passes_when_reviews_are_current(self):
        self.assertTrue(policy.evaluate([self.item], self.baseline, self.today, "monitor")["policy_pass"])

    def test_fix_recorded_at_review_is_not_flagged_again(self):
        self.baseline["exceptions"][0]["fix_available"] = True
        item = {**self.item, "fix_available": True}
        result = policy.evaluate([item], self.baseline, self.today, "monitor")
        self.assertTrue(result["policy_pass"])
        self.assertEqual([], result["needs_review"])
        self.assertFalse(policy.evaluate([item], self.baseline, dt.date(2026, 10, 12), "monitor")["policy_pass"])

    def test_clean_scan_reports_stale_exception(self):
        result = policy.evaluate([], self.baseline, self.today)
        self.assertTrue(result["policy_pass"])
        self.assertEqual([], result["accepted_unresolved"])
        self.assertEqual(1, len(result["stale_exceptions"]))

    def test_malformed_scanner_report_fails_closed(self):
        for report in ({}, {"SchemaVersion": 2, "Results": []}):
            with self.assertRaises(ValueError):
                policy.findings(report, {})

    def test_invalid_baselines_rejected(self):
        mutations = {
            "missing identity": lambda b: b["exceptions"][0].pop("component"),
            "missing review date": lambda b: b["exceptions"][0].pop("review_by"),
            "empty reason": lambda b: b["exceptions"][0].update(reason=""),
            "review before reviewed": lambda b: b["exceptions"][0].update(review_by="2026-09-30"),
            "duplicate": lambda b: b["exceptions"].append(copy.deepcopy(b["exceptions"][0])),
            "old schema": lambda b: b.update(schema_version=1),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                baseline = copy.deepcopy(self.baseline)
                mutate(baseline)
                with self.assertRaises(ValueError):
                    policy.evaluate([self.item], baseline, self.today)

    def test_unknown_mode_rejected(self):
        with self.assertRaises(ValueError):
            policy.evaluate([self.item], self.baseline, self.today, "lenient")

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
        self.assertFalse(original[0]["fix_available"])

    def test_published_fix_is_recorded(self):
        report = {"SchemaVersion": 2, "Metadata": {"ImageID": "sha256:sample"}, "Results": [{
                  "Target": "app", "Vulnerabilities": [{"Severity": "HIGH", "VulnerabilityID": "CVE-x",
                  "PkgName": "lib", "InstalledVersion": "1.0", "FixedVersion": "1.1"}]}]}
        self.assertTrue(policy.findings(report, {})[0]["fix_available"])


if __name__ == "__main__":
    unittest.main()
