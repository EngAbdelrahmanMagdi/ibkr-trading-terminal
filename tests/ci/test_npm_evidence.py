import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "infrastructure/ci"))
import npm_evidence


class NpmEvidenceTest(unittest.TestCase):
    def test_indirect_advisory_retains_development_boundary(self):
        report = {"auditReportVersion": 2, "vulnerabilities": {
            "tool": {"severity": "high", "via": ["braces"], "nodes": []},
            "braces": {"severity": "high", "via": [{"severity": "high", "url":
                "https://github.com/advisories/GHSA-example"}], "nodes": ["node_modules/braces"]}}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "apps/web").mkdir(parents=True)
            lock = root / "apps/web/package-lock.json"
            for dev, expected in [(True, "npm:development"), (False, "npm:production")]:
                lock.write_text(json.dumps({"packages": {"node_modules/braces": {"version": "3.0.3", "dev": dev}}}))
                result = npm_evidence.findings(root, report)
                self.assertEqual(1, len(result))
                self.assertEqual(expected, result[0]["artifact"])
                self.assertEqual("GHSA-example", result[0]["advisory"])

    def test_scanner_error_cannot_be_a_clean_result(self):
        with self.assertRaises(ValueError):
            npm_evidence.findings(Path("."), {"auditReportVersion": 2, "error": {"code": "network"}, "vulnerabilities": {}})


if __name__ == "__main__":
    unittest.main()
