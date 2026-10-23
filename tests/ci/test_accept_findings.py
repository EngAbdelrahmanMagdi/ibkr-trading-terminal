import datetime as dt
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "infrastructure/ci"))
import accept_findings


def finding(advisory, artifact="image:redis", fix_available=False):
    return {"artifact": artifact, "architecture": "linux/amd64", "recipe_sha256": "a" * 64,
            "base_images": [], "advisory": advisory, "severity": "HIGH", "component": "libc6",
            "version": "2.41", "target": "debian:13.7", "fix_available": fix_available}


class AcceptFindingsTest(unittest.TestCase):
    reviewed, review_by = dt.date(2026, 10, 22), dt.date(2027, 1, 20)

    def propose(self, findings, exceptions=(), upstream_current=frozenset(), renew=False):
        return accept_findings.propose(findings, {"schema_version": 2, "exceptions": list(exceptions)},
                                       self.reviewed, self.review_by, "repository owner",
                                       upstream_current, renew)

    def test_unfixable_finding_proposed_with_review_date(self):
        proposal, fixable = self.propose([finding("CVE-1")])
        self.assertEqual([], fixable)
        [entry] = proposal["exceptions"]
        self.assertEqual(("CVE-1", "2026-10-22", "2027-01-20"),
                         (entry["advisory"], entry["reviewed"], entry["review_by"]))
        self.assertNotIn("fix_available", entry)

    def test_fixable_finding_is_never_proposed(self):
        proposal, fixable = self.propose([finding("CVE-2", fix_available=True)])
        self.assertEqual([], proposal["exceptions"])
        self.assertEqual("CVE-2", fixable[0]["advisory"])

    def test_current_upstream_image_fix_recorded_with_acceptance(self):
        vendor = "image:grafana/grafana:13.2.4"
        proposal, fixable = self.propose([finding("CVE-4", artifact=vendor, fix_available=True),
                                          finding("CVE-5", fix_available=True)], upstream_current={vendor})
        [entry] = proposal["exceptions"]
        self.assertEqual(("CVE-4", True, accept_findings.UPSTREAM_CURRENT),
                         (entry["advisory"], entry["fix_available"], entry["reason"]))
        self.assertEqual(["CVE-5"], [item["advisory"] for item in fixable])

    def test_built_image_cannot_claim_no_upstream_release(self):
        with self.assertRaises(ValueError):
            self.propose([finding("CVE-6", fix_available=True)], upstream_current={"image:redis"})

    def test_existing_review_kept_and_stale_dropped(self):
        kept, _ = self.propose([finding("CVE-1")])
        old = {**kept["exceptions"][0], "reviewed": "2026-09-01", "review_by": "2026-12-01"}
        stale = {**old, "advisory": "CVE-gone"}
        proposal, _ = self.propose([finding("CVE-1")], [old, stale])
        self.assertEqual([old], proposal["exceptions"])

    def test_renewal_refreshes_dates_and_keeps_reason(self):
        kept, _ = self.propose([finding("CVE-1")])
        old = {**kept["exceptions"][0], "reviewed": "2026-07-01", "review_by": "2026-09-29",
               "reason": "Reviewed rationale"}
        proposal, _ = self.propose([finding("CVE-1")], [old], renew=True)
        [entry] = proposal["exceptions"]
        self.assertEqual(("Reviewed rationale", "2026-10-22", "2027-01-20"),
                         (entry["reason"], entry["reviewed"], entry["review_by"]))

    def test_renewal_drops_exception_once_fix_is_published(self):
        kept, _ = self.propose([finding("CVE-1")])
        proposal, fixable = self.propose([finding("CVE-1", fix_available=True)], kept["exceptions"],
                                         renew=True)
        self.assertEqual([], proposal["exceptions"])
        self.assertEqual(["CVE-1"], [item["advisory"] for item in fixable])

    def test_unknown_artifact_class_requires_fix(self):
        with self.assertRaises(ValueError):
            self.propose([finding("CVE-3", artifact="npm:production")])


if __name__ == "__main__":
    unittest.main()
