import os
import json
from pathlib import Path
import sys
import unittest
import tempfile
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "infrastructure/ci"))
import publication


class PublicationTest(unittest.TestCase):
    def setUp(self):
        self.config = {"hostname": "portfolio.example.invalid", "prefix": "/showcase",
                       "worker_name": "isolated-showcase-assets", "account_id": "a" * 32,
                       "zone_id": "b" * 32, "free_static_only_confirmed": True,
                       "scoped_token_permissions_confirmed": True}

    def test_ci_refusal_precedes_any_credential_read(self):
        with patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}):
            with self.assertRaisesRegex(publication.PublicationError, "forbidden"):
                publication.publish(Path("does-not-exist"), False)

    def test_existing_main_route_stays_intact_but_project_conflict_fails(self):
        broad = {"pattern": "portfolio.example.invalid/*", "script": "existing-main-site"}
        publication.reject_conflicts(self.config, [broad])
        for pattern in ["portfolio.example.invalid/showcase", "portfolio.example.invalid/showcase/*",
                        "portfolio.example.invalid/showcase/assets/*", "portfolio.example.invalid/showcase*"]:
            with self.subTest(pattern=pattern):
                with self.assertRaises(publication.PublicationError):
                    publication.reject_conflicts(self.config, [broad, {"pattern": pattern, "script": "other-project"}])

    def test_owned_routes_can_be_updated(self):
        publication.reject_conflicts(self.config, [{"pattern": "portfolio.example.invalid/showcase/*",
                                                  "script": self.config["worker_name"]}])

    def test_invalid_prefix_and_unconfirmed_policy_fail_closed(self):
        for change in [{"prefix": "/../main"}, {"free_static_only_confirmed": False},
                       {"scoped_token_permissions_confirmed": False}]:
            with self.assertRaises(publication.PublicationError):
                publication.validate({**self.config, **change})

    def test_first_publication_rollback_removes_only_owned_routes(self):
        identity = {key: self.config[key] for key in ["account_id", "zone_id", "hostname", "prefix", "worker_name"]}
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            (output / "ownership.json").write_text(json.dumps(identity))
            (output / "before.json").write_text(json.dumps({**identity, "existing": False}))
            routes = [{"id": "own", "script": self.config["worker_name"]}, {"id": "main", "script": "main-site"}]
            with patch.object(publication, "get", return_value=(b"main", {})), patch.object(publication, "api") as api:
                publication.rollback_owned(self.config, "test-token-not-real", output, routes)
                self.assertEqual(2, api.call_count)
                self.assertTrue(api.call_args_list[0].args[0].endswith("/own"))
                self.assertTrue(all("/main" not in call.args[0] for call in api.call_args_list))


if __name__ == "__main__":
    unittest.main()
