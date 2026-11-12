import importlib.util
from pathlib import Path
import unittest

module = importlib.util.spec_from_file_location("audit_host_pins", Path(__file__).with_name("audit-host-pins.py"))
audit = importlib.util.module_from_spec(module)
module.loader.exec_module(audit)


class AuditTest(unittest.TestCase):
    def test_window_pagination_and_authored_pin_definition(self):
        def job(identity, project, category="experiment", **fields):
            return {"id": identity, "queued": "2026-11-04T00:00:00Z", "spec": {"project": project, "class": category}, **fields}

        pinned = job("1", "private-project", "production")
        pinned["spec"]["allowed_machines"] = ["private-host"]
        constraint = job("2", "other", placement_intent={"host_constraint": "pin"})
        binding = job("3", "other", placement_intent={"host_constraint": "exclusion"})
        boundary = job("4", "other", queued="2026-11-05T00:00:00Z")

        class API:
            def __init__(self):
                self.calls = []

            def status(self, **kwargs):
                self.calls.append(kwargs)
                if not kwargs["before"]:
                    return {"jobs": [pinned, constraint], "jobs_page": {"next_before": "2"}}
                return {"jobs": [constraint, binding, boundary], "jobs_page": {}}

        api = API()
        report, names = audit.audit(api, audit.timestamp("2026-11-04T00:00:00Z"), audit.timestamp("2026-11-05T00:00:00Z"))
        rows = {(r["project"], r["class"]): r for r in report["rows"]}
        self.assertEqual(rows["all", "all"]["jobs"], 3)
        self.assertEqual(rows["all", "all"]["pinned"], 2)
        self.assertEqual(rows["all", "non-production"]["pinned_percent"], 50)
        self.assertEqual(rows["all", "production"]["pinned_percent"], 100)
        self.assertEqual(names["project-1"], "other")
        self.assertNotIn("private", str(report))
        self.assertEqual(len(api.calls), 2)
        self.assertTrue(all(c["jobs"] == "all" for c in api.calls))


if __name__ == "__main__":
    unittest.main()
