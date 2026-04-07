"""Tests for query_vtraces_python output enrichment."""

import json
import unittest

from output import MAX_RESULT_LEN, enrich_traces_output


class TestEnrichTracesOutput(unittest.TestCase):
    """Verify output enrichment function."""

    def test_basic_fields(self):
        out = enrich_traces_output("services", '{"data":["svc-a","svc-b"]}')
        self.assertEqual(out["action"], "services")

    def test_jaeger_data_counting(self):
        raw = json.dumps({"data": [{"traceID": "a"}, {"traceID": "b"}]})
        out = enrich_traces_output("search", raw)
        self.assertEqual(out["result_count"], 2)

    def test_services_found_extraction(self):
        raw = json.dumps({
            "data": [
                {
                    "traceID": "abc",
                    "processes": {
                        "p1": {"serviceName": "frontend"},
                        "p2": {"serviceName": "backend"},
                    },
                },
            ],
        })
        out = enrich_traces_output("search", raw)
        self.assertIn("services_found", out)
        self.assertIn("frontend", out["services_found"])
        self.assertIn("backend", out["services_found"])

    def test_truncation(self):
        long = "x" * (MAX_RESULT_LEN + 5000)
        out = enrich_traces_output("trace_by_id", long)
        self.assertTrue(out["truncated"])
        self.assertEqual(len(out["result"]), MAX_RESULT_LEN)

    def test_invalid_json_passes_through(self):
        out = enrich_traces_output("operations", "not-json")
        self.assertEqual(out["action"], "operations")
        self.assertEqual(out["result"], "not-json")

    def test_dict_input(self):
        data = {"data": [1, 2, 3]}
        out = enrich_traces_output("search", data)
        self.assertEqual(out["result_count"], 3)


if __name__ == "__main__":
    unittest.main()
