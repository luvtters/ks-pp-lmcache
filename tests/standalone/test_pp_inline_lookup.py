"""Exercise the real PP aggregation and new server operation without devices."""

from types import SimpleNamespace
import unittest

from test_lookup_deadline_contract import Reply, methods
from test_pp_early_miss_contract import stages


def inline_stages(left, right):
    adapter, _ = stages()
    adapter.trace_inline_lookup = False
    adapter._unacked_lookups["r"] = SimpleNamespace(
        futures={"early": left, "late": right}, submitted_at=0.0
    )
    return adapter


class InlineLookupTest(unittest.TestCase):
    def test_inline_counts_use_common_prefix_without_status(self):
        adapter = inline_stages(Reply(15), Reply(14))
        self.assertEqual(adapter.check_lookup_result("r"), 14 * 4)
        self.assertEqual(adapter.check_lookup_result("r"), 14 * 4)
        self.assertEqual(adapter.sent, [])

    def test_pending_or_legacy_peer_alone_uses_status(self):
        adapter = inline_stages(Reply(14), Reply(None))
        self.assertIsNone(adapter.check_lookup_result("r"))
        self.assertEqual(adapter.sent, [("late", "r")])
        adapter.replies["late"].ready = True
        self.assertEqual(adapter.check_lookup_result("r"), 2 * 4)
        self.assertEqual(adapter.sent, [("late", "r")])

    def test_zero_preserves_outstanding_peer_for_cleanup(self):
        pending = Reply(ready=False)
        adapter = inline_stages(Reply(0), pending)
        self.assertEqual(adapter.check_lookup_result("r"), 0)
        self.assertIs(adapter._unacked_lookups["r"].futures["late"], pending)
        self.assertEqual(adapter.sent, [])

    def test_inline_probe_uses_original_submit_and_consume_paths_once(self):
        cls = methods("lmcache/v1/multiprocess/modules/lookup.py", "LookupModule",
                      {"lookup_with_result"}, {})
        for value in (0, 14, None):
            with self.subTest(value=value):
                server, calls = cls(), []
                key = SimpleNamespace(request_id="r")
                server.lookup = lambda k, tp: calls.append((k, tp))
                server.query_prefetch_status = lambda r: calls.append(r) or value
                self.assertEqual(server.lookup_with_result(key, 16), value)
                self.assertEqual(calls, [(key, 16), "r"])
