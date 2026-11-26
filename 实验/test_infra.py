import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from kv_pool import attention_probe, Pool, placement
from memory_store import Memory, demo
from dag_scheduler import workload, simulate
from long_run import replay
from context_trace import compare


class InfraTests(unittest.TestCase):
    def test_real_attention(self):
        r = attention_probe([2, 4, 5, 9])
        self.assertLess(r["max_error"], 1e-10)
        self.assertEqual(
            (r["full_kv_pairs"], r["cached_kv_pairs"], r["cached_query_key_dots"]),
            (10, 4, 10),
        )
        self.assertGreater(attention_probe([2, 4, 5, 9], 77)["max_error"], 1e-3)

    def test_prefix_hit_and_capacity(self):
        p = Pool(3)
        a, h = p.acquire((1, 2, 3, 4, 5))
        self.assertEqual(h, 0)
        b, h = p.acquire((1, 2, 3, 4, 6))
        self.assertEqual(h, 4)
        self.assertEqual(a[0], b[0])
        self.assertNotEqual(a[1], b[1])
        self.assertEqual(p.slots[a[0]]["refs"], 2)
        before = [s.copy() for s in p.slots]
        with self.assertRaises(BufferError):
            p.acquire((8, 9))
        self.assertEqual(before, p.slots)
        p.release(a)
        p.release(b)
        shared, h = p.acquire((1, 2, 3, 4, 8), domain="different-tenant")
        self.assertEqual(h, 0)
        p.release(shared)
        self.assertEqual(
            (placement(True)["prefill"], placement(False)["prefill"]), (30, 14)
        )

    def test_changed_ancestor_and_eviction(self):
        p = Pool(3)
        a, _ = p.acquire(range(9))
        p.release(a)
        b, h = p.acquire((99, *range(1, 9)))
        self.assertEqual(h, 0)
        p.release(b)
        c, h = p.acquire(range(9))
        self.assertEqual(h, 0)
        p.release(c)

    def test_memory_visibility(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "m"
            r = demo(path)
            self.assertEqual(
                (r["stale"], r["guarded_before_sync"], r["refreshed"], r["deleted"]),
                ([("spec", "route blue")], [], [("spec", "route green")], []),
            )
            with closing(Memory(path)) as m:
                self.assertEqual(
                    m.recall("B", "route", 99), [("spec", "route private")]
                )
                self.assertEqual(m.recall("B", "route", 100), [])
                self.assertEqual(m.recall("A", "private", 5), [])

    def test_context_and_quality_independent(self):
        with tempfile.TemporaryDirectory() as d:
            rows = compare(Path(d) / "db")
        for turn in (1, 2, 3):
            self.assertEqual(len({r["fact"] for r in rows if r["turn"] == turn}), 1)
        self.assertEqual(
            [
                sum(r["prefill"] for r in rows if r["layout"] == k)
                for k in ("dynamic_first", "stable_first")
            ],
            [30, 14],
        )

    def test_parked_join_obeys_dependencies(self):
        ns = workload()
        a = simulate(ns, False)
        b = simulate(ns)
        self.assertEqual(
            (a["blocked_worker_ticks"], b["blocked_worker_ticks"]), (12, 0)
        )
        self.assertEqual(a["model_quanta"], b["model_quanta"])
        for n in ns:
            for dep in n.deps:
                self.assertGreaterEqual(b["starts"][n.id], b["done"][dep])
        tight = simulate(ns, model_budget=1)
        self.assertEqual(tight["peak_model_batch"], 1)
        self.assertLessEqual(tight["max_owners"], 2)

    def test_fairness_tradeoff(self):
        ns = workload("abcdefgh")
        fifo = simulate(ns)
        fair = simulate(ns, fair=True)
        self.assertLess(fair["b_latency"], fifo["b_latency"])
        self.assertGreater(fair["makespan"], fifo["makespan"])

    def test_checkpoint_tail(self):
        with tempfile.TemporaryDirectory() as tmp:
            for n, stride in [(9, 4), (8, 4), (3, 4), (10000, 256)]:
                r = replay(Path(tmp) / str(n), n, stride)
                self.assertEqual(r["restored"], r["full"])
                self.assertEqual(r["tail"], n % stride)
