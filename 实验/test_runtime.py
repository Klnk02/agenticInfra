import unittest
import tempfile
from pathlib import Path
from runtime_lab import Interrupted, init, inspect_run, publish, resume
from paired_eval import CASES, sample


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)

    def test_happy_and_terminal_resume(self):
        d = self.root / "r"
        init(d)
        self.assertEqual(resume(d), 1)
        before = inspect_run(d)
        self.assertEqual([e['kind'] for e in before['runtime']],
                         ['run_started', 'intent', 'dispatch', 'observation', 'finished'])
        self.assertEqual(before['runtime'][1]['call_id'], before['runtime'][3]['call_id'])
        self.assertEqual(resume(d), 1)
        self.assertEqual(inspect_run(d), before)

    def test_indistinguishable_local_history(self):
        states = []
        for fault in ('before-call', 'after-effect'):
            d = self.root / fault
            init(d)
            with self.assertRaises(Interrupted):
                resume(d, fault)
            states.append(inspect_run(d))
        self.assertEqual(states[0]['runtime'], states[1]['runtime'])
        self.assertEqual([len(s['publisher']) for s in states], [0, 1])

    def test_receiver_rejects_conflicting_arguments(self):
        init(self.root / 'r')
        d = self.root / 'r'
        self.assertEqual(publish(d, 'k', 'a'), publish(d, 'k', 'a'))
        with self.assertRaises(ValueError):
            publish(d, 'k', 'b')
        self.assertEqual(len(inspect_run(d)['publisher']), 1)

    def test_paired_outcomes_and_denominators(self):
        for scope, expected in [('attempt', [1, 1, 0, None]), ('action', [1, 1, 1, None])]:
            rows = [sample(self.root / (scope+case), case, fault, valid, scope)
                    for case, fault, valid in CASES]
            self.assertEqual([r['score'] for r in rows], expected)
            self.assertEqual([r['attempts'] for r in rows], [1, 2, 2, 1])
            self.assertEqual(rows[2]['effects'], 2 if scope == 'attempt' else 1)
            self.assertTrue(all(r['finished'] for r in rows))

    def test_two_interrupts_one_effect(self):
        d = self.root / 'r'
        init(d)
        for _ in range(2):
            with self.assertRaises(Interrupted):
                resume(d, 'after-effect')
        self.assertEqual(resume(d), 1)
        snap = inspect_run(d)
        self.assertEqual(len(snap['publisher']), 1)
        self.assertEqual([e['attempt'] for e in snap['runtime'] if e['kind'] == 'dispatch'], [1, 2, 3])


if __name__ == '__main__':
    unittest.main()
