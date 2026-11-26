import unittest
from harness_turn import BadTurn, FRAMES, assemble, run


class HarnessTests(unittest.TestCase):
    def test_only_complete_valid_proposal_dispatches(self):
        invalid = [[()], [FRAMES[0], ('delta',)], FRAMES[:-1], FRAMES[:2], FRAMES + [('turn_done',)],
                   [FRAMES[0], ('delta', 'wrong', '{}'), *FRAMES[3:]],
                   [FRAMES[0], ('delta', 'c2', '{'), *FRAMES[3:]],
                   [FRAMES[0], ('delta', 'c2', '{"build":"other"}'), *FRAMES[3:]]]
        for frames in invalid:
            with self.subTest(frames=frames), self.assertRaises(BadTurn):
                run(frames, lambda _: self.fail('dispatched rejected turn'))
        req, obs = run(FRAMES, lambda _: 'ok')
        self.assertEqual((req['call_id'], obs['call_id'], obs['content']), ('c2', 'c2', 'ok'))

    def test_budget_and_pair_integrity(self):
        task = ({'role': 'user'}, 2)
        def pair(k, n):
            return ({'role': 'assistant', 'call_id': k}, {'role': 'tool', 'call_id': k}, n)
        pairs = [pair('c1', 5), pair('c2', 4)]
        self.assertEqual([m.get('call_id') for m in assemble(task, pairs, 10)], [None, 'c2', 'c2'])
        self.assertEqual(len(assemble(task, pairs, 11)), 5)
        with self.assertRaises(BadTurn):
            assemble(task, pairs, 5)
        with self.assertRaises(BadTurn):
            assemble(task, [(pairs[0][0], pairs[1][1], 4)], 10)
