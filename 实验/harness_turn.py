"""One harness turn: provider-neutral chunks -> validated proposal -> model view.
No provider SDK, shell, or model-generated code is executed.
"""
import json


class BadTurn(ValueError):
    pass


def propose(frames):
    # The course protocol permits one call; finish alone does not prove stream end.
    buf = ""
    call = None
    closed = False
    ended = False
    for f in frames:
        if not isinstance(f, (tuple, list)) or not f or not all(isinstance(x, str) for x in f):
            raise BadTurn('malformed frame')
        kind = f[0]
        if len(f) != {'start': 3, 'delta': 3, 'call_done': 2, 'turn_done': 1}.get(kind):
            raise BadTurn('unexpected frame shape')
        if kind == 'start' and call is None and f[1]:
            call = (f[1], f[2])
        elif kind == 'delta' and call and not closed and f[1] == call[0]:
            buf += f[2]
        elif kind == 'call_done' and call and not closed and f[1] == call[0]:
            closed = True
        elif kind == 'turn_done' and closed and not ended:
            ended = True
        else:
            raise BadTurn('invalid frame sequence')
    if not ended:
        raise BadTurn('incomplete turn; do not dispatch')
    try:
        args = json.loads(buf)
    except ValueError as exc:
        raise BadTurn('invalid JSON') from exc
    if call[1] != 'read_build' or args != {'build': 'demo'}:
        raise BadTurn('unknown tool or arguments outside contract')
    return {'role': 'assistant', 'call_id': call[0], 'tool': call[1], 'args': args}


def assemble(task, pairs, budget):
    # Synthetic units supplied by the fixture; never claim these are model tokens.
    # Keep the newest complete interaction; drop only whole older pairs.
    if task[1] > budget:
        raise BadTurn('task exceeds context budget')
    left = budget - task[1]
    kept = []
    for proposal, result, units in reversed(pairs):
        if proposal['call_id'] != result['call_id']:
            raise BadTurn('unpaired result')
        if units > left:
            if not kept:
                raise BadTurn('latest observation will not fit; use bounded tool output or stop')
            break
        kept.append([proposal, result])
        left -= units
    return [task[0]] + [msg for pair in reversed(kept) for msg in pair]


FRAMES = [('start', 'c2', 'read_build'), ('delta', 'c2', '{"bu'),
          ('delta', 'c2', 'ild":"demo"}'), ('call_done', 'c2'), ('turn_done',)]


def run(frames, dispatch):
    request = propose(frames)
    return request, {'role': 'tool', 'call_id': request['call_id'], 'content': dispatch(request['args'])}


if __name__ == '__main__':
    calls = []

    def read_build(args):
        calls.append(args)
        return 'passed'

    try:
        run(FRAMES[:-1], read_build)
    except BadTurn as e:
        print('partial:', e, 'tool calls:', len(calls))
    req, obs = run(FRAMES, read_build)
    print('complete:', json.dumps(assemble(({'role': 'user', 'content': 'inspect demo'}, 2),
                                           [(req, obs, 4)], budget=6)), 'tool calls:', len(calls))
