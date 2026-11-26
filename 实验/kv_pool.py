"""Real (tiny) causal attention; then a separate bounded, token-ID block-pool model."""

import math
import random
from collections import Counter


def attention_probe(tokens, wrong_prefix=None):
    rng = random.Random(7)
    qkv = [[[rng.uniform(-1, 1) for _ in range(4)] for _ in range(4)] for _ in range(3)]

    def proj(token, pos, matrix):
        x = [math.sin(token * (j + 1) + pos / (j + 1)) for j in range(4)]
        return [sum(v * w for v, w in zip(x, row)) for row in matrix]

    def attend(q, keys, values):
        scores = [sum(a * b for a, b in zip(q, k)) / 2 for k in keys]
        weights = [math.exp(s - max(scores)) for s in scores]
        return [
            sum(w * v[i] for w, v in zip(weights, values)) / sum(weights)
            for i in range(4)
        ]

    k, v, errors = [], [], []
    for i, token in enumerate(tokens):
        q = proj(token, i, qkv[0])
        keys = [proj(t, j, qkv[1]) for j, t in enumerate(tokens[: i + 1])]
        vals = [proj(t, j, qkv[2]) for j, t in enumerate(tokens[: i + 1])]
        ref = attend(q, keys, vals)
        source = wrong_prefix if i == 0 and wrong_prefix is not None else token
        k.append(proj(source, i, qkv[1]))
        v.append(proj(source, i, qkv[2]))
        got = attend(q, k, v)
        errors.append(max(abs(a - b) for a, b in zip(got, ref)))
    n = len(tokens)
    return dict(
        max_error=max(errors),
        full_kv_pairs=n * (n + 1) // 2,
        cached_kv_pairs=n,
        cached_query_key_dots=n * (n + 1) // 2,
    )


class Pool:
    """Atomic prefill admission; pages held until release. Never models GPU time."""

    def __init__(self, pages, block=4):
        self.block, self.clock = block, 0
        self.slots = [dict(key=None, refs=0, age=0) for _ in range(pages)]

    def acquire(self, tokens, domain="model-v1/tenant-a"):
        # Prefix identity includes *all preceding tokens*, not only current block.
        b = self.block
        keys = [
            (domain, tuple(tokens[: i + b])) if i + b <= len(tokens) else None
            for i in range(0, len(tokens), b)
        ]
        hits = []
        for i, key in enumerate(keys):
            if key is None or (i + 1) * b >= len(tokens):
                break  # leave logits work
            idx = next((n for n, s in enumerate(self.slots) if s["key"] == key), None)
            if idx is None:
                break
            hits.append(idx)
        free = sorted(
            (n for n, s in enumerate(self.slots) if not s["refs"] and n not in hits),
            key=lambda n: (self.slots[n]["key"] is not None, self.slots[n]["age"], n),
        )
        need = len(keys) - len(hits)
        if need > len(free):
            raise BufferError("admission blocked: no evictable pages")
        ids = hits + free[:need]
        for n, key in zip(ids, keys):
            self.clock += 1
            self.slots[n].update(
                key=key, refs=self.slots[n]["refs"] + 1, age=self.clock
            )
        return ids, len(hits) * b

    def release(self, ids):
        for n in ids:
            assert self.slots[n]["refs"] > 0
            self.clock += 1
            self.slots[n]["refs"] -= 1
            self.slots[n]["age"] = self.clock


def placement(early):
    pool = Pool(8)
    lengths = Counter()
    stable = list(range(8))
    for memory in [90, 91, 92]:
        tokens = ([memory] + stable if early else stable + [memory]) + [99]
        pages, hit = pool.acquire(tokens)
        pool.release(pages)
        lengths.update(total=len(tokens), reused=hit, prefill=len(tokens) - hit)
    return dict(lengths)


if __name__ == "__main__":
    for wrong in [None, 77]:
        print("cached prefix:", wrong, attention_probe([2, 4, 5, 9], wrong))
    for early in [True, False]:
        print("memory first" if early else "stable first", placement(early))
