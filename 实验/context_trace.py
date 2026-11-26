"""Same retrieved memory across two context layouts: inspect reuse, quality separately."""

import tempfile
from contextlib import closing
from pathlib import Path
from memory_store import Memory
from kv_pool import Pool


def compare(path):
    pools = {k: Pool(8) for k in ["dynamic_first", "stable_first"]}
    rows = []
    stable = tuple(range(8))
    vocab = {"blue": 90, "green": 91, "gold": 92}
    with closing(Memory(path)) as m:
        m.write("B", "spec", "route private")
        for turn, color in enumerate(vocab, 1):
            m.write("A", "spec", "route " + color)
            m.sync_index()
            facts = m.recall("A", "route", turn)
            assert len(facts) == 1
            # Exact fixture vocabulary, not a model tokenizer; mark the output truth explicitly.
            fragment = (vocab[facts[0][1].split()[-1]],)
            for layout, pool in pools.items():
                prompt = (
                    fragment + stable
                    if layout == "dynamic_first"
                    else stable + fragment
                ) + (99,)
                ids, hit = pool.acquire(prompt)
                pool.release(ids)
                rows.append(
                    dict(
                        turn=turn,
                        tenant="A",
                        layout=layout,
                        fact=facts[0][1],
                        reused=hit,
                        prefill=len(prompt) - hit,
                    )
                )
    return rows


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp:
        for row in compare(Path(tmp) / "state.sqlite"):
            print(row)
