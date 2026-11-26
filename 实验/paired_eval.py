"""Paired deterministic evaluation: compare only the lifetime of the publisher key."""
import json
import tempfile
from pathlib import Path

from runtime_lab import Interrupted, init, inspect_run, resume

CASES = (("normal", "none", True), ("not_sent", "before-call", True),
         ("ack_lost", "after-effect", True), ("oracle_offline", "none", False))


def sample(d, case, fault, can_score, scope):
    build = f"build-{case}"
    init(d, scope, build)
    try:
        final = resume(d, fault)
    except Interrupted:
        final = resume(d)
    snap = inspect_run(d)
    # The simulated oracle outage hides external truth from the *grader* only.
    releases = snap["publisher"] if can_score else None
    score = None if releases is None else int(len(releases) == 1 and
        (releases[0]["receipt"], releases[0]["build"]) == (final, build))
    return dict(case=case, scope=scope, finished=snap["runtime"][-1]["kind"] == "finished",
                attempts=sum(e["kind"] == "dispatch" for e in snap["runtime"]),
                effects=None if releases is None else len(releases), score=score,
                invalid_reason=None if can_score else "oracle_offline")


def main():
    with tempfile.TemporaryDirectory() as tmp:
        for scope in ("attempt", "action"):
            rows = [sample(Path(tmp) / f"{scope}-{case}", case, fault, oracle, scope)
                    for case, fault, oracle in CASES]
            for row in rows:
                print(json.dumps(row))
            valid = [r for r in rows if r["score"] is not None]
            print(f"{scope}: correct/valid={sum(r['score'] for r in valid)}/{len(valid)}; "
                  f"valid/all={len(valid)}/{len(rows)}; finished/all={sum(r['finished'] for r in rows)}/{len(rows)}")


if __name__ == "__main__":
    main()
