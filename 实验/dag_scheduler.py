"""Discrete ticks: schedule two tenant DAGs over bounded workers and model quanta."""

from collections import Counter
from dataclasses import dataclass


@dataclass(frozen=True)
class Node:
    id: str
    deps: tuple = ()
    arrival: int = 0
    before: int = 1
    wait: int = 0
    after: int = 0


def workload(branches="xyz"):
    if not branches or len(set(branches)) != len(branches):
        raise ValueError("unique nonempty branch names required")
    return [
        Node("A/plan"),
        *(Node("A/" + s, ("A/plan",), before=2, wait=4, after=1) for s in branches),
        Node("A/join", tuple("A/" + s for s in branches)),
        Node("B/plan", arrival=2),
        Node("B/work", ("B/plan",), arrival=2, before=2),
        Node("B/join", ("B/work",), arrival=2),
    ]


def simulate(nodes, park=True, fair=False, workers=2, model_budget=2):
    assert min(workers, model_budget) > 0
    by = {n.id: n for n in nodes}
    assert len(by) == len(nodes)
    if any(d not in by for n in nodes for d in n.deps):
        raise ValueError("missing dependency")
    phase = {k: "pending" for k in by}
    left = {n.id: n.before for n in nodes}
    if any(
        n.before < 1 or n.wait < 0 or n.after < 0 or bool(n.wait) != bool(n.after)
        for n in nodes
    ):
        raise ValueError("need positive model work on each side of a nonempty wait")
    last_tenant = None
    ready = []
    owners = []
    wake = {}
    done = {}
    starts = {}
    stats = Counter()
    log = []
    t = 0
    while len(done) < len(nodes):
        if t > 10000:
            raise ValueError("cyclic or unreasonably long fixture")
        for k, n in by.items():
            if (
                phase[k] == "pending"
                and n.arrival <= t
                and all(d in done for d in n.deps)
            ):
                phase[k] = "ready"
                ready.append(k)
                log.append((t, "ready", k))
            if phase[k] == "wait" and wake[k] == t:
                if k in owners:
                    phase[k] = "model"
                else:
                    phase[k] = "ready"
                    ready.append(k)
                log.append((t, "wake", k))
        while ready and len(owners) < workers:
            tenants = sorted({k.split("/")[0] for k in ready})
            selected = next(
                (name for name in tenants if last_tenant is None or name > last_tenant),
                tenants[0],
            )
            k = (
                next(k for k in ready if k.split("/")[0] == selected)
                if fair
                else ready[0]
            )
            ready.remove(k)
            owners.append(k)
            phase[k] = "model"
            last_tenant = k.split("/")[0]
            starts.setdefault(k, t)
            log.append((t, "admit", k))
        stats["max_owners"] = max(stats["max_owners"], len(owners))
        stats["blocked_worker_ticks"] += sum(phase[k] == "wait" for k in owners)
        batch = [k for k in owners if phase[k] == "model"][:model_budget]
        stats["peak_model_batch"] = max(stats["peak_model_batch"], len(batch))
        stats["model_quanta"] += len(batch)
        for k in batch:
            owners.remove(k)
            owners.append(k)  # rotating service among admitted nodes
            left[k] -= 1
            if left[k]:
                continue
            if by[k].wait and k not in wake:
                phase[k] = "wait"
                wake[k] = t + 1 + by[k].wait
                left[k] = by[k].after
                if park:
                    owners.remove(k)
                log.append((t + 1, "wait", k))
            else:
                phase[k] = "done"
                done[k] = t + 1
                owners.remove(k)
                log.append((t + 1, "done", k))
        t += 1
    return dict(
        makespan=t,
        b_latency=done["B/join"] - by["B/plan"].arrival if "B/join" in done else None,
        starts=starts,
        done=done,
        trace=log,
        **stats,
    )


if __name__ == "__main__":
    import argparse

    cli = argparse.ArgumentParser()
    cli.add_argument("--burst", action="store_true")
    args = cli.parse_args()
    for park, fair in [(False, False), (True, False), (True, True)]:
        r = simulate(workload("abcdefgh" if args.burst else "xyz"), park, fair)
        print(
            "park",
            park,
            "tenant_rr",
            fair,
            {k: v for k, v in r.items() if k not in ("starts", "done", "trace")},
        )
        print("join completion:", r["done"]["A/join"], r["done"]["B/join"])
