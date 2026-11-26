"""Offline single-worker runtime. The external publisher is a separate SQLite DB."""
import argparse
import json
import sqlite3
from contextlib import closing
from pathlib import Path


class Interrupted(RuntimeError):
    """Injected stop at a commit boundary. No automatic retry in this process."""


def connect(d, name):
    return sqlite3.connect(d / f"{name}.sqlite")


def record(db, kind, **data):
    db.execute("INSERT INTO events(kind, payload) VALUES (?, ?)",
               (kind, json.dumps(data, sort_keys=True)))


def events(db):
    return [dict(seq=i, kind=k, **json.loads(p)) for i, k, p in db.execute(
        "SELECT seq, kind, payload FROM events ORDER BY seq")]


def init(d, scope="action", build="build-17"):
    d.mkdir(parents=True, exist_ok=False)  # Never overwrite an existing lesson run.
    with closing(connect(d, "publisher")) as pub, pub:
        pub.execute("CREATE TABLE releases(receipt INTEGER PRIMARY KEY, op_key TEXT UNIQUE, build TEXT NOT NULL)")
    with closing(connect(d, "runtime")) as rt, rt:
        rt.execute("CREATE TABLE events(seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL)")
        record(rt, "run_started", run_id="r1", build=build, scope=scope)


def publish(d, key, build):
    # The receiver (not the caller) atomically binds key, arguments and receipt.
    with closing(connect(d, "publisher")) as db, db:
        db.execute("INSERT INTO releases(op_key, build) VALUES (?, ?) ON CONFLICT(op_key) DO NOTHING", (key, build))
        receipt, stored = db.execute("SELECT receipt, build FROM releases WHERE op_key=?", (key,)).fetchone()
        if stored != build:
            raise ValueError("idempotency key reused for different arguments")
        return receipt


def model(view):
    # Only the task and *recorded* observations are model-visible in this stub.
    if view[-1]["role"] == "tool":
        return {"final_receipt": view[-1]["receipt"]}
    return {"tool": "publish", "build": view[0]["build"], "call_id": "call-1"}


def resume(d, fault="none"):
    if not (d / "runtime.sqlite").is_file() or not (d / "publisher.sqlite").is_file():
        raise ValueError("unknown run; use start first")
    with closing(connect(d, "runtime")) as rt:
        hist = events(rt)
        if hist[-1]["kind"] == "finished":
            return hist[-1]["receipt"]
        start = hist[0]
        view = [{"role": "user", "build": start["build"]}]
        action = next((e for e in hist if e["kind"] == "intent"), None)
        if action is None:
            prop = model(view)
            if prop["tool"] != "publish" or prop["build"] != start["build"]:
                raise ValueError("proposal outside task's tool contract")
            action = dict(call_id=prop["call_id"], action_id="r1/publish/1", build=prop["build"])
            with rt:
                record(rt, "intent", **action)
        result = next((e for e in hist if e["kind"] == "observation"), None)
        if result is None:
            attempt = 1 + sum(e["kind"] == "dispatch" for e in hist)
            key = action["action_id"] if start["scope"] == "action" else f"{action['action_id']}/{attempt}"
            with rt:
                record(rt, "dispatch", action_id=action["action_id"], attempt=attempt, key=key)
            if fault == "before-call":
                raise Interrupted(fault)
            receipt = publish(d, key, action["build"])
            if fault == "after-effect":
                raise Interrupted(fault)
            with rt:
                record(rt, "observation", receipt=receipt, call_id=action["call_id"], attempt=attempt)
        else:
            receipt = result["receipt"]
        view.append({"role": "tool", "call_id": action["call_id"], "receipt": receipt})
        answer = model(view)
        with rt:
            record(rt, "finished", receipt=answer["final_receipt"])
        return answer["final_receipt"]


def inspect_run(d):
    if not all((d / f"{name}.sqlite").is_file() for name in ("runtime", "publisher")):
        raise ValueError("both databases are required")
    with closing(connect(d, "runtime")) as rt, closing(connect(d, "publisher")) as pub:
        return {"runtime": events(rt), "publisher": [dict(receipt=n, key=k, build=b) for n, k, b in
            pub.execute("SELECT receipt, op_key, build FROM releases ORDER BY receipt")]}


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    sub = cli.add_subparsers(dest="op", required=True)
    for op in ("start", "resume", "show"):
        p = sub.add_parser(op)
        p.add_argument("dir", type=Path)
        if op == "start":
            p.add_argument("--scope", choices=("action", "attempt"), default="action")
        if op != "show":
            p.add_argument("--fault", choices=("none", "before-call", "after-effect"), default="none")
    args = cli.parse_args()
    if args.op == "start":
        init(args.dir, args.scope)
    if args.op == "show":
        print(json.dumps(inspect_run(args.dir), indent=2))
    else:
        try:
            print("final receipt:", resume(args.dir, args.fault))
        except Interrupted as stop:
            print(f"injected stop: {stop}; inspect before resuming")
            return 75
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
