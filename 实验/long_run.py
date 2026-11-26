"""Replay a durable event stream from a periodic, versioned checkpoint; CPU/SQLite."""

import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path


def replay(path, steps=10000, stride=256):
    if stride <= 0 or steps < 1:
        raise ValueError("positive steps and stride required")
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript(
            "CREATE TABLE event(seq INTEGER PRIMARY KEY,delta INTEGER); CREATE TABLE snap(seq INTEGER PRIMARY KEY, total INTEGER, version INTEGER);"
        )
        total = 0
        for seq in range(1, steps + 1):
            delta = seq % 7 - 2
            total += delta
            db.execute("INSERT INTO event VALUES(?,?)", (seq, delta))
            if seq % stride == 0:
                db.execute("INSERT INTO snap VALUES(?,?,1)", (seq, total))
    # Reopen a fresh connection: recovery uses durable state, not a variable in the worker.
    with closing(sqlite3.connect(path)) as db:
        boundary, base = db.execute(
            "SELECT seq,total FROM snap WHERE version=1 ORDER BY seq DESC LIMIT 1"
        ).fetchone() or (0, 0)
        tail = list(
            db.execute("SELECT delta FROM event WHERE seq>? ORDER BY seq", (boundary,))
        )
        gold = db.execute("SELECT SUM(delta) FROM event").fetchone()[0]
        return dict(
            events=steps,
            checkpoint=boundary,
            tail=len(tail),
            restored=base + sum(x for (x,) in tail),
            full=gold,
        )


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as d:
        for n in (256, 1024):
            print("stride", n, replay(Path(d) / str(n), stride=n))
