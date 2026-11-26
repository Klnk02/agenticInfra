"""SQLite truth + deliberately lagging FTS5 index. Two tenants, no cloud service."""

import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path


class Memory:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        with self.db:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS truth(tenant, k, rev INTEGER, body, expires INTEGER, deleted INTEGER, PRIMARY KEY(tenant,k))"
            )
            self.db.execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS idx USING fts5(tenant UNINDEXED,k UNINDEXED,rev UNINDEXED,body)"
            )

    def write(self, tenant, k, text, expires=100, delete=False):
        with self.db:
            self.db.execute(
                "INSERT INTO truth VALUES (?,?,1,?,?,?) ON CONFLICT(tenant,k) DO UPDATE SET rev=rev+1,body=excluded.body,expires=excluded.expires,deleted=excluded.deleted",
                (tenant, k, text, expires, int(delete)),
            )

    def sync_index(self):
        with self.db:
            self.db.execute("DELETE FROM idx")
            self.db.execute(
                "INSERT INTO idx SELECT tenant,k,rev,body FROM truth WHERE deleted=0"
            )

    def recall(self, tenant, term, now, guarded=True):
        if not guarded:  # Deliberately stale control; tenant is still filtered.
            return list(
                self.db.execute(
                    "SELECT k,body FROM idx WHERE idx MATCH ? AND tenant=? ORDER BY rank,k",
                    (term, tenant),
                )
            )
        return list(
            self.db.execute(
                """SELECT t.k,t.body FROM idx JOIN truth t
            ON t.tenant=idx.tenant AND t.k=idx.k AND t.rev=idx.rev
            WHERE idx MATCH ? AND t.tenant=? AND t.deleted=0 AND t.expires>? ORDER BY rank,t.k""",
                (term, tenant, now),
            )
        )

    def close(self):
        self.db.close()


def demo(path):
    with closing(Memory(path)) as m:
        m.write("A", "spec", "route blue")
        m.write("B", "spec", "route private")
        m.sync_index()
        m.write("A", "spec", "route green")
        lag = [m.recall("A", "route", 5, g) for g in [False, True]]
    with closing(Memory(path)) as m:
        m.sync_index()
        fresh = m.recall("A", "route", 5)
        m.write("A", "spec", "", delete=True)
        return dict(
            stale=lag[0],
            guarded_before_sync=lag[1],
            refreshed=fresh,
            deleted=m.recall("A", "route", 5),
            other_tenant=m.recall("B", "route", 5),
        )


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as d:
        print(demo(Path(d) / "memory.db"))
