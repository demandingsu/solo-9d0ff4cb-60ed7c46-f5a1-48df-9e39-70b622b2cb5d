"""SQLite connection, schema and the global writer lock.

Concurrency model
-----------------
SQLite runs in WAL mode. All *mutating* operations (import, certificate
changes, publish) hold :data:`writer_lock` (an ``RLock``) and run inside a
single ``BEGIN IMMEDIATE`` transaction, so they are fully serialized and each
one sees one committed snapshot. Read-only endpoints take short-lived
autocommit connections and never block behind a writer.
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterator

# One writer (and therefore one publish computation) at a time. RLock so a
# service function may call helper functions that also take the lock.
writer_lock = threading.RLock()

_DB_PATH = os.environ.get("CAL_DB_PATH", os.path.join("data", "calibration.db"))

# Publication lifecycle:
#   serving     — current valid, queryable release
#   stale       — a previous valid release, superseded by newer data but still
#                 queryable as an expired old version
#   superseded  — replaced by a newer successful release, kept in history
#   quarantined — polluted legacy row (contains/derives from non-finite values);
#                 never valid supply data, never returned as the current row
SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    data_version INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO meta(id, data_version) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS readings (
    probe_id      TEXT NOT NULL,
    minute_utc    INTEGER NOT NULL,
    temperature_c REAL NOT NULL,
    data_version  INTEGER NOT NULL,
    PRIMARY KEY (probe_id, minute_utc)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS calibrations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    probe_id     TEXT NOT NULL,
    valid_from   INTEGER NOT NULL,
    valid_to     INTEGER NOT NULL,
    offset_c     REAL NOT NULL,
    data_version INTEGER NOT NULL,
    quarantine_reason TEXT,
    CHECK (valid_from < valid_to)
);
CREATE INDEX IF NOT EXISTS idx_cal_probe_span
    ON calibrations(probe_id, valid_from, valid_to);

CREATE TABLE IF NOT EXISTS publications (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    probe_id     TEXT NOT NULL,
    day          INTEGER NOT NULL,
    data_version INTEGER NOT NULL,
    status       TEXT NOT NULL CHECK (status IN
                     ('serving', 'stale', 'superseded', 'quarantined')),
    payload      TEXT NOT NULL,
    quarantine_reason TEXT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_pub_lookup
    ON publications(probe_id, day, status);
CREATE INDEX IF NOT EXISTS idx_pub_version ON publications(data_version);
"""

# Bump whenever a new migration step is added.
CURRENT_USER_VERSION = 2


def get_db_path() -> str:
    return _DB_PATH


def configure(db_path: str) -> None:
    """Override the database path (used by tests)."""
    global _DB_PATH
    _DB_PATH = db_path


def connect() -> sqlite3.Connection:
    """Open a WAL-mode connection with foreign keys and dict-like rows."""
    os.makedirs(os.path.dirname(os.path.abspath(_DB_PATH)), exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    """Create the schema, migrate an old database and quarantine polluted rows.

    The migration is idempotent and runs in one write transaction:

    * an old ``publications`` table whose status CHECK lacks ``quarantined``
      is rebuilt with the widened constraint (rows and ids preserved);
    * ``calibrations.quarantine_reason`` / ``publications.quarantine_reason``
      columns are added when missing;
    * every non-finite certificate offset and every publication whose payload
      contains a non-finite value (or cannot even be parsed as JSON) is flagged
      as ``quarantined``. A polluted current row is removed from supply; if an
      earlier valid release exists for the same probe/day it is promoted back
      to the queryable ``stale`` old version.
    """
    with writer_lock:
        conn = connect()
        try:
            # executescript issues an implicit COMMIT, so DDL runs in
            # autocommit mode; only the data migration/quarantine below is
            # wrapped in an explicit transaction.
            conn.executescript(SCHEMA)
            conn.execute("BEGIN IMMEDIATE")
            try:
                _migrate_schema(conn)
                scan_and_quarantine(conn)
                conn.execute(f"PRAGMA user_version = {CURRENT_USER_VERSION}")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Widen an old database to the current schema without losing rows."""
    cols = _table_columns(conn, "calibrations")
    if "quarantine_reason" not in cols:
        conn.execute(
            "ALTER TABLE calibrations ADD COLUMN quarantine_reason TEXT"
        )

    pub_cols = _table_columns(conn, "publications")
    if "quarantine_reason" not in pub_cols:
        conn.execute("ALTER TABLE publications ADD COLUMN quarantine_reason TEXT")

    # Rebuild publications only when the status CHECK lacks 'quarantined'.
    ddl = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='publications'"
    ).fetchone()["sql"]
    if "'quarantined'" not in ddl:
        # Plain execute() (not executescript, which would commit the running
        # migration transaction) so the rebuild stays atomic.
        conn.execute(
            """
            CREATE TABLE publications_new (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                probe_id     TEXT NOT NULL,
                day          INTEGER NOT NULL,
                data_version INTEGER NOT NULL,
                status       TEXT NOT NULL CHECK (status IN
                                 ('serving', 'stale', 'superseded', 'quarantined')),
                payload      TEXT NOT NULL,
                quarantine_reason TEXT,
                created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            )
            """
        )
        conn.execute(
            "INSERT INTO publications_new "
            "(id, probe_id, day, data_version, status, payload, "
            "quarantine_reason, created_at) "
            "SELECT id, probe_id, day, data_version, status, payload, "
            "NULL, created_at FROM publications"
        )
        conn.execute("DROP TABLE publications")
        conn.execute("ALTER TABLE publications_new RENAME TO publications")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pub_lookup "
            "ON publications(probe_id, day, status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pub_version ON publications(data_version)"
        )


# --------------------------------------------------------------------------- #
# Pollution detection / quarantine (non-finite values from legacy databases)
# --------------------------------------------------------------------------- #
def _contains_non_finite(value: Any) -> bool:
    """Recursively detect NaN / +-Infinity inside a decoded JSON structure."""
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, dict):
        return any(_contains_non_finite(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_non_finite(v) for v in value)
    return False


def _scan_publication_payload(raw_payload: str) -> str | None:
    """Return a human-readable quarantine reason, or ``None`` when clean."""
    try:
        payload = json.loads(raw_payload)
    except (ValueError, TypeError) as exc:
        return f"发布 payload 不是合法 JSON，无法读取（{exc}）"
    if _contains_non_finite(payload):
        return "发布内容含非有限温度（NaN/Infinity），由非有限证书偏移或加法溢出产生"
    return None


def scan_and_quarantine(conn: sqlite3.Connection) -> dict[str, int]:
    """Flag polluted certificates and publications inside an open transaction.

    Idempotent: rows already carrying a ``quarantine_reason`` are skipped, and
    rows that have since been repaired stay clean. Returns the number of newly
    quarantined rows of each kind.
    """
    certs_quarantined = 0
    for row in conn.execute(
        "SELECT id, probe_id, offset_c, quarantine_reason FROM calibrations"
    ).fetchall():
        if row["quarantine_reason"]:
            continue
        offset = row["offset_c"]
        if not isinstance(offset, float) or not math.isfinite(offset):
            reason = (
                f"证书 #{row['id']}（探头 {row['probe_id']}）偏移为非有限值 "
                f"{_float_label(offset)}，不得作为校准依据"
            )
            conn.execute(
                "UPDATE calibrations SET quarantine_reason = ? WHERE id = ?",
                (reason, row["id"]),
            )
            certs_quarantined += 1

    pubs_quarantined = 0
    polluted_pairs: set[tuple[str, int]] = set()
    # Only pairs with a row *newly* quarantined in this scan need the
    # supply-promotion step. Rows already flagged on a previous startup are
    # skipped, so restarting the service never demotes a healthy serving row
    # merely because an old quarantined historical row exists for the pair.
    newly_polluted_pairs: set[tuple[str, int]] = set()
    for row in conn.execute(
        "SELECT id, probe_id, day, status, payload, quarantine_reason "
        "FROM publications ORDER BY id"
    ).fetchall():
        if row["quarantine_reason"]:
            # Already flagged on an earlier scan: keep the status consistent
            # and remember the pair, but do not re-run supply promotion.
            if row["status"] != "quarantined":
                conn.execute(
                    "UPDATE publications SET status = 'quarantined' WHERE id = ?",
                    (row["id"],),
                )
            polluted_pairs.add((row["probe_id"], row["day"]))
            continue
        reason = _scan_publication_payload(row["payload"])
        if reason is None:
            continue
        conn.execute(
            "UPDATE publications SET status = 'quarantined', quarantine_reason = ? "
            "WHERE id = ?",
            (reason, row["id"]),
        )
        pubs_quarantined += 1
        polluted_pairs.add((row["probe_id"], row["day"]))
        newly_polluted_pairs.add((row["probe_id"], row["day"]))

    # A quarantined current row must not be the served one. Normal operation
    # keeps at most one serving/stale row per probe/day, but a legacy database
    # may already contain duplicate 'serving' rows written before the fix, so
    # re-qualify the newest *valid* row of the pair explicitly: quarantine every
    # non-historical valid row except the newest, and make that newest one the
    # queryable stale old version. With no earlier valid row nothing serves.
    # Runs only for pairs newly quarantined by this scan (restart-safe).
    for probe_id, day in sorted(newly_polluted_pairs):
        valid_current = conn.execute(
            "SELECT id FROM publications WHERE probe_id = ? AND day = ? "
            "AND status IN ('serving', 'stale') AND quarantine_reason IS NULL "
            "ORDER BY id DESC",
            (probe_id, day),
        ).fetchall()
        keep_id = valid_current[0]["id"] if valid_current else None
        for r in valid_current[1:]:
            conn.execute(
                "UPDATE publications SET status = 'superseded' WHERE id = ?",
                (r["id"],),
            )
        if keep_id is not None:
            conn.execute(
                "UPDATE publications SET status = 'stale' WHERE id = ?",
                (keep_id,),
            )
            continue
        earlier = conn.execute(
            "SELECT id FROM publications WHERE probe_id = ? AND day = ? "
            "AND status = 'superseded' AND quarantine_reason IS NULL "
            "ORDER BY id DESC LIMIT 1",
            (probe_id, day),
        ).fetchone()
        if earlier is not None:
            conn.execute(
                "UPDATE publications SET status = 'stale' WHERE id = ?",
                (earlier["id"],),
            )

    return {
        "certificates_quarantined": certs_quarantined,
        "publications_quarantined": pubs_quarantined,
    }


def _float_label(value: Any) -> str:
    if value is None:
        # SQLite round-trips a stored NaN back as NULL.
        return "NULL（原 NaN）"
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return repr(value)
    return repr(value)


def quarantine_report(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """All currently quarantined certificates and publications."""
    certs = [
        dict(r)
        for r in conn.execute(
            "SELECT id, probe_id, valid_from, valid_to, offset_c, data_version, "
            "quarantine_reason FROM calibrations WHERE quarantine_reason IS NOT NULL "
            "ORDER BY id"
        ).fetchall()
    ]
    pubs = [
        dict(r)
        for r in conn.execute(
            "SELECT id, probe_id, day, data_version, status, quarantine_reason "
            "FROM publications WHERE quarantine_reason IS NOT NULL ORDER BY id"
        ).fetchall()
    ]
    return {"certificates": certs, "publications": pubs}


@contextmanager
def write_tx() -> Iterator[sqlite3.Connection]:
    """Serialize writers and open an immediate write transaction.

    Any exception rolls the transaction back; state (including
    ``data_version`` and stale flags) is then exactly as before the call.
    """
    with writer_lock:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()


def current_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT data_version FROM meta WHERE id = 1").fetchone()
    return int(row["data_version"])


def bump_version(conn: sqlite3.Connection) -> int:
    conn.execute("UPDATE meta SET data_version = data_version + 1 WHERE id = 1")
    return current_version(conn)


def mark_days_stale(conn: sqlite3.Connection, probe_id: str, days: list[int]) -> int:
    """Flip the serving publication of each (probe, day) to ``stale``.

    Returns the number of publications marked. Days without a serving
    publication are simply absent from the list, so nothing changes for them.
    """
    changed = 0
    for day in sorted(set(days)):
        cur = conn.execute(
            "UPDATE publications SET status = 'stale' "
            "WHERE probe_id = ? AND day = ? AND status = 'serving'",
            (probe_id, day),
        )
        changed += cur.rowcount
    return changed


def stale_days_for_minutes(
    conn: sqlite3.Connection, probe_id: str, minutes: list[int]
) -> list[int]:
    """UTC days of *minutes* that currently have a serving publication."""
    if not minutes:
        return []
    days = sorted({m // 1440 for m in minutes})
    placeholders = ",".join("?" for _ in days)
    rows = conn.execute(
        f"SELECT DISTINCT day FROM publications "
        f"WHERE probe_id = ? AND status = 'serving' AND day IN ({placeholders})",
        [probe_id, *days],
    ).fetchall()
    return [int(r["day"]) for r in rows]


def stale_days_for_span(
    conn: sqlite3.Connection, probe_id: str, start_minute: int, end_minute: int
) -> list[int]:
    """Serving-publication days overlapped by a half-open minute interval."""
    if end_minute <= start_minute:
        return []
    first_day = start_minute // 1440
    last_day = (end_minute - 1) // 1440
    placeholders = ",".join("?" for _ in range(first_day, last_day + 1))
    rows = conn.execute(
        f"SELECT DISTINCT day FROM publications "
        f"WHERE probe_id = ? AND status = 'serving' "
        f"AND day BETWEEN ? AND ?",
        (probe_id, first_day, last_day),
    ).fetchall()
    return [int(r["day"]) for r in rows]
