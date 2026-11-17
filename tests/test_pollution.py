"""Non-finite value (NaN / Infinity) hardening and legacy-data quarantine.

Covers the full failure chain:

* non-finite certificate offsets are rejected before any write, leaving the
  stored certificate, data_version and stale flags untouched;
* a recomputation whose calibrated temperature is non-finite (overflow of
  finite inputs, or a polluted snapshot) fails without replacing the previous
  readable publication;
* an already-polluted database is migrated on startup: polluted certificates
  and publications are quarantined (never valid supply), an earlier valid
  release is restored as a queryable stale old version, and repairing the
  certificate plus republishing restores serving while history is retained.
"""
import json
import math
import sqlite3

import pytest

from app import db, services
from app.engine import Calibration, ComputeError, Reading, compute_day
from app.timeutil import day_to_iso, parse_day

from conftest import CSV_HEADER, add_cert, import_csv

DAY = "2024-01-01"
BASE = parse_day(DAY) * 1440
DAY_IDX = BASE // 1440
H = {"Content-Type": "application/json"}


# --------------------------------------------------------------------------- #
# Engine-level finiteness guards
# --------------------------------------------------------------------------- #
def test_engine_rejects_non_finite_calibration_offset():
    readings = [Reading(BASE, 9.0)]
    with pytest.raises(ComputeError):
        compute_day("P", DAY_IDX, readings, [Calibration(BASE, BASE + 1, float("inf"))])
    with pytest.raises(ComputeError):
        compute_day("P", DAY_IDX, readings, [Calibration(BASE, BASE + 1, float("-inf"))])


def test_engine_rejects_overflow_of_finite_inputs():
    # Both finite, but their sum overflows to +Infinity.
    readings = [Reading(BASE, 1.5e308)]
    with pytest.raises(ComputeError):
        compute_day("P", DAY_IDX, readings, [Calibration(BASE, BASE + 1, 1.5e308)])


def test_engine_rejects_non_finite_reading():
    with pytest.raises(ComputeError):
        compute_day("P", DAY_IDX, [Reading(BASE, float("nan"))],
                    [Calibration(BASE, BASE + 1, 0.0)])


# --------------------------------------------------------------------------- #
# API: non-finite offsets rejected atomically
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("token", ["Infinity", "-Infinity", "NaN"])
def test_add_certificate_rejects_non_finite_offset(client, token):
    v0 = client.get("/health").json()["data_version"]
    r = client.post("/api/certificates", headers=H, content=(
        '{"probe_id":"P-01","valid_from":%d,"valid_to":%d,"offset_c":%s}'
        % (BASE, BASE + 10, token)))
    assert r.status_code == 400
    # nothing written: no certificate, no version bump, no stale flags
    assert client.get("/api/certificates").json()["count"] == 0
    assert client.get("/health").json()["data_version"] == v0


def test_patch_non_finite_offset_leaves_certificate_untouched(client):
    cid = add_cert(client, "P-01", BASE, BASE + 10, 0.1).json()["certificate_id"]
    v0 = client.get("/health").json()["data_version"]
    for token in ("Infinity", "-Infinity", "NaN"):
        r = client.patch(f"/api/certificates/{cid}", headers=H,
                         content='{"offset_c":%s}' % token)
        assert r.status_code == 400
    cert = client.get("/api/certificates?probe_id=P-01").json()["certificates"][0]
    assert cert["offset_c"] == 0.1 and cert["finite"] is True
    assert client.get("/health").json()["data_version"] == v0


def test_finite_huge_offset_is_allowed_but_overflow_publish_fails(client):
    import_csv(client, CSV_HEADER + f"P-02,{BASE},1.5e308\n")
    r = add_cert(client, "P-02", BASE, BASE + 1440, 1.5e308)
    assert r.status_code == 200  # finite, just huge
    pub = client.post("/api/publications", json={"probe_id": "P-02", "day": DAY})
    assert pub.status_code == 500
    # failure body is strict, browser-parseable JSON
    json.loads(pub.text)
    # no half-baked row was created
    assert client.get("/api/publications?probe_id=P-02").json()["count"] == 0


def test_failed_recompute_keeps_previous_readable_release(client):
    import_csv(client, CSV_HEADER + f"P-01,{BASE},9.0\nP-01,{BASE+1},9.5\n")
    add_cert(client, "P-01", BASE, BASE + 1440, 0.1)
    first = client.post("/api/publications",
                        json={"probe_id": "P-01", "day": DAY}).json()
    assert first["status"] == "serving"

    # make the stored snapshot overflow via a huge finite offset
    cid = client.get("/api/certificates?probe_id=P-01").json()["certificates"][0]["id"]
    client.patch(f"/api/certificates/{cid}", json={"offset_c": 1.7e308})
    # push the reading into a range whose sum with the offset overflows;
    # the correction also legitimately flips the old release to stale
    import_csv(client, CSV_HEADER + f"P-01,{BASE},1.0e308\n")
    repub = client.post("/api/publications",
                        json={"probe_id": "P-01", "day": DAY})
    assert repub.status_code == 500

    current = client.get("/api/publications?probe_id=P-01").json()["publications"]
    assert len(current) == 1
    assert current[0]["publication_id"] == first["publication_id"]
    # no replacement row: the previous (now stale) readable release survives
    assert current[0]["status"] == "stale"
    # payload still fully readable and finite
    peak = current[0]["payload"]["segments"][0]["max_calibrated_c"]
    assert math.isfinite(peak) and peak == 9.6


def test_every_api_response_is_strict_json_with_nan_tokens(client):
    # strict parsers must accept every response body (no Infinity/NaN literals)
    add_cert(client, "P-01", BASE, BASE + 10, 0.1)
    body = client.get("/api/certificates").text
    json.loads(body, parse_constant=lambda c: (_ for _ in ()).throw(AssertionError(c)))


# --------------------------------------------------------------------------- #
# Legacy database migration / quarantine
# --------------------------------------------------------------------------- #
def _raw_connect():
    conn = sqlite3.connect(db.get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def test_startup_quarantines_polluted_legacy_certificate_and_publication(client):
    # Build a healthy serving publication first (via the fixed API).
    import_csv(client, CSV_HEADER + f"P-01,{BASE},9.0\n")
    add_cert(client, "P-01", BASE, BASE + 1440, 0.1)
    client.post("/api/publications", json={"probe_id": "P-01", "day": DAY})

    # Simulate legacy pollution written by the old, unhardened build.
    conn = _raw_connect()
    conn.execute(
        "INSERT INTO calibrations(probe_id, valid_from, valid_to, offset_c, data_version) "
        "VALUES ('P-09', ?, ?, ?, 9)", (BASE, BASE + 1440, float("inf")))
    polluted_payload = json.dumps({
        "probe_id": "P-09", "day": DAY_IDX,
        "segments": [{"max_calibrated_c": float("inf")}],
        "uncalibrated_minutes": [],
    })
    conn.execute(
        "INSERT INTO publications(probe_id, day, data_version, status, payload) "
        "VALUES ('P-09', ?, 9, 'serving', ?)", (DAY_IDX, polluted_payload))
    conn.commit()
    conn.close()

    # Restart: schema migration + quarantine scan run inside lifespan.
    db.init_db()

    conn = _raw_connect()
    cert = conn.execute(
        "SELECT offset_c, quarantine_reason FROM calibrations WHERE probe_id='P-09'"
    ).fetchone()
    assert math.isinf(cert["offset_c"]) and cert["quarantine_reason"]
    pub = conn.execute(
        "SELECT status, quarantine_reason FROM publications WHERE probe_id='P-09'"
    ).fetchone()
    assert pub["status"] == "quarantined" and pub["quarantine_reason"]
    conn.close()

    # Report surfaces probe / day / version; default supply view excludes it.
    report = client.get("/api/quarantine").json()
    assert report["certificate_count"] >= 1 and report["publication_count"] >= 1
    p09 = [p for p in report["polluted_publications"] if p["probe_id"] == "P-09"][0]
    assert p09["day_utc"] == DAY and p09["data_version"] == 9
    assert "P-09" not in {
        x["probe_id"] for x in client.get("/api/publications").json()["publications"]
    }
    health = client.get("/health").json()
    assert health["quarantined_certificates"] >= 1
    assert health["quarantined_publications"] >= 1


def test_earlier_valid_release_becomes_queryable_stale_after_quarantine(client):
    import_csv(client, CSV_HEADER + f"P-01,{BASE},9.0\n")
    add_cert(client, "P-01", BASE, BASE + 1440, 0.1)
    good = client.post("/api/publications",
                       json={"probe_id": "P-01", "day": DAY}).json()

    # Legacy bug: a second, polluted row was inserted on top while still 'serving'.
    conn = _raw_connect()
    bad = json.dumps({"probe_id": "P-01", "day": DAY_IDX,
                      "segments": [{"max_calibrated_c": float("-inf")}]})
    conn.execute(
        "INSERT INTO publications(probe_id, day, data_version, status, payload) "
        "VALUES ('P-01', ?, 7, 'serving', ?)", (DAY_IDX, bad))
    conn.commit()
    conn.close()

    db.init_db()

    current = client.get("/api/publications?probe_id=P-01").json()["publications"]
    assert len(current) == 1
    assert current[0]["publication_id"] == good["publication_id"]
    assert current[0]["status"] == "stale"  # expired old version, still queryable
    history = client.get(
        "/api/publications?probe_id=P-01&include_history=true").json()["publications"]
    statuses = {h["publication_id"]: h["status"] for h in history}
    assert statuses[good["publication_id"]] == "stale"
    assert "quarantined" in statuses.values()


def test_quarantined_payload_is_returned_with_readable_labels(client):
    conn = _raw_connect()
    conn.execute(
        "INSERT INTO readings(probe_id, minute_utc, temperature_c, data_version) "
        "VALUES ('P-07', ?, 9.0, 1)", (BASE,))
    bad = json.dumps({"probe_id": "P-07", "day": DAY_IDX,
                      "segments": [{"max_calibrated_c": float("inf")}]})
    conn.execute(
        "INSERT INTO publications(probe_id, day, data_version, status, payload) "
        "VALUES ('P-07', ?, 1, 'serving', ?)", (DAY_IDX, bad))
    conn.commit()
    conn.close()

    db.init_db()
    history = client.get(
        "/api/publications?probe_id=P-07&include_history=true").json()["publications"]
    row = history[0]
    assert row["quarantined"] is True
    # non-finite value rendered as an explicit label, not silently dropped to null
    assert row["payload"]["segments"][0]["max_calibrated_c"] == "Infinity"
    # and the whole envelope is still strict, browser-parseable JSON
    json.loads(client.get("/api/publications?include_history=true").text,
               parse_constant=lambda c: (_ for _ in ()).throw(AssertionError(c)))


def test_repair_cert_and_republish_restores_serving_and_keeps_history(client):
    import_csv(client, CSV_HEADER + f"P-03,{BASE},9.0\n")
    conn = _raw_connect()
    cur = conn.execute(
        "INSERT INTO calibrations(probe_id, valid_from, valid_to, offset_c, data_version) "
        "VALUES ('P-03', ?, ?, ?, 4)", (BASE, BASE + 1440, float("inf")))
    cert_id = cur.lastrowid
    bad = json.dumps({"probe_id": "P-03", "day": DAY_IDX,
                      "segments": [{"max_calibrated_c": float("inf")}]})
    conn.execute(
        "INSERT INTO publications(probe_id, day, data_version, status, payload) "
        "VALUES ('P-03', ?, 4, 'serving', ?)", (DAY_IDX, bad))
    conn.commit()
    conn.close()
    db.init_db()

    # A polluted cert cannot be edited without supplying a finite offset.
    r = client.patch(f"/api/certificates/{cert_id}", headers=H,
                     content='{"valid_to":%d}' % (BASE + 2000))
    assert r.status_code == 400

    # Repair with a finite offset, then republish.
    assert client.patch(f"/api/certificates/{cert_id}",
                        json={"offset_c": 0.2}).status_code == 200
    repub = client.post("/api/publications",
                        json={"probe_id": "P-03", "day": DAY})
    assert repub.status_code == 200 and repub.json()["status"] == "serving"

    current = client.get("/api/publications?probe_id=P-03").json()["publications"]
    assert len(current) == 1 and current[0]["status"] == "serving"
    history = client.get(
        "/api/publications?probe_id=P-03&include_history=true").json()["publications"]
    statuses = [h["status"] for h in history]
    assert "quarantined" in statuses and statuses[-1] == "serving"
    # repaired certificate disappears from the quarantine report
    report = client.get("/api/quarantine").json()
    assert all(c["id"] != cert_id for c in report["polluted_certificates"])


def test_migration_idempotent_and_safe_on_clean_database(client):
    db.init_db()
    db.init_db()  # running again must not change or re-flag anything
    report = client.get("/api/quarantine").json()
    assert report["certificate_count"] == 0 and report["publication_count"] == 0
    add_cert(client, "P-01", BASE, BASE + 10, 0.1)
    certs = client.get("/api/certificates?probe_id=P-01").json()["certificates"]
    assert certs[0]["finite"] is True and certs[0]["quarantined"] is False


def test_restart_keeps_repaired_serving_and_never_repromotes(client):
    # A repaired + republished day must stay serving across service restarts,
    # even though its older quarantined historical row is still present.
    conn = _raw_connect()
    conn.execute(
        "INSERT INTO readings(probe_id, minute_utc, temperature_c, data_version) "
        "VALUES ('P-06', ?, 9.0, 1)", (BASE,))
    conn.execute(
        "INSERT INTO calibrations(probe_id, valid_from, valid_to, offset_c, data_version) "
        "VALUES ('P-06', ?, ?, ?, 1)", (BASE, BASE + 1440, float("inf")))
    conn.execute(
        "INSERT INTO publications(probe_id, day, data_version, status, payload) "
        "VALUES ('P-06', ?, 1, 'serving', ?)",
        (DAY_IDX, json.dumps({"segments": [{"max_calibrated_c": float("inf")}]})))
    conn.commit()
    conn.close()

    db.init_db()  # first startup: quarantine the polluted rows
    cid = client.get("/api/certificates?probe_id=P-06").json()["certificates"][0]["id"]
    client.patch(f"/api/certificates/{cid}", json={"offset_c": 0.2})
    repub = client.post("/api/publications",
                        json={"probe_id": "P-06", "day": DAY}).json()
    assert repub["status"] == "serving"
    new_id = repub["publication_id"]

    db.init_db()  # restart: must not demote the healthy serving row
    current = client.get("/api/publications?probe_id=P-06").json()["publications"]
    assert len(current) == 1
    assert current[0]["publication_id"] == new_id
    assert current[0]["status"] == "serving"

    db.init_db()  # and again, still stable
    current = client.get("/api/publications?probe_id=P-06").json()["publications"]
    assert current[0]["status"] == "serving"


def test_recompute_all_records_failure_without_touching_other_days(client):
    # P-OK recomputes fine; P-BAD fails on overflow, independently.
    import_csv(client, CSV_HEADER + f"P-OK,{BASE},9.0\nP-BAD,{BASE},1.5e308\n")
    add_cert(client, "P-OK", BASE, BASE + 1440, 0.0)
    add_cert(client, "P-BAD", BASE, BASE + 1440, 1.5e308)
    out = client.post("/api/publications/recompute-all").json()
    by_probe = {r["probe_id"]: r for r in out["results"]}
    assert by_probe["P-OK"]["ok"] is True
    assert by_probe["P-BAD"]["ok"] is False
    assert out["succeeded"] == 1 and out["failed"] == 1
    assert client.get("/api/publications?probe_id=P-OK").json()["count"] == 1
    assert client.get("/api/publications?probe_id=P-BAD").json()["count"] == 0
