"""
Postgres persistence for alerted job postings — powers the dashboard's
Recent Postings and Stats pages. All functions no-op (return empty/None)
if DATABASE_URL isn't set, so local dev without Postgres doesn't break.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import Json, RealDictCursor

log = logging.getLogger("jobwatcher")

DATABASE_URL = os.getenv("DATABASE_URL")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS postings (
    id SERIAL PRIMARY KEY,
    job_id TEXT,
    company TEXT,
    title TEXT,
    job_url TEXT,
    location TEXT,
    site TEXT,
    date_posted TEXT,
    confidence TEXT,
    erp_signals JSONB,
    summary TEXT,
    tier1_hits JSONB,
    tier2_hits JSONB,
    revenue_millions REAL,
    revenue_confidence TEXT,
    employees INTEGER,
    website TEXT,
    hq_state TEXT,
    industry TEXT,
    tech_stack JSONB,
    enrich_source TEXT,
    alerted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_postings_alerted_at ON postings (alerted_at DESC);
"""


def get_connection():
    if not DATABASE_URL:
        return None
    return psycopg2.connect(DATABASE_URL)


def init_db() -> None:
    conn = get_connection()
    if not conn:
        log.warning("DATABASE_URL not set — Postgres history disabled.")
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        log.info("Postgres postings table ready.")
    finally:
        conn.close()


def insert_posting(row, ai: dict, enrichment: dict, tier1_hits: list, tier2_hits: list) -> None:
    """Persist an alerted posting. Mirrors the fields build_embed() sends to Discord."""
    conn = get_connection()
    if not conn:
        return

    date_post = row.get("date_posted")
    date_str = str(date_post)[:10] if date_post and str(date_post).lower() not in ("nan", "none", "nat", "") else ""

    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO postings (
                    job_id, company, title, job_url, location, site, date_posted,
                    confidence, erp_signals, summary, tier1_hits, tier2_hits,
                    revenue_millions, revenue_confidence, employees, website,
                    hq_state, industry, tech_stack, enrich_source
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                """,
                (
                    str(row.get("id") or ""),
                    str(row.get("company") or "").strip(),
                    str(row.get("title") or "").strip(),
                    str(row.get("job_url") or ""),
                    str(row.get("location") or "").strip(),
                    str(row.get("site") or "").capitalize(),
                    date_str,
                    ai.get("confidence", "low"),
                    Json(ai.get("erp_signals", [])),
                    ai.get("summary", ""),
                    Json(tier1_hits),
                    Json(tier2_hits),
                    enrichment.get("revenue_millions", 1.0),
                    enrichment.get("revenue_confidence", "low"),
                    enrichment.get("employees", 10),
                    enrichment.get("website", ""),
                    enrichment.get("hq_state", ""),
                    enrichment.get("industry_enriched") or ai.get("industry", ""),
                    Json(enrichment.get("tech_stack", [])),
                    enrichment.get("source", ""),
                ),
            )
    except Exception as e:
        log.error("Failed to insert posting into Postgres: %s", e)
    finally:
        conn.close()


def get_recent_postings(limit: int = 25, offset: int = 0) -> list:
    conn = get_connection()
    if not conn:
        return []
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM postings ORDER BY alerted_at DESC LIMIT %s OFFSET %s",
                (limit, offset),
            )
            return cur.fetchall()
    finally:
        conn.close()


def count_postings() -> int:
    conn = get_connection()
    if not conn:
        return 0
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM postings")
            return cur.fetchone()[0]
    finally:
        conn.close()


def get_totals() -> dict:
    conn = get_connection()
    if not conn:
        return {"all_time": 0, "this_week": 0, "this_month": 0}
    now = datetime.now(timezone.utc)
    week_start = now - timedelta(days=7)
    month_start = now - timedelta(days=30)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM postings")
            all_time = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM postings WHERE alerted_at >= %s", (week_start,))
            this_week = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM postings WHERE alerted_at >= %s", (month_start,))
            this_month = cur.fetchone()[0]
        return {"all_time": all_time, "this_week": this_week, "this_month": this_month}
    finally:
        conn.close()


def get_alerts_over_time(days: int = 60) -> list:
    """Daily alert counts for the last N days as [{"day": "2026-06-01", "count": 3}, ...]."""
    conn = get_connection()
    if not conn:
        return []
    since = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT to_char(date_trunc('day', alerted_at), 'YYYY-MM-DD') AS day, count(*) AS count
                FROM postings
                WHERE alerted_at >= %s
                GROUP BY 1
                ORDER BY 1
                """,
                (since,),
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_confidence_breakdown() -> list:
    return _group_count("confidence")


def get_top_industries(limit: int = 8) -> list:
    return _group_count("industry", limit=limit, exclude_empty=True)


def get_top_hq_states(limit: int = 8) -> list:
    return _group_count("hq_state", limit=limit, exclude_empty=True)


def get_source_breakdown() -> list:
    return _group_count("site")


def _group_count(column: str, limit: int | None = None, exclude_empty: bool = False) -> list:
    conn = get_connection()
    if not conn:
        return []
    where_clause = f"WHERE {column} IS NOT NULL AND {column} != ''" if exclude_empty else ""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT {column} AS label, count(*) AS count
                FROM postings
                {where_clause}
                GROUP BY {column}
                ORDER BY count DESC
                {limit_clause}
                """
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_revenue_employee_avgs() -> dict:
    conn = get_connection()
    if not conn:
        return {"avg_revenue": 0, "avg_employees": 0}
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT avg(revenue_millions), avg(employees) FROM postings")
            avg_rev, avg_emp = cur.fetchone()
        return {"avg_revenue": round(avg_rev or 0, 1), "avg_employees": round(avg_emp or 0)}
    finally:
        conn.close()
