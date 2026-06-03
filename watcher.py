"""
JobWatcher — NetSuite prospect signal monitor
Searches job boards for finance leadership hires that signal ERP pain.
Uses OpenAI to evaluate each posting rather than hardcoded keyword rules.
"""

import asyncio
import discord
import json
import os
import random
import re
import threading
import time
import logging
import warnings
import urllib3
from datetime import datetime, timezone, timedelta
from pathlib import Path

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=ResourceWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import requests
from dotenv import load_dotenv
from jobspy import scrape_jobs
from openai import OpenAI

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
if not DISCORD_WEBHOOK_URL:
    raise RuntimeError("DISCORD_WEBHOOK_URL not set in .env")

WEST_WEBHOOK_URL = os.getenv("WEST_WEBHOOK_URL")
if not WEST_WEBHOOK_URL:
    raise RuntimeError("WEST_WEBHOOK_URL not set in .env")

THOMAS_WEBHOOK_URL = os.getenv("THOMAS_WEBHOOK_URL")

BRIGHTDATA_API_KEY  = os.getenv("BRIGHTDATA_API_KEY")
BRIGHTDATA_DATASET  = "gd_m0ci4a4ivx3j5l6nx"

DISCORD_BOT_TOKEN  = os.getenv("DISCORD_BOT_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY not set in .env")

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
if not SERPER_API_KEY:
    raise RuntimeError("SERPER_API_KEY not set in .env")

openai_client = OpenAI(api_key=OPENAI_API_KEY)

SEEN_FILE              = Path(__file__).parent / "seen_jobs.json"
COMPANY_CACHE_FILE     = Path(__file__).parent / "company_cache.json"
LOG_FILE               = Path(__file__).parent / "watcher.log"
DIGEST_FILE            = Path(__file__).parent / "digest_log.json"
CUSTOM_BLOCKLIST_FILE  = Path(__file__).parent / "custom_blocklist.json"
SEEN_COMPANIES_FILE    = Path(__file__).parent / "seen_companies.json"

# Maximum estimated company revenue to alert on (millions USD).
# Companies confirmed over this are skipped. Unknown revenue passes through.
MAX_REVENUE_MILLION = 50

# Search every N hours
SEARCH_INTERVAL_HOURS = 3
HOURS_OLD = SEARCH_INTERVAL_HOURS + 1

# US states to search
US_LOCATIONS = [
    "Washington State", "Oregon", "Idaho", "Montana",
    "North Dakota", "South Dakota", "Minnesota", "Nebraska",
    "Kansas", "Oklahoma", "Colorado", "Wyoming", "New Mexico",
    "Arizona", "Utah", "Nevada", "California", "Alaska", "Hawaii",
]

# Canadian provinces to search
CA_LOCATIONS = [
    "Yukon", "Northwest Territories", "British Columbia",
    "Alberta", "Saskatchewan",
]

# West territory — company HQ must be in one of these to route to the West channel.
WEST_STATES = {
    # Full names
    "alaska", "british columbia", "california", "hawaii", "idaho",
    "northern mariana islands", "northwest territories", "nunavut",
    "nevada", "oregon", "utah", "washington", "yukon",
    # Abbreviations
    "ak", "bc", "ca", "hi", "id", "mp", "nt", "nu", "nv", "or", "ut", "wa", "yt",
}

# Industry keywords that qualify a lead for the West channel.
WEST_INDUSTRIES = [
    "agriculture", "farming", "livestock", "crop",
    "manufacturing", "aerospace", "defense", "automotive", "electronics",
    "furniture", "textiles", "apparel", "chemicals", "industrial",
    "food", "beverage", "packaged food",
    "retail", "grocery", "e-commerce", "ecommerce", "wholesale", "distribution",
    "consumer goods",
]


def is_west_lead(enrichment: dict, ai: dict) -> bool:
    """Return True if the company HQ is in the West territory AND industry matches."""
    hq = enrichment.get("hq_state", "").strip().lower()
    industry = (ai.get("industry") or "").lower()
    in_west = any(state in hq for state in WEST_STATES) if hq else False
    in_industry = any(kw in industry for kw in WEST_INDUSTRIES)
    return in_west and in_industry


# Thomas's territory — eastern US states
THOMAS_LOCATIONS = [
    "Maine", "New Hampshire", "Vermont", "Massachusetts", "Rhode Island",
    "Connecticut", "New York", "Pennsylvania", "New Jersey", "Delaware",
    "Maryland", "Virginia", "Ohio", "Indiana", "Illinois", "Michigan", "Wisconsin",
]

THOMAS_LOCATION_SET = {loc.lower() for loc in THOMAS_LOCATIONS} | {
    "maine", "new hampshire", "vermont", "massachusetts", "rhode island",
    "connecticut", "new york", "pennsylvania", "new jersey", "delaware",
    "maryland", "virginia", "ohio", "indiana", "illinois", "michigan", "wisconsin",
}

# HQ-based state set for routing (full names + abbreviations)
THOMAS_STATES = {
    "maine", "new hampshire", "vermont", "massachusetts", "rhode island",
    "connecticut", "new york", "pennsylvania", "new jersey", "delaware",
    "maryland", "virginia", "ohio", "indiana", "illinois", "michigan", "wisconsin",
    "me", "nh", "vt", "ma", "ri", "ct", "ny", "pa", "nj", "de", "md", "va",
    "oh", "in", "il", "mi", "wi",
}


def is_thomas_lead(enrichment: dict) -> bool:
    """Return True if the company HQ is in Thomas's territory."""
    hq = enrichment.get("hq_state", "").strip().lower()
    if not hq:
        return False
    return any(state in hq for state in THOMAS_STATES)

# Allowed location terms — any job location must contain at least one of these (case-insensitive).
# Jobs with no location ("Unknown Location") are always allowed through.
ALLOWED_LOCATIONS = {loc.lower() for loc in US_LOCATIONS + CA_LOCATIONS + THOMAS_LOCATIONS} | {
    # Full name alternates (no abbreviations — too short, cause false matches e.g. "co" in "mexico")
    "washington", "oregon", "idaho", "montana", "minnesota", "nebraska",
    "kansas", "oklahoma", "colorado", "wyoming", "nevada", "california",
    "alaska", "hawaii", "alberta", "saskatchewan", "british columbia",
    "yukon", "northwest territories",
}

def is_allowed_location(location: str) -> bool:
    """Return True if location is in target territory or unknown."""
    loc = location.strip().lower()
    if not loc or loc == "unknown location":
        return True
    return any(allowed in loc for allowed in ALLOWED_LOCATIONS)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("jobwatcher")

# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

SEEN_MAX_DAYS = 7        # prune job IDs older than this many days
COMPANY_COOLDOWN_DAYS = 30  # suppress repeat alerts for the same company


def load_seen() -> dict:
    """Load seen jobs as {job_id: iso_timestamp}. Prunes entries older than SEEN_MAX_DAYS."""
    if SEEN_FILE.exists():
        try:
            data = json.loads(SEEN_FILE.read_text())
            # Support old format (plain list) by converting to dict with today's date
            if isinstance(data, list):
                data = {jid: datetime.now().isoformat() for jid in data}
            cutoff = (datetime.now() - timedelta(days=SEEN_MAX_DAYS)).isoformat()
            return {jid: ts for jid, ts in data.items() if ts >= cutoff}
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_seen(seen: dict) -> None:
    SEEN_FILE.write_text(json.dumps(seen, indent=2))


def load_seen_companies() -> dict:
    """Load alerted companies as {company_key: iso_timestamp}. Prunes entries older than COMPANY_COOLDOWN_DAYS."""
    if SEEN_COMPANIES_FILE.exists():
        try:
            data = json.loads(SEEN_COMPANIES_FILE.read_text())
            cutoff = (datetime.now() - timedelta(days=COMPANY_COOLDOWN_DAYS)).isoformat()
            return {k: ts for k, ts in data.items() if ts >= cutoff}
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_seen_companies(seen_companies: dict) -> None:
    SEEN_COMPANIES_FILE.write_text(json.dumps(seen_companies, indent=2))


def company_key(company: str) -> str:
    """Normalised company name used as the cooldown key."""
    return re.sub(r"[^a-z0-9]", "", company.lower())


def job_id(row) -> str:
    """Primary dedupe key — board native ID or URL."""
    jid = str(row.get("id") or "").strip()
    if jid and jid != "nan":
        return jid
    url = str(row.get("job_url") or "").strip()
    return url or f"{row.get('company','?')}|{row.get('title','?')}"


def company_title_key(row) -> str:
    """Secondary dedupe key — catches same posting returned by multiple location searches."""
    company = str(row.get("company") or "").strip().lower()
    title   = str(row.get("title") or "").strip().lower()
    return f"{company}|{title}"


# ---------------------------------------------------------------------------
# Pre-filter: staffing firm blocklist (runs before AI — zero cost)
# ---------------------------------------------------------------------------

# Known staffing/recruiting firm names (lowercase, partial match)
STAFFING_FIRM_NAMES = {
    "robert half", "kforce", "randstad", "vaco", "cybercoders", "michael page",
    "heidrick", "spencer stuart", "korn ferry", "lhh", "adecco", "manpower",
    "staffmark", "aerotek", "insight global", "beacon hill", "parker lynch",
    "creative financial staffing", "cfs", "ledgent", "roth staffing",
    "accountingfly", "staffers", "hirenetworks", "tatum", "scion",
    "venteon", "brilliant financial", "versique", "lancesoft", "apex systems",
    "staffing solutions", "the select group", "naviga", "sanford rose",
}

# Red-flag words in company name that strongly indicate a recruiter
STAFFING_NAME_KEYWORDS = [
    "staffing", "recruiting", "recruitment", "headhunter", "search group",
    "search partners", "executive search", "talent acquisition", "talent solutions",
    "workforce solutions", "placement", "temp agency", "contract staffing",
]

# User-flagged firms added via 👎 reaction in Discord
def load_custom_blocklist() -> list:
    if CUSTOM_BLOCKLIST_FILE.exists():
        try:
            return json.loads(CUSTOM_BLOCKLIST_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return []


def save_custom_blocklist(blocklist: list) -> None:
    CUSTOM_BLOCKLIST_FILE.write_text(json.dumps(sorted(blocklist), indent=2))


def add_to_custom_blocklist(company: str) -> None:
    entry = company.strip().lower()
    if entry not in _custom_blocklist:
        _custom_blocklist.append(entry)
        save_custom_blocklist(_custom_blocklist)
        log.info("Added '%s' to custom staffing blocklist", company)


_custom_blocklist: list = load_custom_blocklist()


# Red-flag phrases in the job description body
STAFFING_DESCRIPTION_PHRASES = [
    "our client is", "our client has", "on behalf of our client",
    "our client, a", "confidential client", "our client company",
    "working with a client", "placed at our client", "client of ours",
]


TITLE_ALLOW = [
    "controller", "cfo", "chief financial officer", "fractional cfo",
    "vp of finance", "vp finance", "vice president of finance",
    "vice president finance", "director of finance", "finance director",
    "financial controller", "corporate controller", "assistant controller",
    "finance manager", "vp accounting", "vp of accounting",
    "vice president of accounting", "vice president accounting",
    "director of accounting", "accounting director",
    "head of finance", "head of accounting",
]

TITLE_BLOCK = [
    "plant controller", "inventory controller", "project controller",
    "production controller", "document controller", "cost controller",
    "stock controller", "logistics controller", "traffic controller",
    "accounting manager", "accounts payable", "accounts receivable",
    "bookkeeper", "payroll", "staff accountant", "senior accountant",
]


def is_target_title(title: str) -> bool:
    """Return True only if the title is a finance leadership role we care about."""
    t = title.lower()
    # Block known noise titles first
    for blocked in TITLE_BLOCK:
        if blocked in t:
            return False
    # Must match at least one allowed title
    return any(allowed in t for allowed in TITLE_ALLOW)


def is_staffing_firm(company: str, description: str) -> bool:
    """Returns True if the posting is from a recruiter or staffing agency."""
    co = company.lower()
    desc = description.lower()

    # User-flagged firms (via 👎 reaction in Discord)
    for entry in _custom_blocklist:
        if entry in co:
            return True

    # Exact known firm match
    for firm in STAFFING_FIRM_NAMES:
        if firm in co:
            return True

    # Red-flag keyword in company name
    for kw in STAFFING_NAME_KEYWORDS:
        if kw in co:
            return True

    # Hidden client language in description
    for phrase in STAFFING_DESCRIPTION_PHRASES:
        if phrase in desc:
            return True

    return False


# ---------------------------------------------------------------------------
# Keyword tiers — deterministic scan (free, no API cost)
# ---------------------------------------------------------------------------

# Tier 1: direct buying signals — company is actively evaluating or moving ERP
TIER1_KEYWORDS = [
    "netsuite",
    "erp evaluation", "erp selection", "erp implementation", "erp migration",
    "erp upgrade", "erp project", "new erp", "evaluating erp", "evaluating systems",
    "system evaluation", "system selection", "implementing new system",
    "implementing a new system", "replace our accounting", "erp replacement",
    "financial system implementation", "new accounting system",
]

# Tier 2: legacy / pain signals — strong indicators they need an upgrade
TIER2_KEYWORDS = [
    "quickbooks", "quick books",
    "sage", "xero", "excel-based", "spreadsheets", "spreadsheet",
    "great plains", "acumatica", "intacct", "epicor",
    "oracle", "sap", "dynamics 365", "ms dynamics",
    "manual processes", "manual reporting", "manual consolidation",
]


def find_keywords(description: str) -> tuple[list[str], list[str]]:
    """Return (tier1_matches, tier2_matches) found in the job description."""
    desc = description.lower()
    tier1 = [kw for kw in TIER1_KEYWORDS if kw in desc]
    tier2 = [kw for kw in TIER2_KEYWORDS if kw in desc]
    return tier1, tier2


# ---------------------------------------------------------------------------
# Company enrichment (Serper → Bright Data ZoomInfo scraper)
# Returns: revenue_millions, employees, website, hq_state, industry, revenue_confidence
# Falls back to basic Serper+OpenAI if Bright Data fails or no ZoomInfo URL found.
# ---------------------------------------------------------------------------

def load_company_cache() -> dict:
    if COMPANY_CACHE_FILE.exists():
        try:
            return json.loads(COMPANY_CACHE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_company_cache(cache: dict) -> None:
    COMPANY_CACHE_FILE.write_text(json.dumps(cache, indent=2))


_company_cache: dict = load_company_cache()


def serper_find_zoominfo_url(company: str) -> str | None:
    """Search Google for the company's ZoomInfo profile URL."""
    try:
        r = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            json={"q": f"{company} zoominfo", "num": 5},
            timeout=10,
        )
        r.raise_for_status()
        for item in r.json().get("organic", []):
            link = item.get("link", "")
            if "zoominfo.com/c/" in link:
                return link
    except Exception as e:
        log.warning("Serper ZoomInfo URL search failed for '%s': %s", company, e)
    return None


def brightdata_scrape_zoominfo(zoominfo_url: str) -> dict | None:
    """Trigger a Bright Data ZoomInfo scrape and poll until complete."""
    if not BRIGHTDATA_API_KEY:
        return None
    try:
        # Trigger async scrape
        r = requests.post(
            f"https://api.brightdata.com/datasets/v3/trigger?dataset_id={BRIGHTDATA_DATASET}&format=json",
            headers={"Authorization": f"Bearer {BRIGHTDATA_API_KEY}", "Content-Type": "application/json"},
            json={"input": [{"url": zoominfo_url}]},
            timeout=30,
        )
        r.raise_for_status()
        snapshot_id = r.json().get("snapshot_id")
        if not snapshot_id:
            return None

        # Poll for results (up to 3 minutes)
        for _ in range(36):
            time.sleep(5)
            poll = requests.get(
                f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}?format=json",
                headers={"Authorization": f"Bearer {BRIGHTDATA_API_KEY}"},
                timeout=30,
            )
            if poll.status_code == 200:
                data = poll.json()
                if data and isinstance(data, list):
                    return data[0]
            elif poll.status_code != 202:
                log.warning("Bright Data poll error: %s", poll.status_code)
                return None
    except Exception as e:
        log.warning("Bright Data scrape failed for '%s': %s", zoominfo_url, e)
    return None


FALLBACK_ENRICHMENT_SYSTEM = """You are a company research analyst. Given Google search results,
extract or estimate: revenue_millions (number), employees (integer), website (URL),
hq_state (US state or Canadian province, full name), revenue_confidence (high/medium/low).
Always provide numbers — never null. Make best estimates if data is missing.
Reply ONLY with this JSON:
{"revenue_millions": <number>, "employees": <integer>, "website": "<url>",
 "revenue_confidence": "<high|medium|low>", "hq_state": "<state or empty>"}"""


def enrich_company_fallback(company: str) -> dict:
    """Fallback enrichment using Serper snippets + OpenAI when Bright Data unavailable."""
    try:
        r = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            json={"q": f"{company} zoominfo industry revenue", "num": 10},
            timeout=10,
        )
        r.raise_for_status()
        snippets = [f"{i.get('title','')}\n{i.get('snippet','')}" for i in r.json().get("organic", [])]
        context = "\n\n---\n\n".join(snippets[:10])
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": FALLBACK_ENRICHMENT_SYSTEM},
                {"role": "user", "content": f"Company: {company}\n\nSearch results:\n{context}"},
            ],
            max_tokens=160,
            temperature=0,
        )
        result = json.loads(resp.choices[0].message.content.strip())
    except Exception as e:
        log.warning("Fallback enrichment failed for %s: %s", company, e)
        result = {}
    result.setdefault("revenue_millions", 1.0)
    result.setdefault("employees", 10)
    result.setdefault("website", "")
    result.setdefault("revenue_confidence", "low")
    result.setdefault("hq_state", "")
    result.setdefault("industry_enriched", "")
    return result


def enrich_company(company: str) -> dict:
    """
    Enrich a company using Bright Data's ZoomInfo scraper.
    Falls back to Serper+OpenAI if Bright Data is unavailable or ZoomInfo URL not found.
    Results are cached so each company is only looked up once.
    """
    key = company.strip().lower()
    if key in _company_cache:
        return _company_cache[key]

    result = None
    zoominfo_url = serper_find_zoominfo_url(company)

    if zoominfo_url:
        log.info("  ZoomInfo URL: %s", zoominfo_url)
        bd_data = brightdata_scrape_zoominfo(zoominfo_url)
        if bd_data:
            industries = bd_data.get("industry") or []
            industry_str = ", ".join(industries) if isinstance(industries, list) else str(industries)
            hq = bd_data.get("headquarters") or ""
            # Extract state from full address string e.g. "123 St, City, California, 90210, United States"
            hq_state = ""
            if hq:
                parts = [p.strip() for p in hq.split(",")]
                if len(parts) >= 3:
                    hq_state = parts[-3]
            result = {
                "revenue_millions": (bd_data.get("revenue") or 0) / 1_000_000 if bd_data.get("revenue") else 1.0,
                "employees": bd_data.get("total_employees") or 10,
                "website": bd_data.get("website") or "",
                "revenue_confidence": "high" if bd_data.get("revenue") else "low",
                "hq_state": hq_state,
                "industry_enriched": industry_str,
                "tech_stack": [t.get("tech_name", "") for t in (bd_data.get("tech_stack") or [])],
                "source": "brightdata",
            }
            log.info("  [BrightData] '%s': $%.1fM rev, %s employees, %s, %s",
                     company, result["revenue_millions"], result["employees"],
                     result["hq_state"], industry_str)

    if not result:
        log.info("  Falling back to Serper+OpenAI for '%s'", company)
        result = enrich_company_fallback(company)
        result["source"] = "fallback"
        log.info("  [Fallback] '%s': $%.1fM rev, %s employees, %s",
                 company, result["revenue_millions"], result["employees"], result.get("website", ""))

    _company_cache[key] = result
    save_company_cache(_company_cache)
    return result


def revenue_over_limit(company: str) -> bool:
    data = enrich_company(company)
    return data["revenue_millions"] > MAX_REVENUE_MILLION


# ---------------------------------------------------------------------------
# Stage 1 — Quick AI filter (job text only, no Serper credits spent)
# ---------------------------------------------------------------------------

STAGE1_SYSTEM = """You are a filter for a NetSuite ERP sales rep.

Your ONLY job is to decide if this job posting is worth researching further. \
Be generous — if there is any chance it could be a legit ERP prospect, pass it through.

IMMEDIATELY REJECT (return false) if:
- The hiring company is a staffing agency, recruiter, headhunter, or executive search firm \
  (Robert Half, Kforce, Randstad, Vaco, CyberCoders, Michael Page, etc.)
- The posting is for a "client" — the real employer is hidden
- The role is purely bookkeeping, accounts payable, or payroll with no systems scope
- The company is clearly a large enterprise (publicly traded, thousands of employees)

PASS THROUGH (return true) if:
- Role is Controller, CFO, "Chief Financial Officer", or "Fractional CFO"
- There is a mention of QuickBooks, ERP, NetSuite, or improving finance operations, evaluating current systems

Reply ONLY with this JSON:
{
  "pass": true or false,
  "reason": "one sentence"
}"""


def ai_stage1_filter(title: str, company: str, description: str) -> tuple[bool, str]:
    """Quick cheap filter — pass/fail with one-line reason."""
    desc_snippet = (description or "")[:1500]
    user_msg = f"Job Title: {title}\nCompany: {company}\n\nDescription:\n{desc_snippet}"
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": STAGE1_SYSTEM},
                {"role": "user",   "content": user_msg},
            ],
            max_tokens=60,
            temperature=0,
        )
        data = json.loads(resp.choices[0].message.content.strip())
        return bool(data.get("pass")), str(data.get("reason", ""))
    except Exception as e:
        log.warning("Stage1 filter failed for %s @ %s: %s", title, company, e)
        return True, "filter error — passing through"


# ---------------------------------------------------------------------------
# Stage 3 — Final AI verdict (job text + enrichment data)
# ---------------------------------------------------------------------------

STAGE3_SYSTEM = """You are a senior sales intelligence analyst for a NetSuite ERP sales rep \
targeting companies with $1M–$15M in annual revenue.

You will receive a job posting AND enriched company data (revenue estimate, employee count, \
website, industry). Make a final call on whether this company is a strong NetSuite prospect.

A strong prospect:
- Is a real end-user company (not a staffing firm or recruiter)
- Has estimated revenue under $15M
- The job posting signals ERP pain: outgrowing current software, implementing new systems, \
  mentions of QuickBooks/Sage/Xero/spreadsheets, scaling finance team, Series A/B, \
  audit readiness, month-end close challenges
- Is hiring a Controller or CFO — a decision-maker who would buy or champion NetSuite

Reply ONLY with this exact JSON:
{
  "is_prospect": true or false,
  "confidence": "high", "medium", or "low",
  "industry": "short label e.g. SaaS, E-commerce, Manufacturing",
  "erp_signals": ["tag1", "tag2"],
  "summary": "2-3 sentences: first, describe what the company does and their business model; then describe what their current finance/systems pain point appears to be based on the posting"
}

For erp_signals use only: "QuickBooks User", "ERP Migration", "Rapid Growth", "Series A/B", \
"System Implementation", "Leadership Change", "Scaling Finance Team", "Spreadsheet Dependent", \
"Month-End Close Pain", "Audit Readiness", "Sage User", "Xero User", "New ERP Search"."""


def ai_stage3_verdict(title: str, company: str, description: str, enrichment: dict) -> dict:
    """Final verdict using job text + enrichment data."""
    desc_snippet = (description or "")[:2500]
    enrich_str = (
        f"Revenue estimate: ~${enrichment.get('revenue_millions', '?')}M "
        f"({enrichment.get('revenue_confidence', '?')} confidence)\n"
        f"Employees: {enrichment.get('employees', '?')}\n"
        f"Website: {enrichment.get('website', 'unknown')}\n"
        f"Industry (from web): {enrichment.get('industry_web', '')}"
    )
    user_msg = (
        f"Job Title: {title}\nCompany: {company}\n\n"
        f"--- Company Enrichment ---\n{enrich_str}\n\n"
        f"--- Job Description ---\n{desc_snippet}"
    )
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": STAGE3_SYSTEM},
                {"role": "user",   "content": user_msg},
            ],
            max_tokens=300,
            temperature=0,
        )
        return json.loads(resp.choices[0].message.content.strip())
    except Exception as e:
        log.warning("Stage3 verdict failed for %s @ %s: %s", title, company, e)
        return {"is_prospect": False, "confidence": "low", "industry": "", "erp_signals": [], "summary": "Evaluation error."}

# ---------------------------------------------------------------------------
# Digest log — tracks every flagged company for scheduled CSV summaries
# ---------------------------------------------------------------------------

def load_digest_log() -> list:
    if DIGEST_FILE.exists():
        try:
            return json.loads(DIGEST_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return []


def append_digest_entry(company: str, website: str, title: str, job_url: str,
                        location: str, tier1_hits: list, tier2_hits: list,
                        description: str) -> None:
    entries = load_digest_log()
    entries.append({
        "company": company,
        "website": website,
        "job_title": title,
        "job_url": job_url,
        "location": location,
        "hot_keywords": ", ".join(tier1_hits),
        "legacy_keywords": ", ".join(tier2_hits),
        "description_snippet": (description or "")[:2000],
        "timestamp": datetime.now().isoformat(),
    })
    DIGEST_FILE.write_text(json.dumps(entries, indent=2))


def send_full_digest() -> None:
    """Send all accumulated leads as a CSV, then clear the log."""
    entries = load_digest_log()

    if not entries:
        try:
            requests.post(
                DISCORD_WEBHOOK_URL,
                json={"content": "No leads in the current list — nothing to export yet."},
                timeout=10,
            ).raise_for_status()
        except requests.RequestException as e:
            log.error("Digest send failed: %s", e)
        return

    csv_lines = ["Company Name,Website,Job Title,Job URL,Location,Hot Keywords,Legacy Keywords,Job Description Snippet"]
    for e in entries:
        def esc(val):
            return str(e.get(val, "")).replace('"', '""')
        csv_lines.append(
            f'"{esc("company")}","{esc("website")}","{esc("job_title")}","{esc("job_url")}",'
            f'"{esc("location")}","{esc("hot_keywords")}","{esc("legacy_keywords")}","{esc("description_snippet")}"'
        )
    csv_content = "\n".join(csv_lines)

    try:
        r = requests.post(
            DISCORD_WEBHOOK_URL,
            files={"file": ("leads.csv", csv_content.encode(), "text/csv")},
            data={"payload_json": json.dumps({"content": f"**Leads Export** — {len(entries)} prospect(s). List cleared."})},
            timeout=10,
        )
        r.raise_for_status()
        DIGEST_FILE.write_text(json.dumps([], indent=2))
        log.info("Full digest sent and cleared (%d entries)", len(entries))
    except requests.RequestException as e:
        log.error("Digest send failed: %s", e)


# ---------------------------------------------------------------------------
# Discord alert
# ---------------------------------------------------------------------------

COLOR_HIGH = 0x57F287   # green  — high confidence
COLOR_MED  = 0xFEE75C   # yellow — medium confidence
COLOR_LOW  = 0x95A5A6   # grey   — low confidence

CONFIDENCE_COLOR = {"high": COLOR_HIGH, "medium": COLOR_MED, "low": COLOR_LOW}


def build_embed(row, ai: dict, enrichment: dict, tier1_hits: list, tier2_hits: list) -> dict:
    company   = row.get("company") or "Unknown Company"
    title     = row.get("title") or "Unknown Title"
    location  = row.get("location") or "Unknown Location"
    job_url   = row.get("job_url") or ""
    site      = str(row.get("site") or "").capitalize()
    date_post = row.get("date_posted")

    confidence  = ai.get("confidence", "low")
    erp_signals = ai.get("erp_signals", [])
    summary     = ai.get("summary", "")
    industry    = ai.get("industry", "")

    rev        = enrichment.get("revenue_millions", 1.0)
    employees  = enrichment.get("employees", "?")
    website    = enrichment.get("website", "")
    rev_conf   = enrichment.get("revenue_confidence", "low")
    hq_state   = enrichment.get("hq_state", "")
    industry_enriched = enrichment.get("industry_enriched", "")
    tech_stack = enrichment.get("tech_stack", [])
    enrich_source = enrichment.get("source", "")

    rev_str = f"${rev:.1f}M" if rev_conf == "high" else f"~${rev:.1f}M ({rev_conf} confidence)"
    company_display = f"[{company}]({website})" if website else company

    signals_str = " ".join(f"`{s}`" for s in erp_signals) if erp_signals else "`No signals detected`"

    date_str = ""
    if date_post and str(date_post).lower() not in ("nan", "none", "nat", ""):
        date_str = str(date_post)[:10]
    source_label = f"Source: {site}" + (f"  •  ZoomInfo ✓" if enrich_source == "brightdata" else "")
    footer_text = f"{source_label}  •  {date_str}" if date_str else source_label

    # Tier 1 hits force green regardless of AI confidence
    color = COLOR_HIGH if tier1_hits else CONFIDENCE_COLOR.get(confidence, COLOR_LOW)

    fields = [
        {"name": "📰 Job Posting", "value": f"[{title}]({job_url})" if job_url else title, "inline": False},
        {"name": "📍 Location",    "value": location, "inline": True},
        {"name": "💰 Revenue",     "value": rev_str,  "inline": True},
        {"name": "👥 Employees",   "value": str(employees), "inline": True},
    ]

    if hq_state:
        fields.append({"name": "🏠 HQ", "value": hq_state, "inline": True})

    display_industry = industry_enriched or industry
    if display_industry:
        fields.append({"name": "🏭 Industry", "value": display_industry, "inline": True})

    if tech_stack:
        fields.append({"name": "🖥️ Tech Stack", "value": ", ".join(tech_stack[:6]), "inline": False})

    if tier1_hits:
        fields.append({"name": "🔥 Hot Keywords", "value": " ".join(f"`{kw}`" for kw in tier1_hits), "inline": False})

    if tier2_hits:
        fields.append({"name": "⚠️ Legacy System Keywords", "value": " ".join(f"`{kw}`" for kw in tier2_hits), "inline": False})

    fields.append({"name": "⚡ ERP Signals", "value": signals_str, "inline": False})
    fields.append({"name": "📝 Summary",     "value": summary,     "inline": False})

    return {
        "title": f"🎯 {company_display}",
        "color": color,
        "fields": fields,
        "footer": {"text": footer_text},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_discord(embed: dict, webhook_url: str = DISCORD_WEBHOOK_URL) -> None:
    try:
        r = requests.post(webhook_url, json={"embeds": [embed]}, timeout=10)
        r.raise_for_status()
    except requests.RequestException as e:
        log.error("Discord send failed: %s", e)

# ---------------------------------------------------------------------------
# Core search loop
# ---------------------------------------------------------------------------

def run_search() -> None:
    log.info("--- Starting search run ---")
    seen = load_seen()
    seen_companies = load_seen_companies()
    seen_this_run: set = set()  # catches dupes across location searches in the same run
    new_count = 0

    search_term = (
        'Controller OR CFO OR "Chief Financial Officer" OR "Fractional CFO" '
        'OR "Director of Finance" OR "Finance Director" OR "Head of Finance" '
        'OR "VP Finance" OR "VP of Finance" OR "Vice President of Finance" '
        'OR "VP Accounting" OR "VP of Accounting" OR "Director of Accounting" '
        'OR "Head of Accounting" OR "Finance Manager"'
    )

    location_list = (
        [("USA", loc) for loc in US_LOCATIONS] +
        [("Canada", loc) for loc in CA_LOCATIONS]
    )

    for country, location in location_list:
        log.info("Searching %s, %s …", location, country)
        try:
            df = scrape_jobs(
                site_name=["indeed", "linkedin"],
                search_term=search_term,
                location=location,
                results_wanted=50,
                hours_old=HOURS_OLD,
                country_indeed=country,
                linkedin_fetch_description=True,
            )
        except Exception as e:
            log.error("Scrape error for %s: %s", location, e)
            continue

        if df is None or df.empty:
            log.info("  No results for %s", location)
            continue

        log.info("  Got %d postings from %s", len(df), location)

        for _, row in df.iterrows():
            jid = job_id(row)
            ctk = company_title_key(row)
            if jid in seen or ctk in seen_this_run:
                continue
            seen_this_run.add(ctk)

            company     = str(row.get("company") or "").strip()
            title       = str(row.get("title") or "").strip()
            description = str(row.get("description") or "").strip()

            # ── Pre-filter: staffing firm blocklist (free) ────────────────
            if is_staffing_firm(company, description):
                log.info("  STAFFING SKIP %s @ %s", title, company)
                continue

            # ── Pre-filter: title check (free) ───────────────────────────
            if not is_target_title(title):
                log.info("  TITLE SKIP %s @ %s", title, company)
                continue

            # ── Stage 1: cheap AI filter ──────────────────────────────────
            passed, reason = ai_stage1_filter(title, company, description)
            if not passed:
                log.info("  S1 SKIP %s @ %s — %s", title, company, reason)
                continue
            log.info("  S1 PASS %s @ %s — %s", title, company, reason)

            # ── Company cooldown check (free) ─────────────────────────────
            ck = company_key(company)
            if ck in seen_companies:
                log.info("  COMPANY COOLDOWN %s @ %s (alerted within %dd)", title, company, COMPANY_COOLDOWN_DAYS)
                continue

            # ── Stage 2: Serper enrichment ────────────────────────────────
            enrichment = enrich_company(company) if company else {"revenue_millions": 1.0, "employees": 10, "website": "", "revenue_confidence": "low"}
            est_revenue = enrichment["revenue_millions"]
            if est_revenue > MAX_REVENUE_MILLION:
                log.info("  S2 SKIP (revenue ~$%.0fM) %s @ %s", est_revenue, title, company)
                continue

            # ── Stage 3: final AI verdict with enrichment data ────────────
            ai = ai_stage3_verdict(title, company, description, enrichment)
            if not ai.get("is_prospect"):
                log.info("  S3 SKIP %s @ %s — %s", title, company, ai.get("summary", ""))
                continue

            seen[jid] = datetime.now().isoformat()
            seen_companies[ck] = datetime.now().isoformat()
            new_count += 1
            tier1_hits, tier2_hits = find_keywords(description)
            log.info("  ✓ HIT [%s] %s @ %s | T1:%s T2:%s",
                     ai.get("confidence"), title, company, tier1_hits, tier2_hits)

            append_digest_entry(
                company, enrichment.get("website", ""),
                title, str(row.get("job_url") or ""),
                str(row.get("location") or ""),
                tier1_hits, tier2_hits, description,
            )
            embed = build_embed(row, ai, enrichment, tier1_hits, tier2_hits)
            if is_west_lead(enrichment, ai):
                log.info("  → West channel (%s, %s)", enrichment.get("hq_state"), ai.get("industry"))
                send_discord(embed, WEST_WEBHOOK_URL)
            elif THOMAS_WEBHOOK_URL and is_thomas_lead(enrichment):
                log.info("  → Thomas channel (%s)", enrichment.get("hq_state"))
                send_discord(embed, THOMAS_WEBHOOK_URL)
            else:
                send_discord(embed)
            time.sleep(0.5)

        time.sleep(2)

    save_seen(seen)
    save_seen_companies(seen_companies)
    log.info("--- Run complete. %d new alerts sent. ---", new_count)

# ---------------------------------------------------------------------------
# Discord bot — listens for "csv" and triggers on-demand export
# ---------------------------------------------------------------------------

def run_discord_bot() -> None:
    if not DISCORD_BOT_TOKEN or not DISCORD_CHANNEL_ID:
        log.warning("Discord bot not started — DISCORD_BOT_TOKEN or DISCORD_CHANNEL_ID not set.")
        return

    intents = discord.Intents.default()
    intents.message_content = True
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        log.info("Discord bot connected as %s", client.user)

    @client.event
    async def on_message(message):
        if message.author == client.user:
            return
        if message.channel.id != DISCORD_CHANNEL_ID:
            return
        if message.content.strip().lower() == "csv":
            log.info("CSV trigger received from %s", message.author)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, send_full_digest)

    @client.event
    async def on_raw_reaction_add(payload):
        if payload.user_id == client.user.id:
            return
        if payload.channel_id != DISCORD_CHANNEL_ID:
            return
        if str(payload.emoji) != "👎":
            return

        channel = client.get_channel(payload.channel_id)
        if not channel:
            channel = await client.fetch_channel(payload.channel_id)

        try:
            message = await channel.fetch_message(payload.message_id)
        except discord.NotFound:
            return

        if not message.embeds:
            return

        title = message.embeds[0].title or ""
        if not title.startswith("🎯"):
            return

        # Parse company name — title is either "🎯 Company" or "🎯 [Company](url)"
        raw = title.lstrip("🎯").strip()
        match = re.match(r'^\[(.+?)\]\(.+?\)$', raw)
        company = match.group(1) if match else raw

        if not company:
            return

        add_to_custom_blocklist(company)
        await channel.send(f"👎 Added **{company}** to the staffing blocklist — they'll be skipped from now on.")

    client.run(DISCORD_BOT_TOKEN)


# ---------------------------------------------------------------------------
# Scheduler entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("JobWatcher starting. Interval: every %dh. %d US + %d CA locations.",
             SEARCH_INTERVAL_HOURS, len(US_LOCATIONS), len(CA_LOCATIONS))

    run_search()

    bot_thread = threading.Thread(target=run_discord_bot, daemon=True)
    bot_thread.start()

    while True:
        jitter = random.randint(-10, 10)
        sleep_minutes = (SEARCH_INTERVAL_HOURS * 60) + jitter
        log.info("Next run in %d minutes.", sleep_minutes)
        time.sleep(sleep_minutes * 60)
        run_search()


if __name__ == "__main__":
    main()
