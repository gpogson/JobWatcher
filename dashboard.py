"""
JobWatcher Dashboard — Flask web UI for monitoring the watcher pipeline.
Run as a background thread alongside watcher.py.

Two pages:
  /       Recent Postings — mirrors what's sent to Discord, backed by Postgres.
  /stats  History stats on alerted leads + live pipeline health (from the log).
"""

import json
import os
import re
from functools import wraps
from pathlib import Path

from flask import Flask, redirect, render_template_string, request, session

import db

app = Flask(__name__)
app.secret_key = os.environ.get("DASHBOARD_SECRET", os.urandom(24))

BASE_DIR     = Path(__file__).parent
LOG_FILE     = BASE_DIR / "watcher.log"
COMPANY_FILE = BASE_DIR / "company_cache.json"

DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "jobwatcher")

PER_PAGE = 25

CONFIDENCE_BORDER = {"high": "border-green-500", "medium": "border-yellow-500", "low": "border-gray-700"}
CONFIDENCE_BADGE  = {"high": "bg-green-500/15 text-green-400", "medium": "bg-yellow-500/15 text-yellow-400", "low": "bg-gray-700 text-gray-300"}

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Log-based pipeline health (unchanged — feeds the Stats page)
# ---------------------------------------------------------------------------

def parse_log_stats():
    s = {
        "runs": 0, "last_run": "—",
        "scraped": 0, "title_skips": 0, "staffing_skips": 0, "dealership_skips": 0,
        "s1_skips": 0, "s1_passes": 0,
        "s2_skips": 0, "tam_skips": 0, "hits": 0,
        "bd_attempts": 0, "bd_successes": 0, "bd_fallbacks": 0,
        "s3_overrides": 0,
    }

    if not LOG_FILE.exists():
        s["bd_success_rate"] = 0
        s["hit_rate"] = 0
        return s

    lines = LOG_FILE.read_text(errors="replace").splitlines()

    for line in lines:
        if "--- Starting search run ---" in line:
            s["runs"] += 1
            try:
                s["last_run"] = line.split("  ")[0].strip()
            except Exception:
                pass
        elif "Got" in line and "postings from" in line:
            m = re.search(r"Got (\d+) postings", line)
            if m:
                s["scraped"] += int(m.group(1))
        elif "TITLE SKIP"      in line: s["title_skips"]      += 1
        elif "STAFFING SKIP"   in line: s["staffing_skips"]   += 1
        elif "DEALERSHIP SKIP" in line: s["dealership_skips"] += 1
        elif "S1 SKIP"       in line: s["s1_skips"]       += 1
        elif "S1 PASS"       in line: s["s1_passes"]      += 1
        elif "S2 SKIP"       in line: s["s2_skips"]       += 1
        elif "TAM SKIP"      in line: s["tam_skips"]      += 1
        elif "✓ HIT" in line or "✓ TAM HIT" in line: s["hits"] += 1
        elif "ZoomInfo URL:" in line:              s["bd_attempts"]  += 1
        elif "[BrightData]"  in line:              s["bd_successes"] += 1
        elif "Falling back to Serper" in line:     s["bd_fallbacks"] += 1
        elif "S3 OVERRIDDEN" in line:              s["s3_overrides"] += 1

    s["bd_success_rate"] = (
        round(s["bd_successes"] / s["bd_attempts"] * 100)
        if s["bd_attempts"] else 0
    )
    s["hit_rate"] = (
        round(s["hits"] / s["s1_passes"] * 100)
        if s["s1_passes"] else 0
    )
    return s


def get_log_tail(n=120):
    if not LOG_FILE.exists():
        return ["No log file found."]
    lines = LOG_FILE.read_text(errors="replace").splitlines()
    return lines[-n:]


def cache_size():
    try:
        return len(json.loads(COMPANY_FILE.read_text())) if COMPANY_FILE.exists() else 0
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Shared nav
# ---------------------------------------------------------------------------

def render_nav(active: str) -> str:
    def tab(href, label, key):
        cls = (
            "text-white border-b-2 border-indigo-500"
            if active == key else
            "text-gray-400 hover:text-white border-b-2 border-transparent"
        )
        return f'<a href="{href}" class="px-1 pb-1 text-sm font-medium transition-colors {cls}">{label}</a>'

    return f'<div class="flex items-center gap-6">{tab("/", "Recent Postings", "postings")}{tab("/stats", "Stats", "stats")}</div>'


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

LOGIN_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>JobWatcher — Login</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>body{font-family:'Inter',sans-serif;}</style>
</head>
<body class="min-h-screen bg-gray-950 flex items-center justify-center">
  <div class="w-full max-w-sm">
    <div class="text-center mb-8">
      <div class="text-4xl mb-3">🎯</div>
      <h1 class="text-2xl font-bold text-white">JobWatcher</h1>
      <p class="text-gray-400 text-sm mt-1">Pipeline Dashboard</p>
    </div>
    <div class="bg-gray-900 border border-gray-800 rounded-2xl p-8 shadow-2xl">
      {% if error %}
      <div class="bg-red-500/10 border border-red-500/30 text-red-400 text-sm rounded-lg px-4 py-3 mb-5">
        Incorrect password
      </div>
      {% endif %}
      <form method="POST">
        <label class="block text-sm font-medium text-gray-400 mb-2">Password</label>
        <input type="password" name="password" autofocus
          class="w-full bg-gray-800 border border-gray-700 text-white rounded-lg px-4 py-3 text-sm
                 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent mb-5"
          placeholder="Enter password">
        <button type="submit"
          class="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-semibold rounded-lg py-3 text-sm transition-colors">
          Sign in
        </button>
      </form>
    </div>
  </div>
</body>
</html>
"""

PAGE_HEAD = """
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  {% if auto_refresh %}<meta http-equiv="refresh" content="60">{% endif %}
  <title>JobWatcher — {{ page_title }}</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Inter', sans-serif; }
    .log-line { font-family: 'JetBrains Mono', 'Fira Code', monospace; }
    .log-hit    { color: #4ade80; }
    .log-skip   { color: #6b7280; }
    .log-pass   { color: #60a5fa; }
    .log-warn   { color: #fb923c; }
    .log-bd     { color: #a78bfa; }
  </style>
"""

PAGE_HEADER = """
  <header class="border-b border-gray-800 bg-gray-900/80 backdrop-blur sticky top-0 z-10">
    <div class="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between gap-6">
      <div class="flex items-center gap-3 shrink-0">
        <span class="text-2xl">🎯</span>
        <div>
          <h1 class="text-lg font-bold text-white leading-tight">JobWatcher</h1>
          <p class="text-xs text-gray-500">{{ page_title }}</p>
        </div>
      </div>
      {{ nav | safe }}
      <a href="/logout" class="text-xs text-gray-500 hover:text-white transition-colors shrink-0">Sign out</a>
    </div>
  </header>
"""

POSTINGS_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>""" + PAGE_HEAD + """</head>
<body class="min-h-screen bg-gray-950 text-gray-100">
""" + PAGE_HEADER + """

  <main class="max-w-4xl mx-auto px-6 py-8">
    {% if not postings %}
    <div class="text-center py-24 text-gray-600">
      <p class="text-4xl mb-3">📭</p>
      {% if not db_configured %}
      <p class="text-sm">Postgres isn't configured yet — set DATABASE_URL to start storing postings.</p>
      {% else %}
      <p class="text-sm">No postings yet. Check back after the next search run.</p>
      {% endif %}
    </div>
    {% else %}
    <div class="space-y-4">
      {% for p in postings %}
      <div class="bg-gray-900 border-l-4 {{ confidence_border.get(p.confidence, 'border-gray-700') }} border-t border-r border-b border-gray-800 rounded-xl p-5">
        <div class="flex items-start justify-between gap-4">
          <div class="min-w-0">
            <h3 class="text-base font-semibold text-white truncate">
              {% if p.website %}<a href="{{ p.website }}" target="_blank" rel="noopener" class="hover:text-indigo-400">{{ p.company }}</a>{% else %}{{ p.company or "Unknown Company" }}{% endif %}
            </h3>
            <p class="text-sm text-gray-300 mt-1">
              {% if p.job_url %}<a href="{{ p.job_url }}" target="_blank" rel="noopener" class="hover:text-indigo-400 hover:underline">{{ p.title }}</a>{% else %}{{ p.title or "Unknown Title" }}{% endif %}
            </p>
          </div>
          <span class="shrink-0 text-xs font-medium px-2.5 py-1 rounded-full {{ confidence_badge.get(p.confidence, 'bg-gray-700 text-gray-300') }}">{{ (p.confidence or "low")|capitalize }}</span>
        </div>

        <div class="grid grid-cols-2 sm:grid-cols-4 gap-3 mt-4 text-xs">
          <div><p class="text-gray-500">Location</p><p class="text-gray-200 mt-0.5">{{ p.location or "Unknown" }}</p></div>
          <div><p class="text-gray-500">Revenue</p><p class="text-gray-200 mt-0.5">${{ "%.1f"|format(p.revenue_millions or 0) }}M</p></div>
          <div><p class="text-gray-500">Employees</p><p class="text-gray-200 mt-0.5">{{ p.employees or "?" }}</p></div>
          {% if p.hq_state %}<div><p class="text-gray-500">HQ</p><p class="text-gray-200 mt-0.5">{{ p.hq_state }}</p></div>{% endif %}
        </div>

        {% if p.industry %}<p class="text-xs text-gray-500 mt-3">🏭 {{ p.industry }}</p>{% endif %}

        {% if p.tech_stack %}
        <div class="flex flex-wrap gap-1.5 mt-2">
          {% for t in p.tech_stack[:6] %}<span class="text-xs bg-gray-800 text-gray-300 px-2 py-0.5 rounded">{{ t }}</span>{% endfor %}
        </div>
        {% endif %}

        {% if p.tier1_hits %}
        <p class="text-xs text-green-400 mt-2">🔥 {% for kw in p.tier1_hits %}<code class="mr-1">{{ kw }}</code>{% endfor %}</p>
        {% endif %}
        {% if p.tier2_hits %}
        <p class="text-xs text-yellow-400 mt-1">⚠️ {% for kw in p.tier2_hits %}<code class="mr-1">{{ kw }}</code>{% endfor %}</p>
        {% endif %}

        {% if p.erp_signals %}
        <div class="flex flex-wrap gap-1.5 mt-2">
          {% for sig in p.erp_signals %}<span class="text-xs bg-indigo-500/10 text-indigo-300 px-2 py-0.5 rounded">{{ sig }}</span>{% endfor %}
        </div>
        {% endif %}

        {% if p.summary %}<p class="text-sm text-gray-400 mt-3 leading-relaxed">{{ p.summary }}</p>{% endif %}

        <div class="flex items-center justify-between mt-4 pt-3 border-t border-gray-800 text-xs text-gray-500">
          <span>{{ p.site }}{% if p.enrich_source == 'brightdata' %} · ZoomInfo ✓{% endif %}</span>
          <span>{{ p.alerted_at.strftime('%Y-%m-%d %H:%M') if p.alerted_at else '' }}</span>
        </div>
      </div>
      {% endfor %}
    </div>

    <div class="flex items-center justify-between mt-8">
      {% if page > 1 %}
      <a href="/?page={{ page - 1 }}" class="text-sm text-gray-400 hover:text-white transition-colors">← Prev</a>
      {% else %}<span></span>{% endif %}
      <span class="text-xs text-gray-500">Page {{ page }} of {{ total_pages }}</span>
      {% if page < total_pages %}
      <a href="/?page={{ page + 1 }}" class="text-sm text-gray-400 hover:text-white transition-colors">Next →</a>
      {% else %}<span></span>{% endif %}
    </div>
    {% endif %}
  </main>
</body>
</html>
"""

STATS_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>""" + PAGE_HEAD + """</head>
<body class="min-h-screen bg-gray-950 text-gray-100">
""" + PAGE_HEADER + """

  <main class="max-w-7xl mx-auto px-6 py-8 space-y-8">

    <!-- Lead history summary -->
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <p class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">All-Time Leads</p>
        <p class="text-3xl font-bold text-white">{{ totals.all_time }}</p>
        <p class="text-xs text-gray-600 mt-1">Driven to Discord</p>
      </div>
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <p class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">This Week</p>
        <p class="text-3xl font-bold text-white">{{ totals.this_week }}</p>
        <p class="text-xs text-gray-600 mt-1">Last 7 days</p>
      </div>
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <p class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">This Month</p>
        <p class="text-3xl font-bold text-white">{{ totals.this_month }}</p>
        <p class="text-xs text-gray-600 mt-1">Last 30 days</p>
      </div>
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <p class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Avg Company</p>
        <p class="text-3xl font-bold text-white">${{ avgs.avg_revenue }}M</p>
        <p class="text-xs text-gray-600 mt-1">~{{ avgs.avg_employees }} employees</p>
      </div>
    </div>

    <!-- Alerts over time -->
    <div class="bg-gray-900 border border-gray-800 rounded-xl p-6">
      <h2 class="text-sm font-semibold text-white mb-5">Alerts Over Time <span class="text-gray-600 font-normal text-xs ml-2">last 60 days</span></h2>
      {% if chart_labels %}
      <canvas id="alertsChart" height="70"></canvas>
      {% else %}
      <p class="text-xs text-gray-600 py-8 text-center">No history yet.</p>
      {% endif %}
    </div>

    <!-- Breakdowns -->
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h2 class="text-sm font-semibold text-white mb-4">Confidence</h2>
        {{ bar_list(confidence_breakdown, "bg-green-400") }}
      </div>
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h2 class="text-sm font-semibold text-white mb-4">Top Industries</h2>
        {{ bar_list(top_industries, "bg-purple-400") }}
      </div>
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h2 class="text-sm font-semibold text-white mb-4">Top HQ States</h2>
        {{ bar_list(top_hq_states, "bg-blue-400") }}
      </div>
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h2 class="text-sm font-semibold text-white mb-4">Source</h2>
        {{ bar_list(source_breakdown, "bg-indigo-400") }}
      </div>
    </div>

    <!-- Pipeline health (live, from watcher.log) -->
    <div class="grid grid-cols-1 lg:grid-cols-5 gap-6">
      <div class="lg:col-span-2 bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h2 class="text-sm font-semibold text-white mb-5">Pipeline Funnel <span class="text-gray-600 font-normal text-xs ml-2">this run of the log</span></h2>
        <div class="space-y-3">
          {% set funnel = [
            ("Scraped",          s.scraped,         "bg-indigo-500"),
            ("Title Filter",     s.scraped - s.title_skips - s.staffing_skips - s.dealership_skips, "bg-indigo-400"),
            ("Stage 1 Pass",     s.s1_passes,       "bg-blue-400"),
            ("After Enrichment", s.s1_passes - s.s2_skips, "bg-violet-400"),
            ("TAM Pass",         s.s1_passes - s.s2_skips - s.tam_skips, "bg-purple-400"),
            ("✓ Alerts Sent",    s.hits,            "bg-green-500"),
          ] %}
          {% for label, count, color in funnel %}
          <div>
            <div class="flex justify-between text-xs mb-1">
              <span class="text-gray-400">{{ label }}</span>
              <span class="text-white font-medium">{{ [count, 0] | max }}</span>
            </div>
            <div class="h-2 bg-gray-800 rounded-full overflow-hidden">
              {% if s.scraped > 0 %}
              <div class="{{ color }} h-full rounded-full transition-all"
                   style="width: {{ [(([count,0]|max) / s.scraped * 100)|int, 100]|min }}%"></div>
              {% endif %}
            </div>
          </div>
          {% endfor %}
        </div>

        <div class="mt-6 pt-5 border-t border-gray-800 grid grid-cols-2 gap-3 text-xs">
          <div class="text-gray-500">Total runs <span class="text-white font-medium float-right">{{ s.runs }}</span></div>
          <div class="text-gray-500">S3 overrides <span class="text-green-400 font-medium float-right">{{ s.s3_overrides }}</span></div>
          <div class="text-gray-500">BD fallbacks <span class="text-yellow-400 font-medium float-right">{{ s.bd_fallbacks }}</span></div>
          <div class="text-gray-500">Staffing blocked <span class="text-red-400 font-medium float-right">{{ s.staffing_skips }}</span></div>
          <div class="text-gray-500">Dealerships blocked <span class="text-red-400 font-medium float-right">{{ s.dealership_skips }}</span></div>
          <div class="text-gray-500">BD success rate <span class="text-white font-medium float-right">{{ s.bd_success_rate }}%</span></div>
          <div class="text-gray-500">Companies cached <span class="text-white font-medium float-right">{{ cache }}</span></div>
        </div>
      </div>

      <div class="lg:col-span-3 bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h2 class="text-sm font-semibold text-white mb-4">Live Log <span class="text-gray-600 font-normal text-xs ml-2">last 120 lines</span></h2>
        <div class="bg-gray-950 rounded-lg p-4 max-h-96 overflow-y-auto">
          {% for line in log_lines %}
          <div class="log-line text-xs leading-5
            {% if '✓ HIT' in line or 'TAM HIT' in line %}log-hit
            {% elif 'SKIP' in line %}log-skip
            {% elif 'PASS' in line %}log-pass
            {% elif 'WARNING' in line or 'ERROR' in line %}log-warn
            {% elif 'BrightData' in line %}log-bd
            {% else %}text-gray-500{% endif %}">{{ line }}</div>
          {% endfor %}
        </div>
      </div>
    </div>

  </main>

  {% if chart_labels %}
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script>
    new Chart(document.getElementById('alertsChart'), {
      type: 'bar',
      data: {
        labels: {{ chart_labels | tojson }},
        datasets: [{
          label: 'Alerts',
          data: {{ chart_counts | tojson }},
          backgroundColor: '#6366f1',
          borderRadius: 4,
        }]
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { display: false }, ticks: { color: '#6b7280', maxRotation: 0, autoSkip: true, maxTicksLimit: 14 } },
          y: { beginAtZero: true, ticks: { color: '#6b7280', precision: 0 }, grid: { color: '#1f2937' } }
        }
      }
    });
  </script>
  {% endif %}
</body>
</html>
"""

BAR_LIST_MACRO = """
{% macro bar_list(items, color) %}
  {% if items %}
  <div class="space-y-2.5">
    {% for item in items %}
    <div>
      <div class="flex justify-between text-xs mb-1">
        <span class="text-gray-400 truncate">{{ (item.label or "Unknown")|capitalize }}</span>
        <span class="text-white font-medium">{{ item.count }}</span>
      </div>
      <div class="h-1.5 bg-gray-800 rounded-full overflow-hidden">
        <div class="{{ color }} h-full rounded-full" style="width: {{ (item.count / items[0].count * 100)|int }}%"></div>
      </div>
    </div>
    {% endfor %}
  </div>
  {% else %}
  <p class="text-xs text-gray-600">No data yet.</p>
  {% endif %}
{% endmacro %}
"""

STATS_HTML = BAR_LIST_MACRO + STATS_HTML


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    error = False
    if request.method == "POST":
        if request.form.get("password") == DASHBOARD_PASSWORD:
            session["logged_in"] = True
            return redirect("/")
        error = True
    return render_template_string(LOGIN_HTML, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/")
@login_required
def postings():
    try:
        page = max(int(request.args.get("page", 1)), 1)
    except (TypeError, ValueError):
        page = 1

    total = db.count_postings()
    total_pages = max(-(-total // PER_PAGE), 1)  # ceil div
    page = min(page, total_pages)
    offset = (page - 1) * PER_PAGE

    return render_template_string(
        POSTINGS_HTML,
        page_title="Recent Postings",
        nav=render_nav("postings"),
        auto_refresh=False,
        postings=db.get_recent_postings(limit=PER_PAGE, offset=offset),
        page=page,
        total_pages=total_pages,
        db_configured=bool(db.DATABASE_URL),
        confidence_border=CONFIDENCE_BORDER,
        confidence_badge=CONFIDENCE_BADGE,
    )


@app.route("/stats")
@login_required
def stats():
    over_time = db.get_alerts_over_time(60)
    return render_template_string(
        STATS_HTML,
        page_title="Stats",
        nav=render_nav("stats"),
        auto_refresh=True,
        totals=db.get_totals(),
        chart_labels=[r["day"] for r in over_time],
        chart_counts=[r["count"] for r in over_time],
        confidence_breakdown=db.get_confidence_breakdown(),
        top_industries=db.get_top_industries(),
        top_hq_states=db.get_top_hq_states(),
        source_breakdown=db.get_source_breakdown(),
        avgs=db.get_revenue_employee_avgs(),
        s=parse_log_stats(),
        log_lines=get_log_tail(),
        cache=cache_size(),
    )


# ---------------------------------------------------------------------------
# Entry point (called from watcher.py as a background thread)
# ---------------------------------------------------------------------------

def start_dashboard():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    start_dashboard()
