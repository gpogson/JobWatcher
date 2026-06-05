"""
JobWatcher Dashboard — Flask web UI for monitoring the watcher pipeline.
Run as a background thread alongside watcher.py.
"""

import json
import os
import re
import threading
from datetime import datetime
from functools import wraps
from pathlib import Path

from flask import Flask, redirect, render_template_string, request, session

app = Flask(__name__)
app.secret_key = os.environ.get("DASHBOARD_SECRET", os.urandom(24))

BASE_DIR     = Path(__file__).parent
LOG_FILE     = BASE_DIR / "watcher.log"
DIGEST_FILE  = BASE_DIR / "digest_log.json"
COMPANY_FILE = BASE_DIR / "company_cache.json"
SEEN_FILE    = BASE_DIR / "seen_jobs.json"

DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "jobwatcher")

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
# Data helpers
# ---------------------------------------------------------------------------

def parse_log_stats():
    if not LOG_FILE.exists():
        return {}

    lines = LOG_FILE.read_text(errors="replace").splitlines()

    s = {
        "runs": 0, "last_run": "—",
        "scraped": 0, "title_skips": 0, "staffing_skips": 0,
        "s1_skips": 0, "s1_passes": 0,
        "s2_skips": 0, "tam_skips": 0, "hits": 0,
        "bd_attempts": 0, "bd_successes": 0, "bd_fallbacks": 0,
        "s3_overrides": 0,
    }

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
        elif "TITLE SKIP"    in line: s["title_skips"]    += 1
        elif "STAFFING SKIP" in line: s["staffing_skips"] += 1
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


def get_recent_alerts(limit=25):
    if not DIGEST_FILE.exists():
        return []
    try:
        entries = json.loads(DIGEST_FILE.read_text())
        return sorted(entries, key=lambda x: x.get("timestamp", ""), reverse=True)[:limit]
    except Exception:
        return []


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

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="refresh" content="60">
  <title>JobWatcher Dashboard</title>
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
</head>
<body class="min-h-screen bg-gray-950 text-gray-100">

  <!-- Header -->
  <header class="border-b border-gray-800 bg-gray-900/80 backdrop-blur sticky top-0 z-10">
    <div class="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
      <div class="flex items-center gap-3">
        <span class="text-2xl">🎯</span>
        <div>
          <h1 class="text-lg font-bold text-white leading-tight">JobWatcher</h1>
          <p class="text-xs text-gray-500">Pipeline Dashboard</p>
        </div>
      </div>
      <div class="flex items-center gap-4">
        <span class="text-xs text-gray-500">Auto-refresh 60s &nbsp;·&nbsp; Last run: <span class="text-gray-300">{{ s.last_run }}</span></span>
        <a href="/logout" class="text-xs text-gray-500 hover:text-white transition-colors">Sign out</a>
      </div>
    </div>
  </header>

  <main class="max-w-7xl mx-auto px-6 py-8 space-y-8">

    <!-- Stats cards -->
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <p class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Alerts Sent</p>
        <p class="text-3xl font-bold text-white">{{ s.hits }}</p>
        <p class="text-xs text-gray-600 mt-1">All time</p>
      </div>
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <p class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Hit Rate</p>
        <p class="text-3xl font-bold {% if s.hit_rate > 10 %}text-green-400{% else %}text-yellow-400{% endif %}">{{ s.hit_rate }}%</p>
        <p class="text-xs text-gray-600 mt-1">Of S1 passes</p>
      </div>
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <p class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">BD Success</p>
        <p class="text-3xl font-bold {% if s.bd_success_rate > 70 %}text-green-400{% elif s.bd_success_rate > 40 %}text-yellow-400{% else %}text-red-400{% endif %}">{{ s.bd_success_rate }}%</p>
        <p class="text-xs text-gray-600 mt-1">{{ s.bd_successes }}/{{ s.bd_attempts }} lookups</p>
      </div>
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <p class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Companies Cached</p>
        <p class="text-3xl font-bold text-white">{{ cache }}</p>
        <p class="text-xs text-gray-600 mt-1">ZoomInfo lookups saved</p>
      </div>
    </div>

    <!-- Pipeline funnel + Recent alerts -->
    <div class="grid grid-cols-1 lg:grid-cols-5 gap-6">

      <!-- Funnel -->
      <div class="lg:col-span-2 bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h2 class="text-sm font-semibold text-white mb-5">Pipeline Funnel</h2>
        <div class="space-y-3">
          {% set funnel = [
            ("Scraped",          s.scraped,         "bg-indigo-500"),
            ("Title Filter",     s.scraped - s.title_skips - s.staffing_skips, "bg-indigo-400"),
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
        </div>
      </div>

      <!-- Recent alerts -->
      <div class="lg:col-span-3 bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h2 class="text-sm font-semibold text-white mb-5">Recent Alerts</h2>
        {% if alerts %}
        <div class="space-y-3 max-h-96 overflow-y-auto pr-1">
          {% for a in alerts %}
          <div class="bg-gray-800/60 rounded-lg px-4 py-3 hover:bg-gray-800 transition-colors">
            <div class="flex items-start justify-between gap-2">
              <div class="min-w-0">
                <p class="text-sm font-medium text-white truncate">{{ a.company }}</p>
                <p class="text-xs text-gray-400 truncate mt-0.5">{{ a.job_title }}</p>
                {% if a.hot_keywords %}
                <p class="text-xs text-green-400 mt-1">🔥 {{ a.hot_keywords }}</p>
                {% endif %}
                {% if a.legacy_keywords %}
                <p class="text-xs text-yellow-400">⚠️ {{ a.legacy_keywords }}</p>
                {% endif %}
              </div>
              <div class="text-right shrink-0">
                <p class="text-xs text-gray-500">{{ a.timestamp[:10] }}</p>
                {% if a.website %}
                <a href="{{ a.website }}" target="_blank"
                   class="text-xs text-indigo-400 hover:text-indigo-300 transition-colors">website ↗</a>
                {% endif %}
              </div>
            </div>
          </div>
          {% endfor %}
        </div>
        {% else %}
        <div class="text-center py-12 text-gray-600">
          <p class="text-4xl mb-3">📭</p>
          <p class="text-sm">No alerts yet</p>
        </div>
        {% endif %}
      </div>
    </div>

    <!-- Log tail -->
    <div class="bg-gray-900 border border-gray-800 rounded-xl p-6">
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

  </main>
</body>
</html>
"""


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
def index():
    return render_template_string(
        DASHBOARD_HTML,
        s=parse_log_stats(),
        alerts=get_recent_alerts(),
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
