import io
import os
import csv
import pandas as pd
import requests
from datetime import datetime, timezone
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from dotenv import load_dotenv

load_dotenv()  # loads .env if present

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change-me")
# Optional: put your key in .env as DUNE_API_KEY=xxx and leave the input empty
DEFAULT_DUNE_API_KEY = os.getenv("DUNE_API_KEY", "")

def fetch_dune_data(api_key: str, query_id: int) -> dict:
    url = f"https://api.dune.com/api/v1/query/{query_id}/results"
    headers = {"X-DUNE-API-KEY": api_key}
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

@app.route("/", methods=["GET"])
def index():
    # page with a form
    return render_template("index.html", default_api_key=DEFAULT_DUNE_API_KEY)

@app.route("/fetch", methods=["POST"])
def fetch():
    api_key = request.form.get("api_key", "").strip()
    query_id = request.form.get("query_id", "").strip()

    if not api_key:
        flash("Please enter your Dune API key.", "warning")
        return redirect(url_for("index"))

    try:
        query_id = int(query_id)
    except ValueError:
        flash("Query ID must be a positive integer.", "warning")
        return redirect(url_for("index"))

    try:
        data = fetch_dune_data(api_key, query_id)
    except requests.exceptions.HTTPError as e:
        flash(f"HTTP error: {e.response.status_code} {e.response.reason}", "danger")
        return redirect(url_for("index"))
    except requests.exceptions.RequestException as e:
        flash(f"Network error: {e}", "danger")
        return redirect(url_for("index"))
    except Exception as e:
        flash(f"Unexpected error: {e}", "danger")
        return redirect(url_for("index"))

    # Parse rows
    if not isinstance(data, dict) or "result" not in data or "rows" not in data["result"]:
        flash("API response not in expected format. See raw JSON below.", "danger")
        # Optionally show raw JSON for debugging
        return render_template("results.html", df=None, raw_json=data, query_id=query_id)

    rows = data["result"]["rows"]
    df = pd.DataFrame(rows)

    # Store CSV as bytes in memory and pass via a tokenless postback (simple approach):
    # We’ll keep the CSV in the session-less request by regenerating from df when downloading.
    # To do that, we’ll stash the table in a hidden form on the results page as JSON (compact),
    # or just rebuild CSV on the fly by POST’ing back data. Simpler: provide a download endpoint
    # that refetches with same params (idempotent read). That’s what we’ll do.
    total = len(df)
    return render_template("results.html", df=df, total=total, query_id=query_id, api_key=api_key)

@app.route("/download", methods=["POST"])
def download():
    api_key = request.form.get("api_key", "").strip()
    query_id = int(request.form.get("query_id", "0"))
    if not api_key or not query_id:
        flash("Missing API key or Query ID for download.", "warning")
        return redirect(url_for("index"))

    # Refetch to produce CSV (stateless + simple)
    try:
        data = fetch_dune_data(api_key, query_id)
        rows = data.get("result", {}).get("rows", [])
        df = pd.DataFrame(rows)
    except Exception as e:
        flash(f"Download failed: {e}", "danger")
        return redirect(url_for("index"))

    csv_bytes = to_csv_bytes(df)
    utc_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"dune_query_{query_id}_{utc_ts}.csv"
    return send_file(
        io.BytesIO(csv_bytes),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename
    )

if __name__ == "__main__":
    # Run: python app.py  -> http://127.0.0.1:5000
    app.run(debug=True)
