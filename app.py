import io
import os
import re
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


def safe_csv_name(name: str, fallback: str) -> str:
    """
    Sanitize a user-supplied filename and ensure it ends with .csv.
    Keeps letters, numbers, dot, underscore, dash. Trims to 100 chars.
    """
    name = (name or "").strip()
    cleaned = re.sub(r'[^A-Za-z0-9._-]+', '_', name)[:100]
    if cleaned in {"", ".", ".."}:
        cleaned = fallback
    if not cleaned.lower().endswith(".csv"):
        cleaned += ".csv"
    return cleaned


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
    column_names=data["result"]["metadata"]["column_names"]
    rows = data["result"]["rows"]
    df = pd.DataFrame(rows,columns=column_names)
    total = len(df)

    # Suggest a default file name
    utc_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    suggested_name = f"dune_query_{query_id}_{utc_ts}.csv"

    return render_template(
        "results.html",
        df=df,
        total=total,
        query_id=query_id,
        api_key=api_key,
        suggested_name=suggested_name,  # used by the template's filename input
    )


@app.route("/download", methods=["POST"])
def download():
    api_key = request.form.get("api_key", "").strip()
    query_id_str = request.form.get("query_id", "0")
    user_name = request.form.get("filename", "")

    try:
        query_id = int(query_id_str)
    except ValueError:
        query_id = 0

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

    # Build a safe final filename
    utc_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    default_name = f"dune_query_{query_id}_{utc_ts}.csv"
    final_name = safe_csv_name(user_name, default_name)

    return send_file(
        io.BytesIO(csv_bytes),
        mimetype="text/csv",
        as_attachment=True,
        download_name=final_name,
    )


if __name__ == "__main__":
    # Local run; on Render use Gunicorn: `gunicorn app:app --bind 0.0.0.0:$PORT`
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
