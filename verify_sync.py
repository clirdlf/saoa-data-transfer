#!/usr/bin/env python3
"""
Verify Dropbox → Box:/SAOA sync using rclone listings (size + modtime).

Outputs:
  ./reports/report.csv
  ./reports/report.html
  ./reports/report.json
  ./reports/status.json

Features:
  - Runtime (elapsed_seconds)
  - HTML nav buttons
  - Top-level Directories summary with per-folder status ✔️/❌
  - TOTAL SIZE (GB) in Summary
  - Size (GB) column in Top-level Directories

Usage:
  python verify_box_sync.py
"""

import csv
import json
import os
import sys
import subprocess
import fnmatch
import time
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Dict, Tuple, Optional, List, Iterable

# --------------------
# Configuration
# --------------------
DROPBOX_REMOTE = "dropbox:"       # Source root ("/" on Dropbox)
BOX_REMOTE     = "box:/SAOA"      # Destination root ("/SAOA" on Box)

# If you want to restrict to specific top-level dirs under Dropbox root, list them here:
# e.g., DIRS = ["Projects", "Team A", "Shared Stuff"]
DIRS: Optional[List[str]] = None  # None = everything under "/"

# Comparison behavior
CASE_INSENSITIVE = True
MODTIME_TOLERANCE_SECONDS = 120

# Performance
RCLONE_CHECKERS = 16
USE_FAST_LIST = True

# Exclusions (match on basename)
EXCLUDE_BASENAME_PATTERNS = [
    ".DS_Store",
    "Thumbs.db",
    "._*",
    "~$*",
    "*.boxnote",
    "*.tmp",
]

# Output
REPORT_DIR = "./reports"
CSV_PATH   = os.path.join(REPORT_DIR, "report.csv")
HTML_PATH  = os.path.join(REPORT_DIR, "report.html")
JSON_PATH  = os.path.join(REPORT_DIR, "report.json")
STATUS_PATH= os.path.join(REPORT_DIR, "status.json")


# --------------------
# Helpers
# --------------------
def ensure_reports_dir():
    os.makedirs(REPORT_DIR, exist_ok=True)


def rclone_path(remote: str, subpath: str) -> str:
    sub = subpath.lstrip("/")
    return f"{remote.rstrip('/')}/{sub}" if sub else remote.rstrip("/")


def should_exclude(path: str) -> bool:
    base = PurePosixPath(path).name
    return any(fnmatch.fnmatch(base, pat) for pat in EXCLUDE_BASENAME_PATTERNS)


def parse_rfc3339_modtime(s: str) -> Optional[float]:
    if not s or s == "-":
        return None
    try:
        if "." in s:
            head, tail = s.split(".", 1)
            if s.endswith("Z"):
                frac = tail[:-1][:6]
                s2 = f"{head}.{frac}+00:00"
            elif "+" in tail:
                frac, tz = tail.split("+", 1)
                s2 = f"{head}.{frac[:6]}+{tz}"
            else:
                s2 = f"{head}.{tail[:6]}"
        else:
            s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def run_rclone_lsf(remote_path: str) -> List[Tuple[str, Optional[float], Optional[int]]]:
    fields = "pst"  # p=path, s=size, t=modtime
    sep = "\t"
    cmd = [
        "rclone", "lsf",
        "-R", "--files-only",
        "--format", fields,
        "--separator", sep,
    ]
    if USE_FAST_LIST:
        cmd.append("--fast-list")
    cmd += ["--checkers", str(RCLONE_CHECKERS), remote_path]

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except FileNotFoundError:
        print("ERROR: rclone not found on PATH.", file=sys.stderr)
        sys.exit(2)

    entries: List[Tuple[str, Optional[float], Optional[int]]] = []
    for line in proc.stdout or []:
        line = line.rstrip("\n")
        if not line:
            continue
        parts = line.split(sep)
        if len(parts) != len(fields):
            continue
        p, s, t = parts[0], parts[1], parts[2]
        if should_exclude(p):
            continue
        try:
            size = int(s) if s and s != "-" else None
        except ValueError:
            size = None
        ts = parse_rfc3339_modtime(t)
        entries.append((str(PurePosixPath(p)), ts, size))

    _, err = proc.communicate()
    if proc.returncode not in (0,):
        print("WARNING: rclone lsf returned non-zero.\n", err, file=sys.stderr)

    return entries


def build_index(entries: List[Tuple[str, Optional[float], Optional[int]]]) -> Dict[str, dict]:
    idx: Dict[str, dict] = {}
    for rel, ts, size in entries:
        key = rel.lower() if CASE_INSENSITIVE else rel
        idx[key] = {"path": rel, "modtime": ts, "size": size}
    return idx


def compare(src_idx: Dict[str, dict], dst_idx: Dict[str, dict]) -> Tuple[List[dict], List[dict], int]:
    missing_on_dst: List[dict] = []
    mismatches: List[dict] = []
    matched_count = 0

    for key, src in src_idx.items():
        dst = dst_idx.get(key)
        if not dst:
            missing_on_dst.append({
                "path": src["path"],
                "size": src["size"],
                "modtime": src["modtime"],
            })
            continue

        size_equal = (src["size"] == dst["size"])
        if src["modtime"] is None or dst["modtime"] is None:
            within_tol = False
            mtime_diff = None
        else:
            mtime_diff = abs((dst["modtime"] - src["modtime"]))
            within_tol = mtime_diff <= MODTIME_TOLERANCE_SECONDS

        if size_equal and within_tol:
            matched_count += 1
        else:
            mismatches.append({
                "path": src["path"],
                "src_size": src["size"],
                "dst_size": dst["size"],
                "size_equal": size_equal,
                "src_modtime": src["modtime"],
                "dst_modtime": dst["modtime"],
                "modtime_diff_seconds": mtime_diff,
                "within_tolerance": within_tol,
            })

    return missing_on_dst, mismatches, matched_count


def top_level_of(path: str) -> str:
    """Return the first path segment of a posix path."""
    parts = PurePosixPath(path).parts
    return parts[0] if parts else ""


def bytes_to_gb(n: Optional[int]) -> Optional[float]:
    """Decimal gigabytes (GB) for readability: 1 GB = 1_000_000_000 bytes."""
    if n is None:
        return None
    return n / 1_000_000_000.0


def aggregate_by_top_level(
    src_idx: Dict[str, dict],
    missing: Iterable[dict],
    mismatches: Iterable[dict],
) -> Dict[str, dict]:
    """
    Build per-top-level folder stats:
      { top: { total_files, matched, missing, mismatches, all_synced, size_bytes, size_gb } }
    """
    tops: Dict[str, dict] = {}

    # Initial totals per top
    for v in src_idx.values():
        top = top_level_of(v["path"])
        if DIRS is not None and len(DIRS) > 0 and top not in DIRS:
            continue
        stats = tops.setdefault(top, {"total_files": 0, "missing": 0, "mismatches": 0, "size_bytes": 0})
        stats["total_files"] += 1
        if isinstance(v.get("size"), int):
            stats["size_bytes"] += v["size"]

    # Tally missing and mismatches
    for m in missing:
        top = top_level_of(m["path"])
        if top in tops:
            tops[top]["missing"] += 1

    for mm in mismatches:
        top = top_level_of(mm["path"])
        if top in tops:
            tops[top]["mismatches"] += 1

    # Compute matched, status, and GB
    for top, s in tops.items():
        matched = s["total_files"] - s["missing"] - s["mismatches"]
        s["matched"] = matched
        s["all_synced"] = (s["missing"] == 0 and s["mismatches"] == 0)
        s["size_gb"] = round(bytes_to_gb(s["size_bytes"]) or 0.0, 2)

    return tops


def write_csv(missing: List[dict], mismatches: List[dict]):
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "issue_type", "path",
            "src_size", "dst_size", "size_equal",
            "src_modtime_epoch", "dst_modtime_epoch",
            "modtime_diff_seconds", "within_tolerance"
        ])
        for m in missing:
            w.writerow([
                "missing_on_box", m["path"],
                m.get("size"), "", "",
                m.get("modtime"), "", "", ""
            ])
        for mm in mismatches:
            w.writerow([
                "mismatch", mm["path"],
                mm.get("src_size"), mm.get("dst_size"), mm.get("size_equal"),
                mm.get("src_modtime"), mm.get("dst_modtime"),
                mm.get("modtime_diff_seconds"), mm.get("within_tolerance"),
            ])


def fmt_ts(ts: Optional[float]) -> str:
    if ts is None:
        return "—"
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def write_html(summary: dict, missing: List[dict], mismatches: List[dict], per_top: Dict[str, dict]):
    nav_html = """
    <div class="nav">
      <a href="#summary">Summary</a>
      <a href="#top-level">Top-level Directories</a>
      <a href="#missing">Missing on Box</a>
      <a href="#mismatches">Mismatches</a>
    </div>
    <style>
      .nav { margin: 16px 0; }
      .nav a {
        display:inline-block; margin-right:10px; padding:6px 12px;
        border:1px solid #ccc; border-radius:6px;
        text-decoration:none; background:#f9fafb; color:#111;
      }
      .nav a:hover { background:#e5e7eb; }
    </style>
    """

    style = """
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 24px; }
    h1 { margin: 0 0 8px; }
    .meta, .notice { color: #555; }
    .summary { display: grid; grid-template-columns: repeat(5, minmax(160px, 1fr)); gap: 12px; margin: 16px 0 24px; }
    .card { border: 1px solid #e5e7eb; border-radius: 10px; padding: 12px; }
    table { width: 100%; border-collapse: collapse; margin-top: 12px; }
    th, td { border-bottom: 1px solid #eee; padding: 6px 8px; text-align: left; font-size: 14px; }
    th { background: #fafafa; }
    .tag { display: inline-block; padding: 2px 8px; border-radius: 999px; background: #eef2ff; font-size: 12px; }
    .ok { color: #065f46; }
    .warn { color: #92400e; }
    .fail { color: #991b1b; }
    code { background: #f6f8fa; padding: 1px 4px; border-radius: 4px; }
    .status-ok { color: #10b981; font-weight: 600; }
    .status-bad { color: #ef4444; font-weight: 600; }
    """

    # Top-level rows (include Size GB)
    html_rows_top = "".join(
        f"<tr>"
        f"<td>{top}</td>"
        f"<td style='text-align:right;'>{stats['total_files']}</td>"
        f"<td style='text-align:right;'>{stats['matched']}</td>"
        f"<td style='text-align:right;'>{stats['missing']}</td>"
        f"<td style='text-align:right;'>{stats['mismatches']}</td>"
        f"<td style='text-align:right;'>{stats['size_gb']}</td>"
        f"<td>{'✅' if stats['all_synced'] else '❌'}</td>"
        f"</tr>"
        for top, stats in sorted(per_top.items(), key=lambda kv: kv[0].lower())
    )

    total_size_gb = round(summary["total_size_gb"], 2)

    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title>Dropbox → Box Verification Report</title>
<style>{style}</style>
</head>
<body>
  <h1>Dropbox → Box Verification Report</h1>
  <div class="meta">
    Generated: <strong>{summary["generated_at"]}</strong><br/>
    Runtime: <strong>{summary["elapsed_seconds"]} seconds</strong><br/>
    Source: <code>{summary["src_remote"]}</code> → Destination: <code>{summary["dst_remote"]}</code><br/>
    Path mapping: <code>{summary["src_root"]}</code> → <code>{summary["dst_root"]}</code><br/>
    Case-insensitive compare; modtime tolerance ±{MODTIME_TOLERANCE_SECONDS}s; junk files ignored.
  </div>

  {nav_html}

  <h2 id="summary">Summary</h2>
  <div class="summary">
    <div class="card"><div class="tag">Total in source</div><div style="font-size:24px;font-weight:600;">{summary["counts"]["total_src_files"]}</div></div>
    <div class="card"><div class="tag">Matched</div><div style="font-size:24px;font-weight:600;">{summary["counts"]["matched"]}</div></div>
    <div class="card"><div class="tag">Missing on Box</div><div style="font-size:24px;font-weight:600;" class="{ 'ok' if summary['counts']['missing_on_dst']==0 else 'fail' }">{summary["counts"]["missing_on_dst"]}</div></div>
    <div class="card"><div class="tag">Mismatches</div><div style="font-size:24px;font-weight:600;" class="{ 'ok' if summary['counts']['mismatches']==0 else 'warn' }">{summary["counts"]["mismatches"]}</div></div>
    <div class="card"><div class="tag">Total size (GB)</div><div style="font-size:24px;font-weight:600;">{total_size_gb}</div></div>
  </div>

  <h2 id="top-level">Top-level Directories</h2>
  <table>
    <thead>
      <tr>
        <th>Directory</th>
        <th style="text-align:right;">Total</th>
        <th style="text-align:right;">Matched</th>
        <th style="text-align:right;">Missing</th>
        <th style="text-align:right;">Mismatches</th>
        <th style="text-align:right;">Size (GB)</th>
        <th>Status</th>
      </tr>
    </thead>
    <tbody>
      {html_rows_top if html_rows_top else "<tr><td colspan='7'>No files found.</td></tr>"}
    </tbody>
  </table>

  <h2 id="missing">Missing on Box ({len(missing)})</h2>
  <table>
    <thead><tr><th>Path (relative)</th><th>Size (bytes)</th><th>Source Modtime (UTC)</th></tr></thead>
    <tbody>
      {"".join(f"<tr><td>{m['path']}</td><td>{m.get('size','')}</td><td>{fmt_ts(m.get('modtime'))}</td></tr>" for m in missing)}
    </tbody>
  </table>

  <h2 id="mismatches">Mismatches (size and/or modtime) ({len(mismatches)})</h2>
  <table>
    <thead><tr><th>Path</th><th>Src Size</th><th>Dst Size</th><th>Size Equal</th><th>Src Modtime</th><th>Dst Modtime</th><th>Δ seconds</th><th>Within Tol</th></tr></thead>
    <tbody>
      {"".join(f"<tr><td>{mm['path']}</td><td>{mm.get('src_size','')}</td><td>{mm.get('dst_size','')}</td><td>{'Yes' if mm.get('size_equal') else 'No'}</td><td>{fmt_ts(mm.get('src_modtime'))}</td><td>{fmt_ts(mm.get('dst_modtime'))}</td><td>{'' if mm.get('modtime_diff_seconds') is None else round(mm['modtime_diff_seconds'], 2)}</td><td>{'Yes' if mm.get('within_tolerance') else 'No'}</td></tr>" for mm in mismatches)}
    </tbody>
  </table>

  <hr/>
  <p class="meta">JSON detail: <code>{os.path.basename(JSON_PATH)}</code> • CSV: <code>{os.path.basename(CSV_PATH)}</code> • Status: <code>{os.path.basename(STATUS_PATH)}</code></p>
</body>
</html>
"""
    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)


def main():
    ensure_reports_dir()

    started = time.time()
    generated_at = datetime.now(timezone.utc).isoformat()

    # Build listings
    if DIRS:
        src_entries: List[Tuple[str, Optional[float], Optional[int]]] = []
        dst_entries: List[Tuple[str, Optional[float], Optional[int]]] = []
        for top in DIRS:
            src_remote_path = rclone_path(DROPBOX_REMOTE, top)
            dst_remote_path = rclone_path(BOX_REMOTE, top)
            src_entries.extend(run_rclone_lsf(src_remote_path))
            dst_entries.extend(run_rclone_lsf(dst_remote_path))
    else:
        src_entries = run_rclone_lsf(rclone_path(DROPBOX_REMOTE, ""))
        dst_entries = run_rclone_lsf(rclone_path(BOX_REMOTE, ""))

    # Index by relative path
    src_idx = build_index(src_entries)
    dst_idx = build_index(dst_entries)

    # Compare
    missing_on_dst, mismatches, matched_count = compare(src_idx, dst_idx)

    total_src_files = len(src_idx)
    # Aggregate sizes (source only)
    total_size_bytes = sum(v["size"] for v in src_idx.values() if isinstance(v.get("size"), int))
    total_size_gb = bytes_to_gb(total_size_bytes) or 0.0

    counts = {
        "total_src_files": total_src_files,
        "matched": matched_count,
        "missing_on_dst": len(missing_on_dst),
        "mismatches": len(mismatches),
    }

    # Per top-level aggregation (includes size bytes & GB)
    per_top = aggregate_by_top_level(src_idx, missing_on_dst, mismatches)

    elapsed_seconds = round(time.time() - started, 2)

    # JSON report
    report = {
        "generated_at": generated_at,
        "elapsed_seconds": elapsed_seconds,
        "src_remote": DROPBOX_REMOTE,
        "dst_remote": BOX_REMOTE,
        "src_root": "/",
        "dst_root": "/SAOA",
        "case_insensitive": CASE_INSENSITIVE,
        "modtime_tolerance_seconds": MODTIME_TOLERANCE_SECONDS,
        "exclusions": EXCLUDE_BASENAME_PATTERNS,
        "counts": counts,
        "total_size_bytes": total_size_bytes,
        "total_size_gb": round(total_size_gb, 2),
        "per_top_level": per_top,
        "missing_on_box": missing_on_dst,
        "mismatches": mismatches,
    }
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # CSV + HTML + status
    write_csv(missing_on_dst, mismatches)
    write_html({
        "generated_at": generated_at,
        "elapsed_seconds": elapsed_seconds,
        "src_remote": DROPBOX_REMOTE,
        "dst_remote": BOX_REMOTE,
        "src_root": "/",
        "dst_root": "/SAOA",
        "counts": counts,
        "total_size_gb": total_size_gb,
    }, missing_on_dst, mismatches, per_top)

    status = {
        "generated_at": generated_at,
        "elapsed_seconds": elapsed_seconds,
        "status": "pass" if counts["missing_on_dst"] == 0 and counts["mismatches"] == 0 else "fail",
        "counts": counts,
        "total_size_bytes": total_size_bytes,
        "total_size_gb": round(total_size_gb, 2),
        "per_top_level": {k: {"all_synced": v["all_synced"], "total_files": v["total_files"], "size_bytes": v["size_bytes"], "size_gb": v["size_gb"]} for k, v in per_top.items()},
    }
    with open(STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    # Console summary
    print(f"[OK] Wrote {CSV_PATH}")
    print(f"[OK] Wrote {HTML_PATH}")
    print(f"[OK] Wrote {JSON_PATH}")
    print(f"[OK] Wrote {STATUS_PATH}")
    print(f"Summary: {counts} | Total Size (GB): {round(total_size_gb, 2)} | Runtime: {elapsed_seconds}s")


if __name__ == "__main__":
    main()