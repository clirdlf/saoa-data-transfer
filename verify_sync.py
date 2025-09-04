#! /usr/bin/env python3
"""
Verify Dropbox -> Box sync using rclone listings (size + modtime)

Requirements:
- Python 3.9+
- rclone 1.71+ in PATH with remotes named "dropbox" and "box"

Usage:
    python verify_sync.py

Notes:
- Case-insensitive path matching.
- Modtime tolerance: ±120 seconds.
- Excludes common junk files (.DS_Store, Thumbs.db, ~$, ._*).
- If you only want to check certain top-level folders, set DIRS below.
"""

import csv, json, os, sys, subprocess, time
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Dict, Tuple, Optional, List
import fnmatch

# --------------------
# Configuration
# --------------------
DROPBOX_REMOTE = "dropbox:"       # Source root ("/" on Dropbox)
BOX_REMOTE     = "box:/SAOA"      # Destination root ("/SAOA" on Box)

# If you want to restrict to specific top-level dirs under Dropbox root, list them here:
# e.g., DIRS = ["Projects", "Team A", "Shared Stuff"]
DIRS: Optional[List[str]] = None  # or [] / None means "everything under /"

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
    "*.boxnote",     # Box Note stubs
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
    # Normalize to rclone remote:path (no double slashes).
    sub = subpath.lstrip("/")
    return f"{remote.rstrip('/')}/{sub}" if sub else remote.rstrip("/")


def should_exclude(path: str) -> bool:
    base = PurePosixPath(path).name
    return any(fnmatch.fnmatch(base, pat) for pat in EXCLUDE_BASENAME_PATTERNS)


def parse_rfc3339_modtime(s: str) -> Optional[float]:
    """
    rclone lsf 't' format: RFC3339, often with fractional seconds & 'Z'. Example:
      2024-08-09T12:34:56.123456789Z
    Convert to POSIX timestamp (float). Return None if missing/unparseable.
    """
    if not s or s == "-":
        return None
    try:
        # Trim excessive fractional seconds if present
        if "." in s:
            head, tail = s.split(".", 1)
            frac, tz = (tail[:-1], "Z") if s.endswith("Z") else tail.split("+", 1) if "+" in tail else (tail, "")
            frac = frac[:6]  # microseconds precision
            s2 = f"{head}.{frac}"
            if tz == "Z":
                s2 += "+00:00"
            elif tz:
                s2 += f"+{tz}"
        else:
            s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def run_rclone_lsf(remote_path: str) -> List[Tuple[str, Optional[float], Optional[int]]]:
    """
    Stream listing via rclone lsf with custom format:
      path \t modtime \t size
    Returns list of tuples: (relative_path, modtime_ts, size_int)
    """
    fields = "pst"  # p=path, s=size, t=modtime (RFC3339)
    sep = "\t"

    cmd = [
        "rclone", "lsf",
        "-R",
        "--files-only",
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
    # Expected order per 'fields': p, s, t  (BUT docs: order is exactly as provided: 'pst')
    for line in proc.stdout:  # type: ignore
        line = line.rstrip("\n")
        if not line:
            continue
        parts = line.split(sep)
        if len(parts) != len(fields):
            # Unexpected line; skip gracefully
            continue
        p = parts[0]
        s = parts[1]
        t = parts[2]

        if should_exclude(p):
            continue

        try:
            size = int(s) if s and s != "-" else None
        except ValueError:
            size = None

        ts = parse_rfc3339_modtime(t)

        # Normalize to posix-style, strip leading "./"
        norm_p = str(PurePosixPath(p))
        entries.append((norm_p, ts, size))

    # Drain stderr and check return code
    _, err = proc.communicate()
    if proc.returncode not in (0,):
        # rclone lsf returns non-zero on some warnings; surface details
        print("WARNING: rclone lsf returned non-zero.\n", err, file=sys.stderr)

    return entries


def build_index(entries: List[Tuple[str, Optional[float], Optional[int]]]) -> Dict[str, dict]:
    idx: Dict[str, dict] = {}
    for rel, ts, size in entries:
        key = rel.lower() if CASE_INSENSITIVE else rel
        idx[key] = {"path": rel, "modtime": ts, "size": size}
    return idx


def compare(src_idx: Dict[str, dict], dst_idx: Dict[str, dict]) -> Tuple[List[dict], List[dict], int]:
    """
    Returns:
      missing_on_dst: list of dicts {path, size, modtime}
      mismatches:     list of dicts {path, src_size, dst_size, src_modtime, dst_modtime, size_equal, modtime_diff_seconds, within_tolerance}
      matched_count:  int
    """
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
        # If either modtime is None, treat as mismatch (record diff=None)
        if src["modtime"] is None or dst["modtime"] is None:
            within_tol = False
            mtime_diff = None
        else:
            mtime_diff = abs(dst["modtime"] - src["modtime"])
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


def write_html(summary: dict, missing: List[dict], mismatches: List[dict], generated_at: str):
    nav_html = """
    <div class="nav">
      <a href="#summary">Summary</a>
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

    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title>Dropbox → Box Verification Report</title>
<style>/* existing CSS */</style>
</head>
<body>
  <h1>Dropbox → Box Verification Report</h1>
  <div class="meta">
    Generated: <strong>{generated_at}</strong><br/>
    Runtime: <strong>{summary["elapsed_seconds"]} seconds</strong><br/>
    Source: <code>{summary["src_remote"]}</code> → Destination: <code>{summary["dst_remote"]}</code><br/>
    Path mapping: <code>{summary["src_root"]}</code> → <code>{summary["dst_root"]}</code><br/>
    Case-insensitive compare; modtime tolerance ±{MODTIME_TOLERANCE_SECONDS}s; junk files ignored.
  </div>

  {nav_html}

  <h2 id="summary">Summary</h2>
  <div class="summary"> ... </div>

  <h2 id="missing">Missing on Box ({len(missing)})</h2>
  <table> ... </table>

  <h2 id="mismatches">Mismatches (size and/or modtime) ({len(mismatches)})</h2>
  <table> ... </table>
</body>
</html>"""

def main():
    ensure_reports_dir()
    started = time.time()
    generated_at = datetime.now(timezone.utc).isoformat()

    # Build source and destination listings
    # Default: everything under Dropbox root mapped to Box:/SAOA/*
    if DIRS:
        src_entries: List[Tuple[str, Optional[float], Optional[int]]] = []
        dst_entries: List[Tuple[str, Optional[float], Optional[int]]] = []
        for top in DIRS:
            src_remote_path = rclone_path(DROPBOX_REMOTE, top)
            dst_remote_path = rclone_path(BOX_REMOTE, top)
            src_entries.extend(run_rclone_lsf(src_remote_path))
            dst_entries.extend(run_rclone_lsf(dst_remote_path))
    else:
        src_remote_path = rclone_path(DROPBOX_REMOTE, "")
        dst_remote_path = rclone_path(BOX_REMOTE, "")
        src_entries = run_rclone_lsf(src_remote_path)
        dst_entries = run_rclone_lsf(dst_remote_path)

    # Index by relative path (case-insensitive if configured)
    src_idx = build_index(src_entries)
    dst_idx = build_index(dst_entries)

    # Compare
    missing_on_dst, mismatches, matched_count = compare(src_idx, dst_idx)

    total_src_files = len(src_idx)
    
    elapsed_seconds = round(time.time() - started, 2)
    counts = {
        "total_src_files": total_src_files,
        "matched": matched_count,
        "missing_on_dst": len(missing_on_dst),
        "mismatches": len(mismatches),
    }

    # Prepare JSON report
    report = {
        "generated_at": generated_at,
        "elapsed_seconds": elapsed_seconds,
        "total_src_files": total_src_files,
        "src_remote": DROPBOX_REMOTE,
        "dst_remote": BOX_REMOTE,
        "src_root": "/",
        "dst_root": "/SAOA",
        "case_insensitive": CASE_INSENSITIVE,
        "modtime_tolerance_seconds": MODTIME_TOLERANCE_SECONDS,
        "exclusions": EXCLUDE_BASENAME_PATTERNS,
        "counts": counts,
        "missing_on_box": missing_on_dst,
        "mismatches": mismatches,
    }

    # Write files
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    write_csv(missing_on_dst, mismatches)
    write_html({
        "generated_at": generated_at,
        "elapsed_seconds": elapsed_seconds,
        "src_remote": DROPBOX_REMOTE,
        "dst_remote": BOX_REMOTE,
        "src_root": "/",
        "dst_root": "/SAOA",
        "counts": counts,
    }, missing_on_dst, mismatches, generated_at)

    status = {
        "generated_at": generated_at,
        "elapsed_seconds": elapsed_seconds,
        "status": "pass" if counts["missing_on_dst"] == 0 and counts["mismatches"] == 0 else "fail",
        "counts": counts,
    }
    with open(STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    # Console summary
    print(f"[OK] Wrote {CSV_PATH}")
    print(f"[OK] Wrote {HTML_PATH}")
    print(f"[OK] Wrote {JSON_PATH}")
    print(f"[OK] Wrote {STATUS_PATH}")
    print(f"Summary: {counts}")
    print(f"Runtime: {elapsed_seconds}s")


if __name__ == "__main__":
    main()