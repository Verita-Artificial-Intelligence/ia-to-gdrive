#!/usr/bin/env python3
"""
Download books from Internet Archive to Google Drive via true diskless streaming.

Usage:
    python ia_books_to_gdrive.py -i books.txt --dry-run
    python ia_books_to_gdrive.py -i books.txt --drive-folder <FOLDER_ID_OR_LINK>
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
import time
from dataclasses import dataclass

import internetarchive as ia
import requests
from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

IA_THROTTLE_SECONDS = 1.0
IA_MAX_RESULTS = 30
DEFAULT_THRESHOLD = 75.0
MAX_RETRIES = 3

FORMAT_PRIORITY = ("Text PDF", "PDF", "EPUB")

REPORT_COLUMNS = [
    "query_title",
    "query_author",
    "matched_title",
    "matched_creator",
    "match_score",
    "runner_up_score",
    "ia_identifier",
    "ia_direct_url",
    "status",
    "drive_file_id",
]

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class BookQuery:
    title: str
    author: str | None = None


# ---------------------------------------------------------------------------
# Input parsing
# ---------------------------------------------------------------------------


def extract_folder_id(value: str | None) -> str | None:
    if not value:
        return None
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", value)
    if match:
        return match.group(1)
    match = re.search(r"id=([a-zA-Z0-9_-]+)", value)
    if match:
        return match.group(1)
    return value.split("?")[0].strip()


def parse_input(filepath: str) -> list[BookQuery]:
    queries: list[BookQuery] = []
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "|" in line:
                    parts = line.split("|", maxsplit=1)
                    title = parts[0].strip()
                    author = parts[1].strip() or None
                else:
                    title = line
                    author = None
                if not title:
                    continue
                queries.append(BookQuery(title=title, author=author))
    except OSError as e:
        sys.exit(f"Error: Cannot read input file: {e}")
    return queries


# ---------------------------------------------------------------------------
# Text normalization
# ---------------------------------------------------------------------------


def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# Internet Archive search
# ---------------------------------------------------------------------------


def search_ia(
    query: BookQuery, max_results: int = IA_MAX_RESULTS
) -> tuple[list[dict], bool]:
    title_terms = query.title
    if query.author:
        q = f"({title_terms}) AND creator:({query.author}) AND mediatype:(texts)"
    else:
        q = f"({title_terms}) AND mediatype:(texts)"

    results: list[dict] = []
    try:
        search = ia.search_items(
            q,
            fields=["identifier", "title", "creator", "downloads"],
            params={"rows": max_results},
        )
        for item in search:
            downloads = item.get("downloads", 0)
            try:
                downloads = int(downloads)
            except (TypeError, ValueError):
                downloads = 0
            results.append(
                {
                    "identifier": item.get("identifier", ""),
                    "title": item.get("title", ""),
                    "creator": item.get("creator", ""),
                    "downloads": downloads,
                }
            )
            if len(results) >= max_results:
                break
    except Exception as e:
        print(f"  Warning: IA search failed: {e}")
        time.sleep(IA_THROTTLE_SECONDS)
        return [], False

    time.sleep(IA_THROTTLE_SECONDS)
    return results, True


# ---------------------------------------------------------------------------
# Fuzzy matching
# ---------------------------------------------------------------------------


def score_candidate(query: BookQuery, candidate: dict) -> float:
    title_score = fuzz.token_set_ratio(
        normalize(query.title),
        normalize(candidate.get("title", "")),
    )

    author_bonus = 0.0
    if query.author and candidate.get("creator"):
        creator = candidate["creator"]
        if isinstance(creator, list):
            creator = " ".join(creator)
        author_similarity = fuzz.token_set_ratio(
            normalize(query.author),
            normalize(creator),
        )
        author_bonus = (author_similarity / 100.0) * 15

    return title_score + author_bonus


def find_best_match(
    query: BookQuery,
    ia_results: list[dict],
    threshold: float = DEFAULT_THRESHOLD,
) -> dict | None:
    if not ia_results:
        return None

    scored: list[tuple[float, dict]] = []
    for r in ia_results:
        if not r.get("identifier") or not r.get("title"):
            continue
        s = score_candidate(query, r)
        scored.append((s, r))

    if not scored:
        return None

    def _sort_key(item: tuple[float, dict]) -> tuple[float, float]:
        score, cand = item
        dl = cand.get("downloads", 0)
        try:
            dl = int(dl)
        except (TypeError, ValueError):
            dl = 0
        return (-score, -dl)

    scored.sort(key=_sort_key)

    best_score, best = scored[0]
    runner_up_score = scored[1][0] if len(scored) > 1 else 0.0

    if best_score < threshold:
        return None

    creator = best.get("creator", "")
    if isinstance(creator, list):
        creator = ", ".join(creator)

    return {
        "identifier": best["identifier"],
        "title": best["title"],
        "creator": creator,
        "score": round(best_score, 1),
        "runner_up_score": round(runner_up_score, 1),
    }


# ---------------------------------------------------------------------------
# Streaming pipeline (zero disk, minimal RAM)
# ---------------------------------------------------------------------------


class IARangeStream(io.RawIOBase):
    """
    A seekable wrapper around an HTTP stream that allows googleapiclient
    to do resumable chunk uploads without fetching the full file to RAM or disk.
    """

    # Shared across all streams so IA rate-limit cookies propagate globally.
    _session = None

    @classmethod
    def _get_session(cls) -> requests.Session:
        if cls._session is None:
            cls._session = requests.Session()
            cls._session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            })
        return cls._session

    def __init__(self, vanity_url: str, fallback_size: int):
        self.url = vanity_url
        self.position = 0
        self.size = fallback_size
        self.response = None
        self.session = self._get_session()

        # Determine exact remote file size using HEAD to avoid 416 range errors
        # if the IA metadata size deviates from the physical file length.
        # Using the shared session ensures any auth cookies from the redirect
        # chain are stored and reused for subsequent GET requests.
        try:
            head_resp = self.session.head(self.url, allow_redirects=True, timeout=15)
            if head_resp.status_code == 200 and "Content-Length" in head_resp.headers:
                self.size = int(head_resp.headers["Content-Length"])
        except Exception:
            pass

    def _connect(self):
        if self.response:
            self.response.close()

        headers = {}
        # Only send Range header if we are not at 0, skipping the problematic 'bytes=0-'
        if self.position > 0:
            headers = {"Range": f"bytes={self.position}-"}

        # Use the shared session so cookies from HEAD redirects are included.
        self.response = self.session.get(
            self.url, headers=headers, stream=True, timeout=(10, 60)
        )
        self.response.raise_for_status()

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self.position + offset
        elif whence == io.SEEK_END:
            new_pos = self.size + offset

        # Clamp explicitly
        new_pos = max(0, min(new_pos, self.size))

        if new_pos != self.position:
            self.position = new_pos
            if self.response:
                self.response.close()
                self.response = None

        return self.position

    def tell(self) -> int:
        return self.position

    def read(self, size: int = -1) -> bytes:
        if self.position >= self.size:
            return b""
            
        if self.response is None:
            self._connect()

        chunk = self.response.raw.read(size)
        self.position += len(chunk)
        return chunk

    def readinto(self, b: bytearray) -> int:
        chunk = self.read(len(b))
        ret = len(chunk)
        b[:ret] = chunk
        return ret
        
    def close(self):
        if self.response:
            self.response.close()
        super().close()


def stream_book_to_gdrive(
    service,
    identifier: str,
    folder_id: str | None = None,
    formats: tuple[str, ...] = FORMAT_PRIORITY,
    max_retries: int = MAX_RETRIES,
    progress_callback=None,
    status_callback=None,
) -> tuple[dict | None, str, str, str]:
    """
    Identifies the target IA file and streams it directly to Google Drive.
    Returns (drive_metadata, direct_url, status, error_detail)
    """
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaIoBaseUpload

    try:
        item = ia.get_item(identifier)
        all_files = list(item.get_files())
    except Exception as e:
        msg = f"Could not fetch item {identifier}: {e}"
        print(f"  Warning: {msg}")
        if status_callback:
            status_callback(msg)
        return None, "", "download_failed", msg

    target = None
    for fmt in formats:
        for f in all_files:
            if f.format == fmt:
                target = f
                break
        if target:
            break

    if target is None:
        msg = f"No file in formats {formats} found for item '{identifier}'"
        if status_callback:
            status_callback(msg)
        return None, "", "no_downloadable_file", msg

    try:
        meta_size = int(target.size)
    except (TypeError, ValueError):
        meta_size = 0

    direct_url = f"https://archive.org/download/{identifier}/{target.name}"
    
    file_metadata: dict = {"name": target.name}
    if folder_id:
        file_metadata["parents"] = [folder_id]

    mime_map = {".pdf": "application/pdf", ".epub": "application/epub+zip"}
    ext = os.path.splitext(target.name)[1].lower()
    mime_type = mime_map.get(ext, "application/octet-stream")

    print(f"  Streaming: {target.name} ({meta_size // 1024}KB) via dynamic RangeStream...")

    stream = IARangeStream(direct_url, meta_size)
    try:
        media = MediaIoBaseUpload(
            stream, mimetype=mime_type, chunksize=5 * 1024 * 1024, resumable=True
        )

        for attempt in range(1, max_retries + 1):
            try:
                request = service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields="id, name",
                    supportsAllDrives=True,
                )
                response = None
                while response is None:
                    status_obj, response = request.next_chunk()
                    if status_obj:
                        progress = int(status_obj.progress() * 100)
                        if progress_callback:
                            progress_callback(progress)
                        elif progress % 20 == 0:
                            print(f"    - Uploaded {progress}%")
                return response, direct_url, "success", ""
                
            except HttpError as e:
                status_code = e.resp.status if hasattr(e, "resp") else 0
                err_msg = f"Google API error {status_code}: {e}"
                if status_code in (429, 500, 502, 503) and attempt < max_retries:
                    wait = 2**attempt
                    print(f"  Upload attempt {attempt} failed ({status_code}), retrying in {wait}s")
                    if status_callback:
                        status_callback(f"Attempt {attempt} failed ({status_code}), retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"  Upload failed (HttpError): {e}")
                    if status_callback:
                        status_callback(err_msg)
                    return None, direct_url, "upload_failed", err_msg
            except Exception as e:
                err_msg = str(e)
                if attempt < max_retries:
                    wait = 2**attempt
                    print(f"  Upload attempt {attempt} interrupted: {e}, retrying in {wait}s")
                    if status_callback:
                        status_callback(f"Attempt {attempt}: {err_msg}, retrying in {wait}s...")
                    time.sleep(wait)
                    # Re-open the stream for the next attempt
                    stream.seek(0)
                else:
                    print(f"  Upload failed (Exception): {e}")
                    if status_callback:
                        status_callback(f"Upload failed after {max_retries} attempts: {err_msg}")
                    return None, direct_url, "upload_failed", err_msg
    finally:
        stream.close()

    return None, direct_url, "upload_failed", "Exhausted retries"


# ---------------------------------------------------------------------------
# Google Drive auth
# ---------------------------------------------------------------------------

SCOPES = ["https://www.googleapis.com/auth/drive"]

def get_drive_service(
    credentials_path: str = "credentials.json",
    token_path: str = "token.json",
):
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    creds = None
    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        except Exception:
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None

        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path, SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open(token_path, "w") as token:
            token.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


# ---------------------------------------------------------------------------
# Preflight validation
# ---------------------------------------------------------------------------


def preflight(
    credentials_path: str,
    token_path: str,
    output_dir: str,
    folder_id: str | None,
    dry_run: bool,
):
    os.makedirs(output_dir, exist_ok=True)

    if dry_run:
        return None

    if not os.path.exists(credentials_path):
        sys.exit(f"Error: Google credentials file not found: {credentials_path}")

    try:
        service = get_drive_service(credentials_path, token_path)
    except Exception as e:
        sys.exit(f"Error: Google Drive authentication failed: {e}")

    if folder_id:
        try:
            meta = (
                service.files()
                .get(fileId=folder_id, fields="id, name, mimeType", supportsAllDrives=True)
                .execute()
            )
            if meta.get("mimeType") != "application/vnd.google-apps.folder":
                sys.exit(
                    f"Error: Drive ID '{folder_id}' is not a folder "
                    f"(type: {meta.get('mimeType')})"
                )
            print(f"Drive folder verified: {meta.get('name', folder_id)}")
        except SystemExit:
            raise
        except Exception as e:
            sys.exit(f"Error: Cannot access Drive folder '{folder_id}': {e}")

    return service


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def _sanitize_csv(value: str) -> str:
    if value and value[0] in ("=", "+", "-", "@"):
        return "'" + value
    return value


def make_row(
    query: BookQuery,
    match: dict | None = None,
    status: str = "",
    drive_id: str = "",
    direct_url: str = "",
) -> dict:
    creator = ""
    if match:
        creator = match.get("creator", "")
        if isinstance(creator, list):
            creator = ", ".join(creator)

    return {
        "query_title": _sanitize_csv(query.title),
        "query_author": _sanitize_csv(query.author or ""),
        "matched_title": _sanitize_csv(match["title"] if match else ""),
        "matched_creator": _sanitize_csv(creator),
        "match_score": match["score"] if match else "",
        "runner_up_score": match.get("runner_up_score", "") if match else "",
        "ia_identifier": match["identifier"] if match else "",
        "ia_direct_url": direct_url,
        "status": status,
        "drive_file_id": drive_id,
    }


def write_report(results: list[dict], output_dir: str) -> str:
    path = os.path.join(output_dir, "report.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        writer.writerows(results)
    return path


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def run(
    input_file: str,
    output_dir: str,
    credentials_path: str,
    token_path: str,
    folder_id: str | None,
    threshold: float,
    dry_run: bool,
) -> None:
    service = preflight(credentials_path, token_path, output_dir, folder_id, dry_run)

    queries = parse_input(input_file)
    if not queries:
        print("No book titles found in input file.")
        write_report([], output_dir)
        return

    mode = "DRY RUN — search & match only" if dry_run else "full pipeline (diskless stream)"
    print(f"Processing {len(queries)} book(s) [{mode}]\n")

    results: list[dict] = []

    for i, query in enumerate(queries, 1):
        label = f"[{i}/{len(queries)}] \"{query.title}\""
        if query.author:
            label += f" by {query.author}"
        print(label)

        ia_results, search_ok = search_ia(query)
        if not search_ok:
            print("  ✗ Search failed (API error)")
            results.append(make_row(query, status="search_failed"))
            continue
        if not ia_results:
            print("  ✗ No results found on Internet Archive")
            results.append(make_row(query, status="no_results"))
            continue

        match = find_best_match(query, ia_results, threshold=threshold)
        if not match:
            print(f"  ✗ No match above threshold ({threshold})")
            results.append(make_row(query, status="below_threshold"))
            continue

        print(
            f"  Matched: \"{match['title']}\" "
            f"(score: {match['score']}, runner-up: {match['runner_up_score']})"
        )

        if dry_run:
            results.append(make_row(query, match=match, status="dry_run"))
            continue

        drive_file, direct_url, dl_status, err_detail = stream_book_to_gdrive(
            service, match["identifier"], folder_id
        )

        if not drive_file:
            print("  ✗ Streaming upload failed")
            results.append(
                make_row(
                    query, match=match, status=dl_status, direct_url=direct_url
                )
            )
            continue

        print(f"  ✓ Uploaded to Drive (ID: {drive_file['id']})")
        results.append(
            make_row(
                query,
                match=match,
                status="success",
                drive_id=drive_file["id"],
                direct_url=direct_url, # Now accurately pointing to correct URL!
            )
        )

    try:
        report_path = write_report(results, output_dir)
    except OSError as e:
        print(f"\nError: Could not write report: {e}")
        report_path = "(failed to write)"

    print("\n" + "=" * 60)
    success = sum(1 for r in results if r["status"] == "success")
    matched = sum(1 for r in results if r["status"] in ("success", "dry_run"))
    failed = len(results) - matched
    print(f"Done. {matched} matched, {success} uploaded, {failed} skipped/failed.")
    print(f"Report: {report_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download books from Internet Archive to Google Drive natively.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="Path to TXT file with book titles",
    )
    parser.add_argument(
        "--drive-folder",
        default=None,
        help="Google Drive folder ID or full link to upload into",
    )
    parser.add_argument(
        "--credentials",
        default="credentials.json",
        help="Path to Google OAuth credentials.json",
    )
    parser.add_argument(
        "--token",
        default="token.json",
        help="Path to store OAuth token",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"Fuzzy match score threshold 0-100",
    )
    parser.add_argument(
        "--output-dir",
        default="./downloads",
        help="Local directory for strictly resolving report.csv",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Search and match only — don't download or upload",
    )

    args = parser.parse_args()

    if not os.path.isfile(args.input):
        sys.exit(f"Error: Input file not found or is not a file: {args.input}")

    folder_id = extract_folder_id(args.drive_folder)

    run(
        input_file=args.input,
        output_dir=args.output_dir,
        credentials_path=args.credentials,
        token_path=args.token,
        folder_id=folder_id,
        threshold=args.threshold,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
