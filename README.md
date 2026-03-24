# Internet Archive Books to Google Drive

CLI script that takes a list of book titles, searches Internet Archive, downloads the best matching available file, uploads it to Google Drive, and writes a CSV report. It is meant for batch runs from a text file, not interactive use.

## Inputs

- a text file with one book per line
- optional author after `|`
- Google OAuth credentials JSON
- optional Google Drive folder ID
- optional flags such as `--threshold`, `--output-dir`, `--dry-run`, and `--cleanup`

## Outputs

- downloaded book files in the output directory
- `report.csv` with one row per input line
- `token.json` after the first successful Google OAuth flow

## Input File

One book per line. Optional author after `|`.

```txt
Moby Dick | Herman Melville
The Great Gatsby
Meditations | Marcus Aurelius
```

Rules:

- blank lines are ignored
- lines starting with `#` are ignored
- author is optional
- use authors for ambiguous titles

Sample file: [`books.txt`](/Users/vrn21/Developer/verita/utils/books.txt)

## Setup

Requirements:

- Python 3.10+
- `uv`
- Google OAuth client credentials JSON for Drive API

Install:

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Google Drive

Put your OAuth credentials file at `./credentials.json`, or pass a different path with `--credentials`.

On the first non-dry run, the script opens the OAuth flow and writes `token.json`.

If `--drive-folder` is not provided, uploads go to the Drive root.

## How to get `credentials.json`

1. Open [Google Cloud Console](https://console.cloud.google.com/).
2. Create or select a project.
3. Enable the Google Drive API for that project.
4. Open `APIs & Services` -> `OAuth consent screen` and configure it.
5. Open `APIs & Services` -> `Credentials`.
6. Click `Create Credentials` -> `OAuth client ID`.
7. Choose application type `Desktop app`.
8. Create the client.
9. Download the JSON file.
10. Save it in this project as `credentials.json`.

That is the file this script uses for the Google OAuth flow.

## Run

Dry run:

```bash
uv run ia_books_to_gdrive.py -i books.txt --dry-run
```

Upload to a Drive folder:

```bash
uv run ia_books_to_gdrive.py -i books.txt --drive-folder YOUR_FOLDER_ID
```

Upload to Drive root:

```bash
uv run ia_books_to_gdrive.py -i books.txt
```

Upload and remove local file after success:

```bash
uv run ia_books_to_gdrive.py \
  -i books.txt \
  --drive-folder YOUR_FOLDER_ID \
  --cleanup
```

Stricter matching:

```bash
uv run ia_books_to_gdrive.py \
  -i books.txt \
  --drive-folder YOUR_FOLDER_ID \
  --threshold 85
```

## What It Does

For each line in the input file, the script:

1. searches Internet Archive
2. scores the search results with fuzzy title matching
3. adds author weight if author was provided
4. picks the best match above the threshold
5. downloads the first available `Text PDF`, `PDF`, or `EPUB`
6. uploads the file to Google Drive
7. writes one row to `report.csv`

If no result is good enough, nothing is downloaded for that line.

## CLI Options

- `-i, --input`: input text file
- `--drive-folder`: Drive folder ID
- `--credentials`: OAuth credentials JSON path
- `--token`: token JSON path
- `--threshold`: match threshold, default `75.0`
- `--output-dir`: output directory, default `./downloads`
- `--dry-run`: search and match only
- `--cleanup`: remove local file after successful upload

## Output

Default output directory: `./downloads`

Files created:

- downloaded book files
- `report.csv`

## Report Columns

- `query_title`
- `query_author`
- `matched_title`
- `matched_creator`
- `match_score`
- `runner_up_score`
- `ia_identifier`
- `local_file`
- `status`
- `drive_file_id`

## Status Values

- `dry_run`
- `success`
- `search_failed`
- `no_results`
- `below_threshold`
- `no_downloadable_file`
- `download_failed`
- `upload_failed`

## Limits

- no UI
- no duplicate detection
- no resume state across runs
- no service account auth
- no restricted/borrow-only IA items

## Main Files

- [`ia_books_to_gdrive.py`](/Users/vrn21/Developer/verita/utils/ia_books_to_gdrive.py)
- [`books.txt`](/Users/vrn21/Developer/verita/utils/books.txt)
- [`test_ia_books_to_gdrive.py`](/Users/vrn21/Developer/verita/utils/test_ia_books_to_gdrive.py)
