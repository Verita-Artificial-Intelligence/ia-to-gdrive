#!/usr/bin/env python3
"""Unit tests for ia_books_to_gdrive.py (Diskless Streaming Architecture)."""

from __future__ import annotations

import csv
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from ia_books_to_gdrive import (
    BookQuery,
    find_best_match,
    make_row,
    normalize,
    parse_input,
    score_candidate,
    write_report,
    search_ia,
    _sanitize_csv,
    REPORT_COLUMNS,
    IARangeStream,
    stream_book_to_gdrive,
    extract_folder_id,
)


class TestExtractFolderId(unittest.TestCase):
    def test_full_url(self):
        url = "https://drive.google.com/drive/folders/1Gt2j-Fj2pc4yXi4HDI0khAoYZQ9vuJdp?usp=sharing"
        self.assertEqual(extract_folder_id(url), "1Gt2j-Fj2pc4yXi4HDI0khAoYZQ9vuJdp")

    def test_open_url(self):
        url = "https://drive.google.com/open?id=1Gt2j-Fj2pc4yXi4HDI0khAoYZQ9vuJdp"
        self.assertEqual(extract_folder_id(url), "1Gt2j-Fj2pc4yXi4HDI0khAoYZQ9vuJdp")

    def test_plain_id(self):
        self.assertEqual(extract_folder_id("1Gt2j-Fj2pc4yXi4HDI0khAoYZQ9vuJdp"), "1Gt2j-Fj2pc4yXi4HDI0khAoYZQ9vuJdp")

    def test_plain_id_with_query(self):
        self.assertEqual(extract_folder_id("1Gt2j-Fj2pc4yXi4HDI0khAoYZQ9vuJdp?usp=sharing"), "1Gt2j-Fj2pc4yXi4HDI0khAoYZQ9vuJdp")

    def test_none(self):
        self.assertIsNone(extract_folder_id(None))
        self.assertIsNone(extract_folder_id(""))


class TestNormalize(unittest.TestCase):
    def test_lowercase(self):
        self.assertEqual(normalize("Hello World"), "hello world")

    def test_strip_punctuation(self):
        self.assertEqual(normalize("War & Peace: A Novel"), "war peace a novel")


class TestParseInput(unittest.TestCase):
    def _write_and_parse(self, content: str) -> list[BookQuery]:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write(content)
            f.flush()
            path = f.name
        try:
            return parse_input(path)
        finally:
            os.unlink(path)

    def test_simple_titles(self):
        queries = self._write_and_parse("Moby Dick\n")
        self.assertEqual(len(queries), 1)
        self.assertEqual(queries[0].title, "Moby Dick")


class TestScoreCandidate(unittest.TestCase):
    def test_exact_title_match(self):
        query = BookQuery(title="War and Peace")
        candidate = {"title": "War and Peace", "creator": ""}
        score = score_candidate(query, candidate)
        self.assertEqual(score, 100.0)


class TestFindBestMatch(unittest.TestCase):
    def test_selects_best(self):
        query = BookQuery(title="Moby Dick")
        results = [
            {"identifier": "mobydick01", "title": "Moby Dick", "creator": "Melville", "downloads": 100},
            {"identifier": "mobydick02", "title": "Moby Dick or The Whale", "creator": "Melville", "downloads": 50},
        ]
        match = find_best_match(query, results, threshold=75)
        self.assertIsNotNone(match)
        self.assertEqual(match["identifier"], "mobydick01")


class TestSearchIA(unittest.TestCase):
    @patch("ia_books_to_gdrive.ia.search_items")
    @patch("ia_books_to_gdrive.time.sleep")
    def test_returns_results_on_success(self, mock_sleep, mock_search):
        mock_search.return_value = iter([
            {"identifier": "book1", "title": "Test Book", "creator": "Author", "downloads": 42},
        ])
        results, ok = search_ia(BookQuery(title="Test Book"))
        self.assertTrue(ok)
        self.assertEqual(len(results), 1)


class TestIARangeStream(unittest.TestCase):
    @patch.object(IARangeStream, "_get_session")
    def test_stream_flow(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        # Mock HEAD response (for size detection)
        mock_head_resp = MagicMock()
        mock_head_resp.status_code = 200
        mock_head_resp.headers = {"Content-Length": "1000"}
        mock_session.head.return_value = mock_head_resp

        # Mock GET response (for streaming)
        mock_resp = MagicMock()
        mock_resp.raw.read.return_value = b"testdata"
        mock_session.get.return_value = mock_resp

        # Reset class-level session so our mock is used
        IARangeStream._session = None

        stream = IARangeStream("http://fake.url", 500)
        
        # HEAD should correctly assign size
        self.assertEqual(stream.size, 1000)

        # seek(0, SEEK_END) shouldn't reconnect because we read nothing yet
        stream.seek(0, __import__("io").SEEK_END)
        self.assertEqual(stream.tell(), 1000)
        self.assertFalse(mock_session.get.called)

        stream.seek(0, __import__("io").SEEK_SET)
        stream.read(8)
        self.assertEqual(stream.tell(), 8)
        self.assertTrue(mock_session.get.called)
        
        # Cleanup
        IARangeStream._session = None


class TestStreamBookToGdrive(unittest.TestCase):
    @patch("ia_books_to_gdrive.ia.get_item")
    def test_get_item_raises(self, mock_get_item):
        mock_get_item.side_effect = Exception("Network error")
        meta, url, status, err = stream_book_to_gdrive(None, "bad_id")
        self.assertIsNone(meta)
        self.assertEqual(status, "download_failed")


class TestWriteReport(unittest.TestCase):
    def test_writes_csv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            results = [{"query_title": "Test", "ia_direct_url": "url"}]
            # Pad the row with empty strings for all missing REPORT_COLUMNS
            full_results = []
            for r in results:
                d = {col: "" for col in REPORT_COLUMNS}
                d.update(r)
                full_results.append(d)
                
            path = write_report(full_results, tmpdir)
            self.assertTrue(os.path.exists(path))


if __name__ == "__main__":
    unittest.main()
