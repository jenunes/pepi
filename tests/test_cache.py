from __future__ import annotations

from pathlib import Path

import pepi.cache as cache


def test_cache_roundtrip(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    key = "unit-test-key"
    payload = {"value": 42}

    cache.save_to_cache(key, payload)
    loaded = cache.load_from_cache(key)

    assert loaded == payload


def test_get_file_hash_changes_when_content_changes(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.log"
    file_path.write_text("one\n", encoding="utf-8")
    first_hash = cache.get_file_hash(file_path)

    file_path.write_text("one\ntwo\n", encoding="utf-8")
    second_hash = cache.get_file_hash(file_path)

    assert first_hash != second_hash
