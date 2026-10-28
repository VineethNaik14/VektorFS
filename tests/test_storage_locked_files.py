"""Windows refuses to delete/replace a chunk while another handle has it open
(WinError 32). These tests simulate that on any OS."""

import os

import pytest

from node.storage import StorageManager


def _in_use() -> PermissionError:
    exc = PermissionError(13, "The process cannot access the file because it is being used")
    exc.winerror = 32           # only exists natively on Windows; set it so we can simulate there
    return exc


def test_delete_retries_while_file_is_in_use(tmp_path, monkeypatch):
    sm = StorageManager(tmp_path)
    sm.store("abc", b"data")
    real_unlink, calls = type(tmp_path).unlink, {"n": 0}

    def flaky(self, *a, **kw):
        if self.name == "abc" and calls["n"] < 3:
            calls["n"] += 1
            raise _in_use()
        return real_unlink(self, *a, **kw)

    monkeypatch.setattr(type(tmp_path), "unlink", flaky)
    sm.delete("abc")
    assert calls["n"] == 3 and not sm.exists("abc")


def test_store_retries_replace_while_old_chunk_is_being_read(tmp_path, monkeypatch):
    sm = StorageManager(tmp_path)
    sm.store("abc", b"old")
    real_replace, calls = os.replace, {"n": 0}

    def flaky(src, dst):
        if calls["n"] < 2:
            calls["n"] += 1
            raise _in_use()
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", flaky)
    sm.store("abc", b"new")
    assert sm.get("abc") == b"new"


def test_real_permission_errors_are_not_retried(tmp_path, monkeypatch):
    sm = StorageManager(tmp_path)
    sm.store("abc", b"data")
    calls = {"n": 0}

    def denied(self, *a, **kw):
        calls["n"] += 1
        raise PermissionError(13, "Access is denied")   # no winerror 32

    monkeypatch.setattr(type(tmp_path), "unlink", denied)
    with pytest.raises(PermissionError):
        sm.delete("abc")
    assert calls["n"] == 1
