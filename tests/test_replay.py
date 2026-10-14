"""
test_replay.py
--------------
Mock tests for replay/replay.py: failed records are republished through the
(patched) Kafka producer, the ones that fail again stay in the file, and the
function reports how many were replayed / remain (pipeline_tasks alerts on it).
"""

import json

import pytest

import replay.replay as replay


@pytest.fixture
def failed_file(tmp_path, monkeypatch):
    path = tmp_path / "failed_products.json"
    monkeypatch.setattr(replay, "FAILED_FILE", str(path))
    return path


def test_no_file_means_nothing_to_replay(failed_file):
    assert replay.replay_failed_records() == {"replayed": 0, "remaining": 0}


def test_empty_file_means_nothing_to_replay(failed_file):
    failed_file.write_text("[]")
    assert replay.replay_failed_records() == {"replayed": 0, "remaining": 0}


def test_records_that_fail_again_stay_in_the_file(failed_file, mocker):
    failed_file.write_text(json.dumps([{"id": 1}, {"id": 2}, {"id": 3}]))
    mocker.patch("replay.replay.send_product", side_effect=[True, False, True])

    assert replay.replay_failed_records() == {"replayed": 2, "remaining": 1}
    assert json.loads(failed_file.read_text()) == [{"id": 2}]
