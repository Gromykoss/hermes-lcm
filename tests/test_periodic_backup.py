"""Deterministic acceptance matrix for process-local periodic backups.

The tests use real LCMEngine construction and production backup entry points.
Interleavings are controlled with Events; no correctness assertion depends on a
sleep.  Every test has bounded waits and the autouse fixture proves teardown.
"""

from __future__ import annotations

import json
import os
import shutil
import threading
import time
from pathlib import Path

import pytest

from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine
import hermes_lcm.periodic_backup as pb


@pytest.fixture(autouse=True)
def clean_registry():
    pb._reset_registry_for_tests()
    yield
    pb._reset_registry_for_tests()
    assert not [
        thread
        for thread in threading.enumerate()
        if thread.name.startswith("lcm-periodic-backup")
    ]


def _private_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)
    return path


def _config(tmp_path: Path, **overrides) -> tuple[LCMConfig, Path, Path]:
    home = _private_dir(tmp_path / "home")
    root = _private_dir(home / "lcm-large-outputs")
    destination = _private_dir(tmp_path / "backup")
    values = {
        "database_path": str(tmp_path / "lcm.db"),
        "periodic_backup_enabled": True,
        "periodic_backup_interval_seconds": 3600.0,
        "periodic_backup_keep_last": 2,
        "periodic_backup_destination": str(destination),
    }
    values.update(overrides)
    return LCMConfig(**values), home, root


def _blocking_worker(monkeypatch, *, ignore_cancel: bool = False):
    entered = threading.Event()
    exit_worker = threading.Event()
    calls: list[str] = []

    def blocked(snapshot, *, cancel, fault_hook=None):
        calls.append(snapshot.worker_token)
        entered.set()
        if ignore_cancel:
            assert exit_worker.wait(2)
        else:
            cancel.wait(2)
        raise pb.PublicationCancelled("test worker exit")

    monkeypatch.setattr(pb, "run_periodic_backup", blocked)
    return entered, exit_worker, calls


def _snapshot(engine: LCMEngine) -> pb.BackupSnapshot:
    source_key = str(Path(engine._store.db_path).resolve(strict=True))
    with pb._REGISTRY_LOCK:
        return pb._snapshot_locked(pb._SOURCES[source_key])


def _namespace(snapshot: pb.BackupSnapshot) -> Path:
    return pb._source_namespace(snapshot, create=False)


def _publish(original_run, snapshot, *, hook=None):
    return original_run(snapshot, cancel=threading.Event(), fault_hook=hook)


def _pointer_bytes(snapshot: pb.BackupSnapshot) -> bytes | None:
    pointer = _namespace(snapshot) / "latest-good.json"
    return pointer.read_bytes() if pointer.exists() and pointer.is_file() else None


def _generation_names(snapshot: pb.BackupSnapshot) -> list[str]:
    namespace = _namespace(snapshot)
    if not namespace.exists():
        return []
    return sorted(
        entry.name for entry in namespace.iterdir()
        if entry.name.startswith("generation-") and entry.is_dir()
    )


def test_default_off_has_no_backup_record_thread_or_filesystem_io(tmp_path):
    missing_destination = tmp_path / "must-not-exist"
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "disabled.db"),
            periodic_backup_enabled=False,
            periodic_backup_destination=str(missing_destination),
        )
    )
    try:
        assert engine._periodic_backup_registration.status == "disabled"
        assert engine._periodic_backup_lease_handle is None
        assert pb._SOURCES == {}
        assert not missing_destination.exists()
        engine._store.append("s", {"role": "user", "content": "available"})
    finally:
        engine.shutdown()


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("periodic_backup_interval_seconds", 0, "invalid_interval"),
        ("periodic_backup_interval_seconds", float("inf"), "invalid_interval"),
        ("periodic_backup_interval_seconds", "not-a-number", "invalid_interval"),
        ("periodic_backup_keep_last", 0, "invalid_keep_last"),
        ("periodic_backup_keep_last", 1.5, "invalid_keep_last"),
        ("periodic_backup_destination", "", "invalid_destination"),
    ],
)
def test_invalid_configuration_is_typed_and_engine_remains_usable(
    tmp_path, monkeypatch, field, value, reason
):
    config, home, _root = _config(tmp_path)
    setattr(config, field, value)
    engine = LCMEngine(config=config, hermes_home=str(home))
    try:
        assert engine._periodic_backup_registration == pb.Registration("error", reason)
        assert engine._periodic_backup_lease_handle is None
        assert pb._SOURCES == {}
        assert engine._store.append("s", {"role": "user", "content": reason}) > 0
    finally:
        engine.shutdown()


def test_strict_backup_environment_values_are_not_coerced(monkeypatch):
    monkeypatch.setenv("LCM_PERIODIC_BACKUP_ENABLED", "true")
    monkeypatch.setenv("LCM_PERIODIC_BACKUP_INTERVAL_SECONDS", "bogus")
    monkeypatch.setenv("LCM_PERIODIC_BACKUP_KEEP_LAST", "2.5")
    config = LCMConfig.from_env()
    assert config.periodic_backup_enabled == "true"
    assert config.periodic_backup_interval_seconds == "bogus"
    assert config.periodic_backup_keep_last == "2.5"


def test_r2_compatible_clone_shares_one_source_root_and_worker(tmp_path, monkeypatch):
    entered, _exit, calls = _blocking_worker(monkeypatch)
    config, home, _root = _config(tmp_path)
    first = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    clone = first.clone_for_agent()
    try:
        key = str(Path(first._store.db_path).resolve())
        status = pb.periodic_backup_source_status(key)
        assert status is not None
        assert status["state"] == "ACTIVE"
        assert status["lease_count"] == 2
        assert status["worker_alive"]
        assert len(calls) == 1
        assert clone._periodic_backup_lease_handle != first._periodic_backup_lease_handle
        assert clone._periodic_backup_registration.status == "active"
    finally:
        clone.shutdown()
        first.shutdown()


def test_n1_different_root_rebind_suspends_source_and_never_readmits(tmp_path, monkeypatch):
    entered, _exit, _calls = _blocking_worker(monkeypatch)
    config, home, _root = _config(tmp_path)
    engine = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    original_handle = engine._periodic_backup_lease_handle
    other_home = _private_dir(tmp_path / "other-home")
    _private_dir(other_home / "lcm-large-outputs")

    assert engine._rebind_storage_for_home(str(other_home))
    key = str(Path(engine._store.db_path).resolve())
    suspended = pb.periodic_backup_source_status(key)
    assert suspended is not None
    assert suspended["state"] == "SUSPENDED"
    assert suspended["root_continuity"] == "broken"
    assert suspended["epoch"] >= 1
    assert engine._periodic_backup_lease_handle == original_handle
    assert engine._periodic_backup_registration.status == "suspended"

    registration, handle = pb.acquire_backup_lease(engine)
    assert registration.status == "suspended"
    assert handle is None
    engine.shutdown()


def test_r1_deleted_recreated_root_does_not_restore_authority(tmp_path, monkeypatch):
    entered, exit_worker, _calls = _blocking_worker(monkeypatch, ignore_cancel=True)
    config, home, root = _config(tmp_path)
    engine = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    old_observation = _snapshot(engine).root_observation
    shutil.rmtree(root)
    _private_dir(root)

    registration, handle = pb.acquire_backup_lease(engine)
    assert registration.status == "suspended"
    assert handle is None
    key = str(Path(engine._store.db_path).resolve())
    status = pb.periodic_backup_source_status(key)
    assert status and status["root_continuity"] == "broken"
    # Even an injected equal observation cannot convert the terminal tombstone.
    with pb._REGISTRY_LOCK:
        pb._SOURCES[key].candidate = pb.BackupCandidate(
            **{
                **pb._SOURCES[key].candidate.__dict__,
                "root_observation": old_observation,
            }
        )
    registration, handle = pb.acquire_backup_lease(engine)
    assert registration.status == "suspended"
    assert handle is None
    engine.shutdown()
    exit_worker.set()


def test_l1_only_exact_handle_releases_and_conflict_cannot_release_owner(tmp_path, monkeypatch):
    entered, _exit, _calls = _blocking_worker(monkeypatch)
    config, home, _root = _config(tmp_path)
    owner = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    key = str(Path(owner._store.db_path).resolve())
    pb.release_backup_lease(owner._periodic_backup_registration)  # presentation is inert
    assert pb.periodic_backup_source_status(key)["lease_count"] == 1

    conflicting = LCMEngine(
        config=LCMConfig(
            **{**config.__dict__, "periodic_backup_keep_last": 99}
        ),
        hermes_home=str(home),
    )
    try:
        assert conflicting._periodic_backup_registration.reason == "policy_conflict"
        assert conflicting._periodic_backup_lease_handle is None
        assert pb.periodic_backup_source_status(key)["lease_count"] == 1
    finally:
        conflicting.shutdown()
        assert pb.periodic_backup_source_status(key)["lease_count"] == 1
        owner.shutdown()
        pb.release_backup_lease(owner._periodic_backup_lease_handle)


@pytest.mark.parametrize("source_state", ["ACTIVE", "STOPPING", "SUSPENDED", "ERROR"])
def test_l2_policy_conflict_precedes_state_and_never_creates_a_second_source(
    tmp_path, monkeypatch, source_state
):
    entered, _exit, _calls = _blocking_worker(monkeypatch)
    config, home, _root = _config(tmp_path)
    owner = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    key = str(Path(owner._store.db_path).resolve())
    with pb._REGISTRY_LOCK:
        source = pb._SOURCES[key]
        source.state = source_state
        if source_state == "SUSPENDED":
            source.root_continuity = "broken"
    conflicting = LCMEngine(
        config=LCMConfig(
            **{**config.__dict__, "periodic_backup_interval_seconds": 99.0}
        ),
        hermes_home=str(home),
    )
    try:
        assert conflicting._periodic_backup_registration == pb.Registration(
            "error", "policy_conflict"
        )
        assert conflicting._periodic_backup_lease_handle is None
        assert list(pb._SOURCES) == [key]
        assert pb.periodic_backup_source_status(key)["lease_count"] == 1
    finally:
        conflicting.shutdown()
        owner.shutdown()


def test_n2_delayed_stop_admits_pending_and_starts_one_successor(tmp_path, monkeypatch):
    entered, exit_worker, calls = _blocking_worker(monkeypatch, ignore_cancel=True)
    config, home, _root = _config(tmp_path)
    first = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    first.shutdown()
    key = str(Path(first._store.db_path).resolve())
    assert pb.periodic_backup_source_status(key)["state"] == "STOPPING"

    second = LCMEngine(config=config, hermes_home=str(home))
    third = LCMEngine(config=config, hermes_home=str(home))
    try:
        assert second._periodic_backup_registration.status == "pending"
        assert third._periodic_backup_registration.status == "pending"
        third.shutdown()  # exact cancellation leaves second pending
        assert pb.periodic_backup_source_status(key)["lease_count"] == 1
        exit_worker.set()
        deadline = time.monotonic() + 2
        while len(calls) < 2 and time.monotonic() < deadline:
            threading.Event().wait(0.01)
        assert len(calls) == 2
        status = pb.periodic_backup_source_status(key)
        assert status and status["state"] == "ACTIVE" and status["lease_count"] == 1
    finally:
        second.shutdown()


def test_pending_all_cancelled_prevents_successor(tmp_path, monkeypatch):
    entered, exit_worker, calls = _blocking_worker(monkeypatch, ignore_cancel=True)
    config, home, _root = _config(tmp_path)
    first = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    first.shutdown()
    second = LCMEngine(config=config, hermes_home=str(home))
    second.shutdown()
    exit_worker.set()
    pb._reset_registry_for_tests()
    assert len(calls) == 1


@pytest.mark.parametrize("fault_name", ["initial", "watcher", "successor"])
def test_n4_thread_start_faults_are_typed_usable_and_leak_free(
    tmp_path, monkeypatch, fault_name
):
    entered, exit_worker, _calls = _blocking_worker(monkeypatch, ignore_cancel=True)
    real_start = threading.Thread.start
    armed = {"value": True}

    def start(thread):
        name = thread.name
        should_fail = (
            (fault_name == "initial" and name.startswith("lcm-periodic-backup-") and "watcher" not in name and "successor" not in name)
            or (fault_name == "watcher" and "backup-watcher" in name)
            or (fault_name == "successor" and "backup-successor" in name)
        )
        if armed["value"] and should_fail:
            armed["value"] = False
            raise RuntimeError(f"{fault_name}-start")
        return real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", start)
    config, home, _root = _config(tmp_path)
    engine = LCMEngine(config=config, hermes_home=str(home))
    key = str(Path(engine._store.db_path).resolve())
    if fault_name == "initial":
        assert engine._periodic_backup_registration.status == "error"
        assert "worker_start_failed" in engine._periodic_backup_registration.reason
        assert engine._store.append("s", {"role": "user", "content": "usable"}) > 0
        engine.shutdown()
        return

    assert entered.wait(2)
    engine.shutdown()
    if fault_name == "watcher":
        status = pb.periodic_backup_source_status(key)
        assert status and status["state"] == "ERROR"
        exit_worker.set()
        return

    pending = LCMEngine(config=config, hermes_home=str(home))
    armed["value"] = True
    exit_worker.set()
    deadline = time.monotonic() + 2
    status = None
    while time.monotonic() < deadline:
        status = pb.periodic_backup_source_status(key)
        if status and status["state"] == "ERROR":
            break
        threading.Event().wait(0.01)
    assert status and status["state"] == "ERROR"
    assert "worker_start_failed" in status["last_error"]
    assert pending._store.append("s", {"role": "user", "content": "usable"}) > 0
    pending.shutdown()


def test_n3_publication_pointer_verification_and_retention(tmp_path, monkeypatch):
    original_run = pb.run_periodic_backup
    entered, _exit, _calls = _blocking_worker(monkeypatch)
    config, home, _root = _config(tmp_path)
    engine = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    try:
        snapshots = []
        pointer = None
        for _ in range(3):
            snapshot = _snapshot(engine)
            snapshots.append(snapshot)
            pointer = _publish(original_run, snapshot)
            assert pointer == pb.read_verified_pointer(snapshot)
        assert len(_generation_names(snapshots[-1])) == 2
        assert pointer["generation"] in _generation_names(snapshots[-1])
    finally:
        engine.shutdown()


def test_n3_same_size_staged_mutation_rejects_without_pointer_or_retention_change(
    tmp_path, monkeypatch
):
    original_run = pb.run_periodic_backup
    entered, _exit, _calls = _blocking_worker(monkeypatch)
    config, home, root = _config(tmp_path)
    engine = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    payload_name = "payload.json"
    (root / payload_name).write_text(json.dumps({"content": "AAAA"}))
    (root / payload_name).chmod(0o600)
    engine._store.append(
        "s",
        {"role": "tool", "content": f"[Externalized payload: chars=4; ref={payload_name}]"},
    )
    first = _snapshot(engine)
    _publish(original_run, first)
    pointer_before = _pointer_bytes(first)
    generations_before = _generation_names(first)

    second = _snapshot(engine)

    def mutate(stage, snapshot):
        if stage == "after_initial_validation":
            path = _namespace(snapshot) / f".stage-{snapshot.generation_id}" / payload_name
            data = path.read_bytes()
            path.write_bytes(data[:-1] + (b" " if data[-1:] != b" " else b"x"))
            path.chmod(0o600)

    with pytest.raises(pb.BackupError, match="generation_byte_mismatch"):
        _publish(original_run, second, hook=mutate)
    assert _pointer_bytes(first) == pointer_before
    assert _generation_names(first) == generations_before
    engine.shutdown()


def test_n3_verified_pointer_rejects_altered_published_generation(
    tmp_path, monkeypatch
):
    original_run = pb.run_periodic_backup
    entered, _exit, _calls = _blocking_worker(monkeypatch)
    config, home, _root = _config(tmp_path)
    engine = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    snapshot = _snapshot(engine)
    pointer = _publish(original_run, snapshot)
    raw_pointer = _pointer_bytes(snapshot)
    database = _namespace(snapshot) / pointer["generation"] / "lcm.db"
    data = database.read_bytes()
    database.write_bytes(data[:-1] + bytes([data[-1] ^ 1]))
    database.chmod(0o600)

    with pytest.raises(pb.UnsafeExistingPointer):
        pb.read_verified_pointer(snapshot)
    assert _pointer_bytes(snapshot) == raw_pointer
    engine.shutdown()


def test_retention_preserves_unrecognized_generation_directory(tmp_path, monkeypatch):
    original_run = pb.run_periodic_backup
    entered, _exit, _calls = _blocking_worker(monkeypatch)
    config, home, _root = _config(tmp_path, periodic_backup_keep_last=1)
    engine = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    first = _snapshot(engine)
    _publish(original_run, first)
    foreign = _namespace(first) / ("generation-" + "f" * 32)
    foreign.mkdir(mode=0o700)
    (foreign / "operator-data").write_text("preserve")
    (foreign / "operator-data").chmod(0o600)

    _publish(original_run, _snapshot(engine))
    assert (foreign / "operator-data").read_text() == "preserve"
    engine.shutdown()


def test_g2_release_before_claim_cannot_publish(tmp_path, monkeypatch):
    original_run = pb.run_periodic_backup
    entered, exit_worker, _calls = _blocking_worker(monkeypatch, ignore_cancel=True)
    config, home, _root = _config(tmp_path)
    engine = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    snapshot = _snapshot(engine)
    at_g2 = threading.Event()
    continue_g2 = threading.Event()
    result: list[BaseException] = []

    def barrier(stage, _snapshot_value):
        if stage == "after_final_validation_before_commit_claim":
            at_g2.set()
            assert continue_g2.wait(2)

    def publish():
        try:
            _publish(original_run, snapshot, hook=barrier)
        except BaseException as exc:
            result.append(exc)

    thread = threading.Thread(target=publish)
    thread.start()
    assert at_g2.wait(2)
    started = time.monotonic()
    engine.shutdown()
    assert time.monotonic() - started < 0.5
    continue_g2.set()
    thread.join(2)
    assert not thread.is_alive()
    assert len(result) == 1 and isinstance(result[0], pb.PublicationCancelled)
    assert _pointer_bytes(snapshot) is None
    assert _generation_names(snapshot) == []
    exit_worker.set()


def test_n1_different_root_suspension_at_g2_blocks_old_root_publish(
    tmp_path, monkeypatch
):
    original_run = pb.run_periodic_backup
    entered, exit_worker, _calls = _blocking_worker(monkeypatch, ignore_cancel=True)
    config, home, _root = _config(tmp_path)
    owner = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    snapshot = _snapshot(owner)
    at_g2 = threading.Event()
    continue_g2 = threading.Event()
    result: list[BaseException] = []

    def barrier(stage, _snapshot_value):
        if stage == "after_final_validation_before_commit_claim":
            at_g2.set()
            assert continue_g2.wait(2)

    def publish():
        try:
            _publish(original_run, snapshot, hook=barrier)
        except BaseException as exc:
            result.append(exc)

    thread = threading.Thread(target=publish)
    thread.start()
    assert at_g2.wait(2)
    other_home = _private_dir(tmp_path / "other-home-g2")
    _private_dir(other_home / "lcm-large-outputs")
    conflicting = LCMEngine(config=config, hermes_home=str(other_home))
    assert conflicting._periodic_backup_registration.status == "suspended"
    assert conflicting._periodic_backup_lease_handle is None
    continue_g2.set()
    thread.join(2)
    assert not thread.is_alive()
    assert len(result) == 1 and isinstance(result[0], pb.PublicationCancelled)
    assert _pointer_bytes(snapshot) is None
    assert _generation_names(snapshot) == []
    conflicting.shutdown()
    owner.shutdown()
    exit_worker.set()


def test_g1_blocked_final_validation_is_source_local_and_other_source_publishes(
    tmp_path, monkeypatch
):
    original_run = pb.run_periodic_backup
    entered, exit_worker, calls = _blocking_worker(monkeypatch, ignore_cancel=True)
    config_a, home_a, _root_a = _config(tmp_path / "a")
    config_b, home_b, _root_b = _config(tmp_path / "b")
    engine_a = LCMEngine(config=config_a, hermes_home=str(home_a))
    engine_b = LCMEngine(config=config_b, hermes_home=str(home_b))
    deadline = time.monotonic() + 2
    while len(calls) < 2 and time.monotonic() < deadline:
        threading.Event().wait(0.01)
    assert len(calls) == 2
    snapshot_a = _snapshot(engine_a)
    snapshot_b = _snapshot(engine_b)
    blocked = threading.Event()
    finish = threading.Event()
    result_a: list[BaseException] = []

    def barrier(stage, _snapshot_value):
        if stage == "before_final_validation":
            blocked.set()
            assert finish.wait(2)

    def publish_a():
        try:
            _publish(original_run, snapshot_a, hook=barrier)
        except BaseException as exc:
            result_a.append(exc)

    thread = threading.Thread(target=publish_a)
    thread.start()
    assert blocked.wait(2)
    started = time.monotonic()
    engine_a.shutdown()
    assert time.monotonic() - started < 0.5
    pointer_b = _publish(original_run, snapshot_b)
    assert pb.read_verified_pointer(snapshot_b) == pointer_b
    finish.set()
    thread.join(2)
    assert not thread.is_alive()
    assert len(result_a) == 1 and isinstance(result_a[0], pb.PublicationCancelled)
    assert _pointer_bytes(snapshot_a) is None
    engine_b.shutdown()
    exit_worker.set()


def test_g2_claim_first_may_finish_after_responsive_release(tmp_path, monkeypatch):
    original_run = pb.run_periodic_backup
    entered, exit_worker, _calls = _blocking_worker(monkeypatch, ignore_cancel=True)
    config, home, _root = _config(tmp_path)
    engine = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    snapshot = _snapshot(engine)
    claimed = threading.Event()
    finish = threading.Event()
    result: list[object] = []

    def barrier(stage, _snapshot_value):
        if stage == "after_commit_claim":
            claimed.set()
            assert finish.wait(2)

    thread = threading.Thread(
        target=lambda: result.append(_publish(original_run, snapshot, hook=barrier))
    )
    thread.start()
    assert claimed.wait(2)
    started = time.monotonic()
    engine.shutdown()
    assert time.monotonic() - started < 0.5
    finish.set()
    thread.join(2)
    assert not thread.is_alive()
    assert result and pb.read_verified_pointer(snapshot) == result[0]
    exit_worker.set()


def test_g3_retention_never_blocks_release_and_preserves_current(tmp_path, monkeypatch):
    original_run = pb.run_periodic_backup
    entered, exit_worker, _calls = _blocking_worker(monkeypatch, ignore_cancel=True)
    config, home, _root = _config(tmp_path)
    engine = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    snapshot = _snapshot(engine)
    retention = threading.Event()
    finish = threading.Event()

    def barrier(stage, _snapshot_value):
        if stage == "after_pointer_durable_before_retention":
            retention.set()
            assert finish.wait(2)

    thread = threading.Thread(target=lambda: _publish(original_run, snapshot, hook=barrier))
    thread.start()
    assert retention.wait(2)
    raw_before = _pointer_bytes(snapshot)
    started = time.monotonic()
    engine.shutdown()
    assert time.monotonic() - started < 0.5
    assert _pointer_bytes(snapshot) == raw_before
    finish.set()
    thread.join(2)
    assert not thread.is_alive()
    pointer = pb.read_verified_pointer(snapshot)
    assert pointer and pointer["generation"] in _generation_names(snapshot)
    exit_worker.set()


@pytest.mark.parametrize("kind", ["dangling_symlink", "directory", "fifo", "hardlink", "foreign_json"])
def test_f1_f2_unsafe_pointer_is_preserved_and_fails_closed(
    tmp_path, monkeypatch, kind
):
    original_run = pb.run_periodic_backup
    entered, _exit, _calls = _blocking_worker(monkeypatch)
    config, home, _root = _config(tmp_path)
    engine = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    snapshot = _snapshot(engine)
    _publish(original_run, snapshot)
    pointer = _namespace(snapshot) / "latest-good.json"
    pointer.unlink()
    if kind == "dangling_symlink":
        pointer.symlink_to("missing")
    elif kind == "directory":
        pointer.mkdir()
    elif kind == "fifo":
        os.mkfifo(pointer, 0o600)
    elif kind == "hardlink":
        other = _namespace(snapshot) / "other-pointer"
        other.write_text("{}")
        other.chmod(0o600)
        os.link(other, pointer)
    else:
        pointer.write_text('{"foreign":true}')
        pointer.chmod(0o600)
    observed = pointer.lstat()

    with pytest.raises(pb.UnsafeExistingPointer):
        pb.read_verified_pointer(snapshot)
    with pytest.raises(pb.UnsafeExistingPointer):
        _publish(original_run, _snapshot(engine))
    assert pointer.lstat().st_ino == observed.st_ino
    engine.shutdown()


def test_f2_lstat_to_open_replacement_is_rejected_and_preserved(tmp_path, monkeypatch):
    original_run = pb.run_periodic_backup
    entered, _exit, _calls = _blocking_worker(monkeypatch)
    config, home, _root = _config(tmp_path)
    engine = LCMEngine(config=config, hermes_home=str(home))
    assert entered.wait(2)
    snapshot = _snapshot(engine)
    _publish(original_run, snapshot)
    pointer = _namespace(snapshot) / "latest-good.json"
    replacement = _namespace(snapshot) / "replacement"
    replacement.write_bytes(pointer.read_bytes())
    replacement.chmod(0o600)
    real_open = pb.os.open
    replaced = {"done": False}

    def racing_open(path, flags, *args, **kwargs):
        if path == "latest-good.json" and not replaced["done"]:
            replaced["done"] = True
            os.replace(replacement, pointer)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(pb.os, "open", racing_open)
    with pytest.raises(pb.UnsafeExistingPointer, match="mismatch"):
        pb.read_verified_pointer(snapshot)
    assert pointer.exists()
    engine.shutdown()
