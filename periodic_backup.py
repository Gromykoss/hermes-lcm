"""Default-off, process-local publication of verified LCM backup generations.

This module is deliberately not a restore API.  Its registry and leases are
process-local scheduling authority; the destination is private implementation
data and is safe only on a local filesystem owned by the current user.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import sqlite3
import stat
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, cast
from urllib.parse import quote

from .externalize import DEFAULT_LARGE_OUTPUT_DIRNAME
from .ingest_protection import extract_all_externalized_payload_refs

_POINTER = "latest-good.json"
_IDENTITY = "source.json"
_NAMESPACE = "lcm-periodic-backups"
_MAX_METADATA_BYTES = 1024 * 1024
_MAX_PAYLOAD_BYTES = 256 * 1024 * 1024
_GENERATION_PREFIX = "generation-"
_STAGE_PREFIX = ".stage-"
_MAX_CANCELLATION_WAIT_SECONDS = 24 * 60 * 60


class BackupError(RuntimeError):
    """Base class for typed optional-backup failures."""


class BackupConfigurationError(BackupError):
    def __init__(self, reason: str, *, source_key: str = "", root_path: str = ""):
        super().__init__(reason)
        self.reason = reason
        self.source_key = source_key
        self.root_path = root_path


class UnsafeExistingPointer(BackupError):
    pass


class RootContinuityError(BackupError):
    pass


class PublicationCancelled(BackupError):
    pass


@dataclass(frozen=True)
class Registration:
    status: Literal["disabled", "active", "pending", "suspended", "error"]
    reason: str = ""


@dataclass(frozen=True)
class LeaseHandle:
    """Opaque exact lease authority.  Presentations are never releasable."""

    source_key: str
    lease_id: str


@dataclass(frozen=True)
class BackupCandidate:
    source_key: str
    database_path: Path
    root_path: Path
    root_observation: tuple[int, int]
    destination: Path
    interval_seconds: float
    keep_last: int

    @property
    def policy(self) -> tuple[str, float, int]:
        return (str(self.destination), self.interval_seconds, self.keep_last)


@dataclass(frozen=True)
class BackupSnapshot:
    source_key: str
    database_path: Path
    root_path: Path
    root_observation: tuple[int, int]
    destination: Path
    interval_seconds: float
    keep_last: int
    epoch: int
    worker_token: str
    generation_id: str
    commit_claim: Callable[["BackupSnapshot"], bool]
    commit_gate: threading.Lock = field(compare=False, repr=False)


@dataclass
class _BackupSource:
    candidate: BackupCandidate
    state: str = "ACTIVE"
    root_continuity: str = "continuous"
    epoch: int = 0
    leases: dict[str, LeaseHandle] = field(default_factory=dict)
    worker: threading.Thread | None = None
    worker_token: str = ""
    cancel: threading.Event = field(default_factory=threading.Event)
    watcher: threading.Thread | None = None
    commit_gate: threading.Lock = field(default_factory=threading.Lock)
    commit_claim: tuple[int, str, str] | None = None
    last_error: str = ""


_REGISTRY_LOCK = threading.RLock()
_SOURCES: dict[str, _BackupSource] = {}


def _strict_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise BackupConfigurationError("invalid_enabled")


def _strict_positive_float(value: Any) -> float:
    if isinstance(value, bool):
        raise BackupConfigurationError("invalid_interval")
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise BackupConfigurationError("invalid_interval") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise BackupConfigurationError("invalid_interval")
    return parsed


def _strict_positive_int(value: Any) -> int:
    if isinstance(value, bool):
        raise BackupConfigurationError("invalid_keep_last")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.strip() and value.strip().lstrip("+").isdigit():
        parsed = int(value)
    else:
        raise BackupConfigurationError("invalid_keep_last")
    if parsed <= 0:
        raise BackupConfigurationError("invalid_keep_last")
    return parsed


def _assert_private_directory(path: Path, *, reason: str) -> os.stat_result:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise BackupConfigurationError(reason) from exc
    if not stat.S_ISDIR(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
        raise BackupConfigurationError(reason)
    if hasattr(os, "getuid") and observed.st_uid != os.getuid():
        raise BackupConfigurationError(reason)
    if observed.st_mode & 0o077:
        raise BackupConfigurationError(reason)
    return observed


def _assert_no_symlink_components(path: Path, *, reason: str) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        try:
            observed = current.lstat()
        except FileNotFoundError:
            raise BackupConfigurationError(reason)
        except OSError as exc:
            raise BackupConfigurationError(reason) from exc
        if stat.S_ISLNK(observed.st_mode):
            raise BackupConfigurationError(reason)


def _configured_root(engine: Any) -> Path:
    configured = getattr(engine._config, "large_output_externalization_path", "")
    if configured:
        return Path(configured).expanduser().absolute()
    home = Path(engine._hermes_home).expanduser().absolute() if engine._hermes_home else Path("~/.hermes").expanduser().absolute()
    return home / DEFAULT_LARGE_OUTPUT_DIRNAME


def build_backup_candidate(engine: Any) -> BackupCandidate | Registration:
    """Validate and canonicalize configuration without creating anything."""

    config = engine._config
    enabled = _strict_bool(getattr(config, "periodic_backup_enabled", False))
    if not enabled:
        return Registration("disabled", "disabled_by_configuration")

    interval = _strict_positive_float(
        getattr(config, "periodic_backup_interval_seconds", 3600.0)
    )
    keep_last = _strict_positive_int(getattr(config, "periodic_backup_keep_last", 3))

    raw_destination = getattr(config, "periodic_backup_destination", "")
    if not isinstance(raw_destination, (str, os.PathLike)) or not str(raw_destination).strip():
        raise BackupConfigurationError("invalid_destination")
    destination_input = Path(raw_destination).expanduser().absolute()
    _assert_no_symlink_components(destination_input, reason="unsafe_destination")
    destination = destination_input.resolve(strict=True)
    _assert_private_directory(destination, reason="unsafe_destination")

    raw_db = Path(getattr(engine._store, "db_path", ""))
    try:
        database_path = raw_db.expanduser().resolve(strict=True)
        db_stat = database_path.stat()
    except OSError as exc:
        raise BackupConfigurationError("invalid_database") from exc
    if not stat.S_ISREG(db_stat.st_mode):
        raise BackupConfigurationError("invalid_database")
    source_key = str(database_path)

    root_input = _configured_root(engine)
    try:
        _assert_no_symlink_components(root_input, reason="unsafe_root")
        root_path = root_input.resolve(strict=True)
        root_stat = _assert_private_directory(root_path, reason="unsafe_root")
    except BackupConfigurationError as exc:
        exc.source_key = source_key
        exc.root_path = str(root_input)
        raise

    return BackupCandidate(
        source_key=source_key,
        database_path=database_path,
        root_path=root_path,
        root_observation=(root_stat.st_dev, root_stat.st_ino),
        destination=destination,
        interval_seconds=interval,
        keep_last=keep_last,
    )


def _registration_for(source: _BackupSource) -> Registration:
    status = source.state.lower()
    if status == "stopping":
        status = "pending"
    if status not in {"active", "pending", "suspended", "error"}:
        status = "error"
    return Registration(
        cast(Literal["disabled", "active", "pending", "suspended", "error"], status),
        source.last_error,
    )


def _snapshot_locked(source: _BackupSource) -> BackupSnapshot:
    candidate = source.candidate
    return BackupSnapshot(
        source_key=candidate.source_key,
        database_path=candidate.database_path,
        root_path=candidate.root_path,
        root_observation=candidate.root_observation,
        destination=candidate.destination,
        interval_seconds=candidate.interval_seconds,
        keep_last=candidate.keep_last,
        epoch=source.epoch,
        worker_token=source.worker_token,
        generation_id=uuid.uuid4().hex,
        commit_claim=_commit_claim,
        commit_gate=source.commit_gate,
    )


def _commit_claim(snapshot: BackupSnapshot) -> bool:
    """G2 linearization point; only short registry state is touched here."""

    with _REGISTRY_LOCK:
        source = _SOURCES.get(snapshot.source_key)
        if (
            source is None
            or source.state != "ACTIVE"
            or source.root_continuity != "continuous"
            or source.epoch != snapshot.epoch
            or source.worker_token != snapshot.worker_token
            or not source.leases
            or source.cancel.is_set()
        ):
            return False
        source.commit_claim = (
            snapshot.epoch,
            snapshot.worker_token,
            snapshot.generation_id,
        )
        return True


def _start_worker_locked(source: _BackupSource, *, successor: bool = False) -> bool:
    source.cancel = threading.Event()
    source.worker_token = uuid.uuid4().hex
    source.state = "ACTIVE"
    source_key = source.candidate.source_key
    worker_token = source.worker_token

    def target() -> None:
        try:
            _worker_main(source_key, worker_token)
        finally:
            _worker_finished(source_key, worker_token, threading.current_thread())

    thread = threading.Thread(
        target=target,
        name=("lcm-periodic-backup-successor-" if successor else "lcm-periodic-backup-")
        + hashlib.sha256(source.candidate.source_key.encode()).hexdigest()[:10],
        daemon=True,
    )
    source.worker = thread
    try:
        thread.start()
    except Exception as exc:
        source.worker = None
        source.state = "ERROR"
        source.last_error = f"worker_start_failed:{type(exc).__name__}"
        source.epoch += 1
        source.cancel.set()
        return False
    return True


def _worker_finished(
    source_key: str,
    worker_token: str,
    worker: threading.Thread,
) -> None:
    """Leave ownership intact until an external observer can join this worker.

    A thread is still alive while its target's ``finally`` block runs.  Clearing
    the slot here would let a concurrent acquisition start a same-source worker
    before this one has actually exited.
    """


def _finalize_joined_worker_locked(
    source_key: str,
    source: _BackupSource,
    worker: threading.Thread,
) -> bool:
    """Finalize a worker that an external observer already joined."""

    if source.worker is not worker:
        return False
    source.worker = None
    if source.watcher is not None and not source.watcher.is_alive():
        source.watcher = None
    if not source.leases:
        _SOURCES.pop(source_key, None)
        return True
    if source.state == "STOPPING":
        _start_worker_locked(source, successor=True)
    return False


def _join_exited_worker(source_key: str) -> None:
    """Observe and join an exited worker without holding the registry lock."""

    with _REGISTRY_LOCK:
        source = _SOURCES.get(source_key)
        worker = source.worker if source is not None else None
        if worker is None or worker.is_alive():
            return
    worker.join()
    with _REGISTRY_LOCK:
        source = _SOURCES.get(source_key)
        if source is not None:
            _finalize_joined_worker_locked(source_key, source, worker)


def _start_watcher_locked(source: _BackupSource, old_worker: threading.Thread) -> bool:
    if source.watcher is not None and source.watcher.is_alive():
        return True
    source_key = source.candidate.source_key
    watcher = threading.Thread(
        target=_watch_worker_exit,
        args=(source_key, old_worker),
        name="lcm-periodic-backup-watcher-"
        + hashlib.sha256(source_key.encode()).hexdigest()[:10],
        daemon=True,
    )
    source.watcher = watcher
    try:
        watcher.start()
    except Exception as exc:
        source.watcher = None
        source.state = "ERROR"
        source.last_error = f"watcher_start_failed:{type(exc).__name__}"
        source.epoch += 1
        return False
    return True


def _watch_worker_exit(source_key: str, old_worker: threading.Thread) -> None:
    old_worker.join()
    with _REGISTRY_LOCK:
        source = _SOURCES.get(source_key)
        if source is None or source.worker is not old_worker:
            return
        source.worker = None
        source.watcher = None
        if source.state == "STOPPING" and source.leases:
            _start_worker_locked(source, successor=True)
        elif source.leases:
            # SUSPENDED and ERROR are terminal until exact leases drain.
            return
        else:
            _SOURCES.pop(source_key, None)


def _suspend_locked(source: _BackupSource, reason: str) -> None:
    if source.state == "SUSPENDED":
        return
    source.root_continuity = "broken"
    source.state = "SUSPENDED"
    source.last_error = reason
    source.epoch += 1
    source.cancel.set()


def acquire_backup_lease(engine: Any) -> tuple[Registration, LeaseHandle | None]:
    """Acquire one exact lease and return presentation and authority separately."""

    try:
        built = build_backup_candidate(engine)
    except BackupConfigurationError as exc:
        if exc.source_key:
            with _REGISTRY_LOCK:
                source = _SOURCES.get(exc.source_key)
                if source is not None:
                    _suspend_locked(source, exc.reason)
                    return _registration_for(source), None
        return Registration("error", exc.reason), None

    if isinstance(built, Registration):
        return built, None

    candidate = built
    _join_exited_worker(candidate.source_key)
    with _REGISTRY_LOCK:
        source = _SOURCES.get(candidate.source_key)
        if source is not None:
            if source.candidate.policy != candidate.policy:
                return Registration("error", "policy_conflict"), None
            if source.root_continuity != "continuous" or source.state == "SUSPENDED":
                return _registration_for(source), None
            if (
                source.candidate.root_path != candidate.root_path
                or source.candidate.root_observation != candidate.root_observation
            ):
                _suspend_locked(source, "root_discontinuity")
                return _registration_for(source), None
            if (
                source.state == "ERROR"
                and not source.leases
                and source.worker is not None
                and source.worker.is_alive()
            ):
                # In particular this contains watcher-start failure: the old
                # source retains its slot until a later external observer sees
                # and joins the actual thread exit.
                return _registration_for(source), None
            lease = LeaseHandle(candidate.source_key, uuid.uuid4().hex)
            source.leases[lease.lease_id] = lease
            if source.state == "ACTIVE":
                return Registration("active"), lease
            if source.state == "STOPPING":
                return Registration("pending", "waiting_for_prior_worker_exit"), lease
            return _registration_for(source), lease

        lease = LeaseHandle(candidate.source_key, uuid.uuid4().hex)
        source = _BackupSource(candidate=candidate, leases={lease.lease_id: lease})
        _SOURCES[candidate.source_key] = source
        if not _start_worker_locked(source):
            return _registration_for(source), lease
        return Registration("active"), lease


def release_backup_lease(handle: LeaseHandle | None) -> None:
    """Release only the exact opaque handle; duplicate release is a no-op."""

    if not isinstance(handle, LeaseHandle):
        return
    with _REGISTRY_LOCK:
        source = _SOURCES.get(handle.source_key)
        if source is None or source.leases.pop(handle.lease_id, None) is None:
            return
        if source.leases:
            return
        source.epoch += 1
        source.cancel.set()
        worker = source.worker
        if worker is not None and worker.is_alive():
            if source.state == "ACTIVE":
                source.state = "STOPPING"
            _start_watcher_locked(source, worker)
        else:
            _SOURCES.pop(handle.source_key, None)


def periodic_backup_source_status(source_key: str) -> dict[str, Any] | None:
    """Return a read-only diagnostic projection without exposing authority."""

    _join_exited_worker(source_key)
    with _REGISTRY_LOCK:
        source = _SOURCES.get(source_key)
        if source is None:
            return None
        return {
            "source_key": source.candidate.source_key,
            "state": source.state,
            "epoch": source.epoch,
            "root_path": str(source.candidate.root_path),
            "root_observation": source.candidate.root_observation,
            "root_continuity": source.root_continuity,
            "lease_count": len(source.leases),
            "worker_alive": bool(source.worker and source.worker.is_alive()),
            "watcher_alive": bool(source.watcher and source.watcher.is_alive()),
            "commit_claim": source.commit_claim,
            "last_error": source.last_error,
        }


def _worker_main(source_key: str, worker_token: str) -> None:
    while True:
        with _REGISTRY_LOCK:
            source = _SOURCES.get(source_key)
            if (
                source is None
                or source.worker_token != worker_token
                or source.state != "ACTIVE"
                or source.cancel.is_set()
                or not source.leases
            ):
                return
            snapshot = _snapshot_locked(source)
            cancel = source.cancel
        try:
            run_periodic_backup(snapshot, cancel=cancel)
        except PublicationCancelled:
            pass
        except (RootContinuityError, UnsafeExistingPointer) as exc:
            with _REGISTRY_LOCK:
                source = _SOURCES.get(source_key)
                if source is not None and source.worker_token == worker_token:
                    _suspend_locked(source, f"{type(exc).__name__}:{exc}")
            return
        except Exception as exc:
            with _REGISTRY_LOCK:
                source = _SOURCES.get(source_key)
                if source is not None and source.worker_token == worker_token:
                    source.state = "ERROR"
                    source.last_error = f"backup_failed:{type(exc).__name__}:{exc}"
                    source.epoch += 1
                    source.cancel.set()
            return
        if _wait_for_cancellation(cancel, snapshot.interval_seconds):
            return


def _wait_for_cancellation(cancel: threading.Event, interval_seconds: float) -> bool:
    """Wait monotonically in platform-safe chunks for any accepted interval."""

    remaining = interval_seconds
    while remaining > 0:
        chunk = min(remaining, _MAX_CANCELLATION_WAIT_SECONDS, threading.TIMEOUT_MAX)
        started = time.monotonic()
        if cancel.wait(chunk):
            return True
        elapsed = time.monotonic() - started
        if elapsed <= 0:
            # Event.wait() is not specified to return spuriously, but keep a
            # mocked or unusual implementation from creating a hot loop.
            elapsed = chunk
        remaining -= elapsed
    return cancel.is_set()


def _call_fault(fault_hook: Callable[..., Any] | None, stage: str, snapshot: BackupSnapshot) -> None:
    if fault_hook is not None:
        fault_hook(stage, snapshot)


def _observe_root(snapshot: BackupSnapshot) -> None:
    try:
        observed = snapshot.root_path.lstat()
    except OSError as exc:
        raise RootContinuityError("root_missing_or_inaccessible") from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or (observed.st_dev, observed.st_ino) != snapshot.root_observation
        or (hasattr(os, "getuid") and observed.st_uid != os.getuid())
        or observed.st_mode & 0o077
    ):
        raise RootContinuityError("root_discontinuity")


def _source_namespace(snapshot: BackupSnapshot, *, create: bool) -> Path:
    parent = snapshot.destination / _NAMESPACE
    source_hash = hashlib.sha256(snapshot.source_key.encode("utf-8")).hexdigest()
    namespace = parent / source_hash
    if create:
        for directory in (parent, namespace):
            if not directory.exists():
                directory.mkdir(mode=0o700)
                _fsync_dir(directory.parent)
            _assert_private_directory(directory, reason="unsafe_destination_namespace")
        identity_path = namespace / _IDENTITY
        expected = {"schema": 1, "source_key": snapshot.source_key}
        if identity_path.exists():
            identity = _read_private_json(namespace, _IDENTITY)
            if identity != expected:
                raise BackupError("foreign_namespace_identity")
        else:
            _write_new_private_json(namespace, _IDENTITY, expected)
            _fsync_dir(namespace)
    elif namespace.exists():
        _assert_private_directory(namespace, reason="unsafe_destination_namespace")
        if _read_private_json(namespace, _IDENTITY) != {
            "schema": 1,
            "source_key": snapshot.source_key,
        }:
            raise BackupError("foreign_namespace_identity")
    return namespace


def _strict_json_bytes(data: bytes) -> Any:
    if len(data) > _MAX_METADATA_BYTES:
        raise BackupError("metadata_too_large")
    duplicates: list[str] = []

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                duplicates.append(key)
            result[key] = value
        return result

    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackupError("invalid_json") from exc
    if duplicates:
        raise BackupError("duplicate_json_key")
    return value


def _open_private_regular_at(directory: Path, name: str) -> tuple[int, os.stat_result, os.stat_result]:
    dir_fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
    try:
        before = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise BackupError("unsafe_entry_type")
        if hasattr(os, "getuid") and before.st_uid != os.getuid():
            raise BackupError("unsafe_entry_owner")
        if before.st_mode & 0o077:
            raise BackupError("unsafe_entry_permissions")
        fd = os.open(name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=dir_fd)
        after = os.fstat(fd)
        if (
            (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
            or not stat.S_ISREG(after.st_mode)
            or after.st_nlink != 1
        ):
            os.close(fd)
            raise BackupError("entry_race")
        return fd, before, after
    finally:
        os.close(dir_fd)


def _read_private_bytes(directory: Path, name: str, *, limit: int = _MAX_METADATA_BYTES) -> bytes:
    fd, before, _after = _open_private_regular_at(directory, name)
    try:
        if before.st_size > limit:
            raise BackupError("entry_too_large")
        data = b""
        while len(data) <= limit:
            chunk = os.read(fd, min(65536, limit + 1 - len(data)))
            if not chunk:
                break
            data += chunk
        if len(data) > limit:
            raise BackupError("entry_too_large")
        final = os.fstat(fd)
        if (final.st_dev, final.st_ino, final.st_size) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
        ):
            raise BackupError("entry_changed_while_reading")
        return data
    finally:
        os.close(fd)


def _read_private_json(directory: Path, name: str) -> Any:
    return _strict_json_bytes(_read_private_bytes(directory, name))


def _pointer_entry(namespace: Path) -> tuple[str, bytes | None]:
    dir_fd = os.open(namespace, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
    try:
        try:
            before = os.stat(_POINTER, dir_fd=dir_fd, follow_symlinks=False)
        except FileNotFoundError:
            return "ABSENT", None
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or (hasattr(os, "getuid") and before.st_uid != os.getuid())
            or before.st_mode & 0o077
            or before.st_size > _MAX_METADATA_BYTES
        ):
            raise UnsafeExistingPointer("unsafe_pointer_metadata")
        try:
            fd = os.open(_POINTER, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=dir_fd)
        except OSError as exc:
            raise UnsafeExistingPointer("pointer_open_failed") from exc
        try:
            opened = os.fstat(fd)
            if (
                (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
                or not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or (hasattr(os, "getuid") and opened.st_uid != os.getuid())
                or opened.st_mode & 0o077
                or opened.st_size != before.st_size
            ):
                raise UnsafeExistingPointer("pointer_lstat_open_mismatch")
            data = b""
            while len(data) <= _MAX_METADATA_BYTES:
                part = os.read(fd, 65536)
                if not part:
                    break
                data += part
            if len(data) > _MAX_METADATA_BYTES:
                raise UnsafeExistingPointer("pointer_too_large")
            final = os.fstat(fd)
            rebound = os.stat(_POINTER, dir_fd=dir_fd, follow_symlinks=False)
            if (
                (final.st_dev, final.st_ino, final.st_size)
                != (before.st_dev, before.st_ino, before.st_size)
                or (rebound.st_dev, rebound.st_ino) != (before.st_dev, before.st_ino)
                or not stat.S_ISREG(final.st_mode)
                or not stat.S_ISREG(rebound.st_mode)
                or final.st_nlink != 1
                or rebound.st_nlink != 1
                or (hasattr(os, "getuid") and final.st_uid != os.getuid())
                or (hasattr(os, "getuid") and rebound.st_uid != os.getuid())
                or final.st_mode & 0o077
                or rebound.st_mode & 0o077
                or rebound.st_size != before.st_size
            ):
                raise UnsafeExistingPointer("pointer_changed_while_reading")
            return "VALID", data
        finally:
            os.close(fd)
    finally:
        os.close(dir_fd)


def read_verified_pointer(snapshot: BackupSnapshot) -> dict[str, Any] | None:
    try:
        namespace = _source_namespace(snapshot, create=False)
        if not namespace.exists():
            return None
        state, raw = _pointer_entry(namespace)
        if state == "ABSENT":
            return None
        pointer = _strict_json_bytes(raw or b"")
        if not isinstance(pointer, dict) or set(pointer) != {
            "schema", "source_key", "generation", "manifest_sha256"
        }:
            raise BackupError("invalid_pointer_shape")
        generation = pointer.get("generation")
        if (
            pointer.get("schema") != 1
            or pointer.get("source_key") != snapshot.source_key
            or not isinstance(generation, str)
            or not generation.startswith(_GENERATION_PREFIX)
            or len(generation) != len(_GENERATION_PREFIX) + 32
            or any(ch not in "0123456789abcdef" for ch in generation[len(_GENERATION_PREFIX):])
            or not isinstance(pointer.get("manifest_sha256"), str)
        ):
            raise BackupError("foreign_pointer")
        generation_dir = namespace / generation
        _assert_private_directory(
            generation_dir,
            reason="unsafe_generation_directory",
        )
        manifest_bytes = _read_private_bytes(generation_dir, "manifest.json")
        if hashlib.sha256(manifest_bytes).hexdigest() != pointer["manifest_sha256"]:
            raise BackupError("pointer_manifest_mismatch")
        manifest = _strict_json_bytes(manifest_bytes)
        if (
            not isinstance(manifest, dict)
            or manifest.get("schema") != 1
            or manifest.get("source_key") != snapshot.source_key
            or manifest.get("generation") != generation
        ):
            raise BackupError("manifest_identity_mismatch")
        _verify_generation(generation_dir, manifest)
        return pointer
    except UnsafeExistingPointer:
        raise
    except (BackupError, OSError) as exc:
        raise UnsafeExistingPointer(str(exc)) from exc


def _sqlite_snapshot(source: Path, destination: Path) -> None:
    uri = f"file:{quote(str(source))}?mode=ro"
    src = sqlite3.connect(uri, uri=True)
    dst = sqlite3.connect(destination)
    try:
        src.backup(dst)
        dst.execute("PRAGMA query_only=ON")
        if dst.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise BackupError("snapshot_integrity_failed")
    finally:
        dst.close()
        src.close()
    os.chmod(destination, 0o600)
    _fsync_file(destination)


def _snapshot_payload_refs(database: Path) -> list[str]:
    refs: set[str] = set()
    # This always reads the already-closed staged snapshot.  immutable=1 keeps
    # SQLite from creating WAL/SHM companions inside the generation being
    # byte-accounted.
    uri = f"file:{quote(str(database))}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    try:
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )]
        for table in tables:
            if not isinstance(table, str) or not table.replace("_", "").isalnum():
                continue
            text_columns = [
                row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')
                if str(row[2] or "").upper().startswith(("TEXT", "VARCHAR", "CHAR", "JSON"))
            ]
            if not text_columns:
                continue
            selected = ",".join(f'"{column}"' for column in text_columns)
            for row in conn.execute(f'SELECT {selected} FROM "{table}"'):
                for value in row:
                    if isinstance(value, str):
                        refs.update(extract_all_externalized_payload_refs(value))
    finally:
        conn.close()
    return sorted(refs)


def _copy_payload(root_fd: int, root: Path, ref: str, target: Path) -> str:
    if not ref or Path(ref).name != ref or "/" in ref or "\\" in ref:
        raise BackupError("invalid_payload_ref")
    before = os.stat(ref, dir_fd=root_fd, follow_symlinks=False)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or (hasattr(os, "getuid") and before.st_uid != os.getuid())
        or before.st_mode & 0o077
        or before.st_size > _MAX_PAYLOAD_BYTES
    ):
        raise BackupError("unsafe_payload")
    fd = os.open(ref, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=root_fd)
    try:
        opened = os.fstat(fd)
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise BackupError("payload_race")
        digest = hashlib.sha256()
        out_fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            while True:
                chunk = os.read(fd, 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                _write_all(out_fd, chunk)
            os.fsync(out_fd)
        finally:
            os.close(out_fd)
        final = os.fstat(fd)
        rebound = os.stat(ref, dir_fd=root_fd, follow_symlinks=False)
        if (
            (final.st_dev, final.st_ino, final.st_size)
            != (before.st_dev, before.st_ino, before.st_size)
            or (rebound.st_dev, rebound.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise BackupError("payload_changed_while_copying")
        # Production recovery parser recognizes this file only through the
        # placeholder ref.  Strict JSON proves the copied recovery object is
        # structurally readable rather than merely hashable bytes.
        payload = _strict_json_bytes(target.read_bytes())
        if not isinstance(payload, dict):
            raise BackupError("invalid_payload_json")
        return digest.hexdigest()
    finally:
        os.close(fd)


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
    finally:
        os.close(fd)
    return digest.hexdigest(), size


def _hash_private_entry(directory: Path, name: str) -> tuple[str, int]:
    fd, _before, _after = _open_private_regular_at(directory, name)
    digest = hashlib.sha256()
    size = 0
    try:
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
        if os.fstat(fd).st_size != size:
            raise BackupError("generation_entry_changed")
    finally:
        os.close(fd)
    return digest.hexdigest(), size


def _private_entry_identity(directory: Path, name: str) -> tuple[int, ...]:
    """Return a stable private-entry identity without trusting path metadata alone."""

    fd, before, opened = _open_private_regular_at(directory, name)
    try:
        final = os.fstat(fd)
        if (
            (before.st_dev, before.st_ino, before.st_size)
            != (opened.st_dev, opened.st_ino, opened.st_size)
            or (final.st_dev, final.st_ino, final.st_size)
            != (opened.st_dev, opened.st_ino, opened.st_size)
            or not stat.S_ISREG(final.st_mode)
            or final.st_nlink != 1
            or (hasattr(os, "getuid") and final.st_uid != os.getuid())
            or final.st_mode & 0o077
        ):
            raise BackupError("generation_entry_identity_changed")
        return (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mode,
            final.st_uid,
            final.st_nlink,
            final.st_mtime_ns,
            final.st_ctime_ns,
        )
    finally:
        os.close(fd)


def _verify_generation(directory: Path, manifest: dict[str, Any]) -> None:
    _assert_private_directory(directory, reason="unsafe_generation_directory")
    files = manifest.get("files")
    if not isinstance(files, dict) or "lcm.db" not in files:
        raise BackupError("invalid_manifest_files")
    expected_names = {"manifest.json", *files.keys()}
    actual_names = {entry.name for entry in directory.iterdir()}
    if actual_names != expected_names:
        raise BackupError("generation_file_set_mismatch")
    verified: dict[str, tuple[str, int, tuple[int, ...]]] = {}
    for name, metadata in files.items():
        if Path(name).name != name or not isinstance(metadata, dict):
            raise BackupError("invalid_manifest_entry")
        file_hash, size = _hash_private_entry(directory, name)
        if metadata != {"sha256": file_hash, "size": size}:
            raise BackupError("generation_byte_mismatch")
        verified[name] = (file_hash, size, _private_entry_identity(directory, name))
    uri = f"file:{quote(str(directory / 'lcm.db'))}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    try:
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise BackupError("staged_database_invalid")
        # Re-run the production ref parser on the immutable staged snapshot.
        refs = _snapshot_payload_refs(directory / "lcm.db")
        if sorted(manifest.get("payload_refs", [])) != refs:
            raise BackupError("staged_recovery_refs_changed")
        for ref in refs:
            if ref not in files:
                raise BackupError("staged_payload_missing")
    finally:
        conn.close()
    # Bind every trusted child's identity and bytes through the end of recovery
    # validation.  A same-size mutation after its first hash must not publish.
    for name, expected in verified.items():
        file_hash, size = _hash_private_entry(directory, name)
        identity = _private_entry_identity(directory, name)
        if (file_hash, size, identity) != expected:
            raise BackupError("generation_entry_changed_during_validation")


def _write_new_private_json(directory: Path, name: str, value: Any) -> bytes:
    data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    dir_fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
    try:
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600, dir_fd=dir_fd)
        try:
            _write_all(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        os.close(dir_fd)
    return data


def _publish_pointer(namespace: Path, pointer: dict[str, Any]) -> None:
    data = json.dumps(pointer, sort_keys=True, separators=(",", ":")).encode("utf-8")
    temporary = f".pointer-{uuid.uuid4().hex}"
    dir_fd = os.open(namespace, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600, dir_fd=dir_fd)
        try:
            _write_all(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temporary, _POINTER, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        os.fsync(dir_fd)
    except Exception:
        try:
            os.unlink(temporary, dir_fd=dir_fd)
        except OSError:
            pass
        raise
    finally:
        os.close(dir_fd)


def _retention(snapshot: BackupSnapshot, namespace: Path, current: str) -> None:
    pointer = read_verified_pointer(snapshot)
    if pointer is None or pointer["generation"] != current:
        raise BackupError("pointer_not_durable_for_retention")
    generations: list[Path] = []
    for entry in namespace.iterdir():
        suffix = entry.name.removeprefix(_GENERATION_PREFIX)
        if (
            not entry.name.startswith(_GENERATION_PREFIX)
            or len(suffix) != 32
            or any(ch not in "0123456789abcdef" for ch in suffix)
        ):
            continue
        try:
            _assert_private_directory(entry, reason="unsafe_generation_directory")
            manifest = _read_private_json(entry, "manifest.json")
            if (
                not isinstance(manifest, dict)
                or manifest.get("source_key") != snapshot.source_key
                or manifest.get("generation") != entry.name
            ):
                continue
            _verify_generation(entry, manifest)
        except (BackupError, OSError):
            # Foreign or damaged generations are never adopted or deleted.
            continue
        generations.append(entry)
    generations.sort(key=lambda item: item.stat().st_mtime_ns, reverse=True)
    retained = {item.name for item in generations[: snapshot.keep_last]}
    retained.add(current)
    for generation in generations:
        if generation.name not in retained and generation.name != current:
            shutil.rmtree(generation)
    _fsync_dir(namespace)


def _fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("short write")
        view = view[written:]


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def run_periodic_backup(
    snapshot: BackupSnapshot,
    *,
    cancel: threading.Event,
    fault_hook: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Stage, validate, claim, publish, and retain one immutable generation."""

    _observe_root(snapshot)
    if cancel.is_set():
        raise PublicationCancelled("cancelled_before_staging")
    namespace = _source_namespace(snapshot, create=True)
    # Unsafe is not absence and must be rejected before any generation commit.
    read_verified_pointer(snapshot)
    stage = namespace / f"{_STAGE_PREFIX}{snapshot.generation_id}"
    final = namespace / f"{_GENERATION_PREFIX}{snapshot.generation_id}"
    stage.mkdir(mode=0o700)
    _fsync_dir(namespace)
    claimed = False
    try:
        database = stage / "lcm.db"
        _sqlite_snapshot(snapshot.database_path, database)
        refs = _snapshot_payload_refs(database)
        files: dict[str, dict[str, Any]] = {}
        db_hash, db_size = _hash_file(database)
        files["lcm.db"] = {"sha256": db_hash, "size": db_size}
        root_fd = os.open(snapshot.root_path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
        try:
            for ref in refs:
                target = stage / ref
                digest = _copy_payload(root_fd, snapshot.root_path, ref, target)
                files[ref] = {"sha256": digest, "size": target.stat().st_size}
        finally:
            os.close(root_fd)
        manifest = {
            "schema": 1,
            "source_key": snapshot.source_key,
            "generation": final.name,
            "created_ns": time.time_ns(),
            "payload_refs": refs,
            "files": files,
        }
        manifest_bytes = _write_new_private_json(stage, "manifest.json", manifest)
        _fsync_dir(stage)
        _verify_generation(stage, manifest)
        _call_fault(fault_hook, "after_initial_validation", snapshot)
        _observe_root(snapshot)
        with snapshot.commit_gate:
            _call_fault(fault_hook, "before_final_validation", snapshot)
            _verify_generation(stage, manifest)
            _call_fault(fault_hook, "after_final_validation_before_commit_claim", snapshot)
            _observe_root(snapshot)
            # Keep the final trusted-child observation immediately adjacent to
            # the brief registry-only linearization claim.
            _verify_generation(stage, manifest)
            if not snapshot.commit_claim(snapshot):
                raise PublicationCancelled("commit_claim_rejected")
            claimed = True
            _call_fault(fault_hook, "after_commit_claim", snapshot)
            os.replace(stage, final)
            _fsync_dir(namespace)
            pointer = {
                "schema": 1,
                "source_key": snapshot.source_key,
                "generation": final.name,
                "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            }
            _publish_pointer(namespace, pointer)
        _call_fault(fault_hook, "after_pointer_durable_before_retention", snapshot)
        _retention(snapshot, namespace, final.name)
        return pointer
    finally:
        if stage.exists() and not claimed:
            shutil.rmtree(stage, ignore_errors=True)
            try:
                _fsync_dir(namespace)
            except OSError:
                pass


def _reset_registry_for_tests(timeout: float = 2.0) -> None:
    """Test-only bounded cleanup; production release never waits."""

    with _REGISTRY_LOCK:
        handles = [handle for source in _SOURCES.values() for handle in source.leases.values()]
    for handle in handles:
        release_backup_lease(handle)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with _REGISTRY_LOCK:
            source_keys = list(_SOURCES)
        for source_key in source_keys:
            _join_exited_worker(source_key)
        with _REGISTRY_LOCK:
            source_threads = [
                thread
                for source in _SOURCES.values()
                for thread in (source.worker, source.watcher)
                if thread is not None and thread.is_alive()
            ]
        named_threads = [
            thread
            for thread in threading.enumerate()
            if thread is not threading.current_thread()
            and thread.name.startswith("lcm-periodic-backup")
            and thread.is_alive()
        ]
        threads = list({*source_threads, *named_threads})
        with _REGISTRY_LOCK:
            if not threads and not _SOURCES:
                return
        for thread in threads:
            thread.join(timeout=max(0.0, min(0.05, deadline - time.monotonic())))
    with _REGISTRY_LOCK:
        if _SOURCES:
            raise AssertionError(f"periodic backup registry leak: {periodic_backup_source_status(next(iter(_SOURCES)))}")
