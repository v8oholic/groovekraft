import threading
import traceback
import uuid
import socket
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

from shared.config import GROOVEKRAFT_USER_AGENT, GROOVEKRAFT_VERSION


def run_discogs_import(cfg, callback=print, should_cancel=lambda: False, progress_callback=lambda pct: None, allow_gui_auth=True):
    from discogs import discogs_importer
    client, _, _ = discogs_importer.connect_to_discogs(cfg.db_path, allow_gui_prompt=allow_gui_auth)
    discogs_importer.import_from_discogs(
        discogs_client=client,
        cfg=cfg,
        callback=callback,
        should_cancel=should_cancel,
        progress_callback=progress_callback,
    )


def run_musicbrainz_match(cfg, username, password, match_all=False, callback=print, should_cancel=lambda: False, progress_callback=lambda pct: None):
    import musicbrainzngs
    from musicbrainz import mb_matcher

    if not username or not password:
        raise RuntimeError("MusicBrainz username and password are required.")

    previous_timeout = socket.getdefaulttimeout()
    try:
        socket.setdefaulttimeout(20)
        musicbrainzngs.set_useragent(app=GROOVEKRAFT_USER_AGENT, version=GROOVEKRAFT_VERSION)
        musicbrainzngs.auth(username, password)
        musicbrainzngs.set_rate_limit(1, 1)

        mb_matcher.match_discogs_against_mb(
            cfg.db_path,
            callback=callback,
            should_cancel=should_cancel,
            progress_callback=progress_callback,
            match_all=match_all,
        )
    finally:
        socket.setdefaulttimeout(previous_timeout)


@dataclass
class WorkflowJob:
    id: str
    job_type: str
    title: str
    status: str = "queued"
    progress: int = 0
    logs: deque[str] = field(default_factory=lambda: deque(maxlen=500))
    error: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    _cancel_event: threading.Event = field(default_factory=threading.Event, repr=False)
    _thread: Optional[threading.Thread] = field(default=None, repr=False)

    def append_log(self, message: str):
        self.logs.append(message)
        self.updated_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self):
        return {
            "id": self.id,
            "job_type": self.job_type,
            "title": self.title,
            "status": self.status,
            "progress": self.progress,
            "logs": list(self.logs),
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }

    def cancel(self):
        self._cancel_event.set()

    def should_cancel(self):
        return self._cancel_event.is_set()


class WorkflowManager:
    def __init__(self, cfg):
        self.cfg = cfg
        self._jobs = {}
        self._lock = threading.Lock()

    def list_jobs(self):
        with self._lock:
            jobs = list(self._jobs.values())
        jobs.sort(key=lambda job: job.created_at, reverse=True)
        return [job.to_dict() for job in jobs]

    def get_job(self, job_id: str):
        with self._lock:
            job = self._jobs.get(job_id)
        return job.to_dict() if job else None

    def get_active_job(self, job_type: Optional[str] = None):
        with self._lock:
            jobs = list(self._jobs.values())
        for job in sorted(jobs, key=lambda item: item.created_at, reverse=True):
            if job.status not in {"queued", "running"}:
                continue
            if job_type and job.job_type != job_type:
                continue
            return job.to_dict()
        return None

    def has_active_job(self, job_type: Optional[str] = None):
        return self.get_active_job(job_type) is not None

    def cancel_job(self, job_id: str):
        with self._lock:
            job = self._jobs.get(job_id)
        if not job:
            return False
        job.cancel()
        if job.status in {"queued", "running"}:
            job.append_log("Cancellation requested.")
        return True

    def start_discogs_import(self, allow_gui_auth=True):
        return self._start_job(
            job_type="discogs-import",
            title="Discogs Import",
            runner=lambda callback, should_cancel, progress_callback: run_discogs_import(
                self.cfg,
                callback=callback,
                should_cancel=should_cancel,
                progress_callback=progress_callback,
                allow_gui_auth=allow_gui_auth,
            ),
        )

    def start_musicbrainz_match(self, username, password, match_all=False):
        return self._start_job(
            job_type="musicbrainz-match",
            title="MusicBrainz Match",
            runner=lambda callback, should_cancel, progress_callback: run_musicbrainz_match(
                self.cfg,
                username=username,
                password=password,
                match_all=match_all,
                callback=callback,
                should_cancel=should_cancel,
                progress_callback=progress_callback,
            ),
        )

    def _start_job(self, job_type: str, title: str, runner: Callable):
        job = WorkflowJob(id=str(uuid.uuid4()), job_type=job_type, title=title)
        job.append_log(f"{title} queued.")

        def target():
            job.status = "running"
            job.started_at = datetime.now(timezone.utc).isoformat()
            job.updated_at = job.started_at
            job.append_log(f"{title} started.")

            def callback(message):
                job.append_log(message)

            def progress_callback(percent):
                job.progress = max(0, min(100, int(percent)))
                job.updated_at = datetime.now(timezone.utc).isoformat()

            try:
                runner(callback, job.should_cancel, progress_callback)
                if job.should_cancel():
                    job.status = "cancelled"
                    job.updated_at = datetime.now(timezone.utc).isoformat()
                    if not job.logs or job.logs[-1] != "Cancellation requested.":
                        job.append_log(f"{title} cancelled.")
                else:
                    job.status = "completed"
                    job.progress = 100
                    job.updated_at = datetime.now(timezone.utc).isoformat()
                    job.append_log(f"{title} completed.")
            except Exception as exc:
                job.status = "failed"
                job.error = str(exc)
                job.updated_at = datetime.now(timezone.utc).isoformat()
                job.append_log(f"Error: {exc}")
                job.append_log(traceback.format_exc())
            finally:
                job.finished_at = datetime.now(timezone.utc).isoformat()
                job.updated_at = job.finished_at

        thread = threading.Thread(target=target, name=f"{job_type}-{job.id[:8]}", daemon=True)
        job._thread = thread
        with self._lock:
            self._jobs[job.id] = job
        thread.start()
        return job.to_dict()
