import html
import json
import os
import posixpath
import secrets
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

from shared.services import CollectionFilters, GrooveKraftService
from shared.workflows import WorkflowManager
import musicbrainz.db_musicbrainz as db_musicbrainz


def run_server(cfg) -> None:
    service = GrooveKraftService(cfg)
    workflows = WorkflowManager(cfg)
    server = GrooveKraftHTTPServer((cfg.server_host, cfg.server_port), GrooveKraftRequestHandler, cfg, service, workflows)
    host, port = server.server_address
    print(f"GrooveKraft server listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


class GrooveKraftHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address, handler_class, cfg, service, workflows):
        super().__init__(server_address, handler_class)
        self.cfg = cfg
        self.service = service
        self.workflows = workflows
        self.pending_discogs_auth = {}


class GrooveKraftRequestHandler(BaseHTTPRequestHandler):
    server_version = "GrooveKraftHTTP/0.1"

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/":
            self.redirect("/collection")
            return
        if path == "/collection":
            self.render_collection(query)
            return
        if path == "/on-this-day":
            self.render_on_this_day(query)
            return
        if path == "/randomiser":
            self.render_randomiser(query)
            return
        if path == "/discogs-import":
            self.render_discogs_import(query)
            return
        if path == "/musicbrainz-match":
            self.render_musicbrainz_match(query)
            return
        if path == "/status.json":
            self.render_status_json()
            return
        if path.startswith("/jobs/") and path.endswith(".json"):
            self.render_job_json(path)
            return
        if path.startswith("/jobs/"):
            self.render_job_detail(path)
            return
        if path.startswith("/release/"):
            self.render_release_detail(path)
            return
        if path.startswith("/images/"):
            self.serve_image(path)
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/discogs-import/start":
            job = self.server.workflows.start_discogs_import(allow_gui_auth=False)
            self.redirect(f"/discogs-import?job={job['id']}")
            return

        if path == "/discogs-import/authorize":
            self.start_discogs_authorization()
            return

        if path == "/discogs-import/complete-auth":
            form = self._read_form()
            auth_id = form.get("auth_id", [""])[0]
            verifier = form.get("oauth_verifier", [""])[0].strip()
            self.complete_discogs_authorization(auth_id, verifier)
            return

        if path == "/musicbrainz-match/start":
            form = self._read_form()
            stored_creds = db_musicbrainz.get_stored_credential_row(self.server.cfg.db_path)
            username = form.get("username", [""])[0].strip()
            password = form.get("password", [""])[0]
            using_stored_credentials = False
            if not username and not password and stored_creds:
                username = stored_creds.username
                password = stored_creds.password
                using_stored_credentials = True
            match_all = form.get("match_all", ["0"])[0] in {"1", "true", "on"}
            remember = form.get("remember_credentials", ["0"])[0] in {"1", "true", "on"}
            if remember and username and password and not using_stored_credentials:
                db_musicbrainz.set_credentials(self.server.cfg.db_path, username, password)
            if not username or not password:
                self.redirect("/musicbrainz-match?credentials=1")
                return
            job = self.server.workflows.start_musicbrainz_match(
                username=username,
                password=password,
                match_all=match_all,
            )
            self.redirect(f"/musicbrainz-match?job={job['id']}")
            return

        if path.startswith("/jobs/") and path.endswith("/cancel"):
            job_id = path.rstrip("/").split("/")[-2]
            self.server.workflows.cancel_job(job_id)
            self.redirect(f"/jobs/{job_id}")
            return

        if not path.startswith("/release/"):
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        discogs_id = self._parse_discogs_id(path)
        if discogs_id is None:
            self.send_error(HTTPStatus.NOT_FOUND, "Release not found")
            return

        form = self._read_form()
        action = form.get("action", [""])[0]

        if action == "play":
            self.server.service.mark_played(discogs_id)
            self.redirect(f"/release/{discogs_id}")
            return
        if action == "clean":
            self.server.service.mark_cleaned(discogs_id)
            self.redirect(f"/release/{discogs_id}")
            return
        if action == "update_date":
            release_date = form.get("release_date", [""])[0].strip()
            locked = form.get("release_date_locked", ["0"])[0] in {"1", "true", "on"}
            if release_date:
                self.server.service.update_release_date(discogs_id, release_date, locked)
            self.redirect(f"/release/{discogs_id}")
            return

        self.send_error(HTTPStatus.BAD_REQUEST, "Unknown action")

    def render_collection(self, query):
        filters = CollectionFilters(
            artist=query.get("artist", [""])[0],
            title=query.get("title", [""])[0],
            storage_format=query.get("storage_format", [None])[0] or None,
            year_from=query.get("year_from", [""])[0],
            year_to=query.get("year_to", [""])[0],
        )
        rows = self.server.service.list_collection(filters)
        body = [self.render_filters(filters), f"<p class='summary'>{len(rows)} releases</p>", "<div class='cards'>"]
        for row in rows:
            image_html = self.release_image_html(row["discogs_id"], row["title"], row["image_exists"], "thumb")
            locked = " 🔒" if row["release_date_locked"] else ""
            body.append(
                f"""
                <a class="card" href="/release/{row['discogs_id']}">
                    {image_html}
                    <div class="card-copy">
                        <h3>{self.escape(row['title'])}</h3>
                        <p>{self.escape(row['artist'])}</p>
                        <p>{self.escape(row['format'] or '')} | {self.escape(row['country'] or '')}</p>
                        <p><strong>{self.escape(row['release_date_human'])}{locked}</strong></p>
                        <p>{self.escape(row['release_date_delta'])}</p>
                        <p>Match: {self.escape(row['match_stars'])}</p>
                    </div>
                </a>
                """
            )
        body.append("</div>")
        self.send_html(self.page("Collection", "".join(body), active="collection"))

    def render_on_this_day(self, query):
        month = self._safe_int(query.get("month", [""])[0]) or None
        day = self._safe_int(query.get("day", [""])[0]) or None
        rows = self.server.service.list_on_this_day(month=month, day=day)
        month_value = month or ""
        day_value = day or ""
        body = [
            """
            <form class="filters" method="get" action="/on-this-day">
                <label>Month <input type="number" name="month" min="1" max="12" value="{month}"></label>
                <label>Day <input type="number" name="day" min="1" max="31" value="{day}"></label>
                <button type="submit">Show</button>
                <a class="button-link" href="/on-this-day">Today</a>
            </form>
            """.format(month=self.escape(str(month_value)), day=self.escape(str(day_value))),
            f"<p class='summary'>{len(rows)} matching releases</p>",
            "<div class='cards'>",
        ]
        for row in rows:
            image_html = self.release_image_html(row["discogs_id"], row["title"], row["image_exists"], "thumb")
            body.append(
                f"""
                <a class="card" href="/release/{row['discogs_id']}">
                    {image_html}
                    <div class="card-copy">
                        <h3>{self.escape(row['title'])}</h3>
                        <p>{self.escape(row['artist'])}</p>
                        <p>{self.escape(row['format'] or '')} | {self.escape(row['country'] or '')}</p>
                        <p><strong>{self.escape(row['release_date_human'])}</strong></p>
                        <p>{self.escape(row['release_date_delta'])}</p>
                        <p>Match: {self.escape(row['match_stars'])}</p>
                    </div>
                </a>
                """
            )
        body.append("</div>")
        self.send_html(self.page("On This Day", "".join(body), active="on-this-day"))

    def render_randomiser(self, query):
        filters = CollectionFilters(
            artist=query.get("artist", [""])[0],
            title=query.get("title", [""])[0],
            storage_format=query.get("storage_format", [None])[0] or None,
            year_from=query.get("year_from", [""])[0],
            year_to=query.get("year_to", [""])[0],
        )
        detail = self.server.service.get_random_release(filters)
        body = [self.render_filters(filters, action="/randomiser", submit_label="Randomise")]
        if detail:
            body.append(self.render_release_panel(detail, show_actions=False))
        else:
            body.append("<p class='summary'>No matching release.</p>")
        self.send_html(self.page("Randomiser", "".join(body), active="randomiser"))

    def render_discogs_import(self, query):
        current_job = self._job_from_query(query)
        oauth_ready = self.discogs_oauth_ready()
        auth_id = query.get("auth_id", [""])[0]
        pending = self.server.pending_discogs_auth.get(auth_id) if auth_id else None
        status_job = current_job or self._latest_job("discogs-import")
        body = ['<section class="panel"><h2>Import from Discogs</h2>']
        if current_job:
            body.append("<p class='summary'>Import is in progress. Import controls are hidden until it finishes or is cancelled.</p>")
            body.append(self.render_job_card(current_job, expanded=True, show_title=False))
        else:
            if oauth_ready:
                body.append("<p class='summary'>Discogs is already authorized for this app.</p>")
                body.append("""
                    <form method="post" action="/discogs-import/start">
                        <button type="submit">Start Import</button>
                    </form>
                """)
            elif pending:
                body.append(f"""
                    <p class="summary">Authorization step 1: open Discogs and approve access.</p>
                    <p><a class="button-link" href="{self.escape(pending['url'])}" target="_blank" rel="noreferrer">Open Discogs Authorization</a></p>
                    <form class="filters" method="post" action="/discogs-import/complete-auth">
                        <input type="hidden" name="auth_id" value="{self.escape(auth_id)}">
                        <label>Verification code <input type="text" name="oauth_verifier" placeholder="Paste the Discogs verifier code"></label>
                        <button type="submit">Finish Authorization</button>
                    </form>
                """)
            else:
                body.append("<p class='summary'>Authorize Discogs once, then imports can run directly from the web app.</p>")
                body.append("""
                    <form method="post" action="/discogs-import/authorize">
                        <button type="submit">Authorize Discogs</button>
                    </form>
                """)
            body.append("<hr class='panel-divider'>")
            body.append("<h3>Import Status</h3>")
            if status_job:
                body.append(self.render_job_card(status_job, expanded=False, show_title=False))
            else:
                body.append("<p class='summary'>No import has been run yet.</p>")
        body.append("</section>")
        self.send_html(self.page("Import from Discogs", "".join(body), active="discogs-import"))

    def render_musicbrainz_match(self, query):
        current_job = self._job_from_query(query)
        stored_creds = db_musicbrainz.get_stored_credential_row(self.server.cfg.db_path)
        status_job = current_job or self._latest_job("musicbrainz-match")
        show_credentials_form = (
            query.get("credentials", [""])[0] == "1"
            or not stored_creds
            or self._job_needs_new_credentials(status_job)
        )
        body = ['<section class="panel"><h2>Match with MusicBrainz</h2>']
        if current_job:
            body.append("<p class='summary'>Matching is in progress. Match options are hidden until it finishes or is cancelled.</p>")
            body.append(self.render_job_card(current_job, expanded=True, show_title=False))
        else:
            if show_credentials_form:
                if self._job_needs_new_credentials(status_job):
                    body.append("<p class='summary'>Authentication failed. Enter your MusicBrainz username and password to retry.</p>")
                else:
                    body.append("<p class='summary'>Enter your MusicBrainz username and password to start matching.</p>")
                body.append("""
                    <form class="match-form" method="post" action="/musicbrainz-match/start">
                        <div class="field-grid">
                            <label>Username <input type="text" name="username" value=""></label>
                            <label>Password <input type="password" name="password" value=""></label>
                        </div>
                        <div class="option-list">
                            <label class="checkbox-row">
                                <input type="checkbox" name="match_all" value="1">
                                <span>Match all items</span>
                            </label>
                            <label class="checkbox-row">
                                <input type="checkbox" name="remember_credentials" value="1" checked>
                                <span>Remember credentials on this machine</span>
                            </label>
                        </div>
                        <div class="form-actions">
                            <button type="submit">Start Match</button>
                        </div>
                    </form>
                """)
            else:
                if stored_creds:
                    body.append(f"<p class='summary'>Stored MusicBrainz credentials for <strong>{self.escape(stored_creds.username)}</strong> will be tried first.</p>")
                else:
                    body.append("<p class='summary'>No stored MusicBrainz credentials are available.</p>")
                body.append("""
                    <form class="match-form" method="post" action="/musicbrainz-match/start">
                        <div class="option-list">
                            <label class="checkbox-row">
                                <input type="checkbox" name="match_all" value="1">
                                <span>Match all items</span>
                            </label>
                        </div>
                        <div class="form-actions">
                            <button type="submit">Start Match</button>
                        </div>
                    </form>
                """)
                body.append('<p><a href="/musicbrainz-match?credentials=1">Enter different credentials</a></p>')
            body.append("<hr class='panel-divider'>")
            body.append("<h3>Match Status</h3>")
            if status_job:
                body.append(self.render_job_card(status_job, expanded=False, show_title=False))
            else:
                body.append("<p class='summary'>No match has been run yet.</p>")
        body.append("</section>")
        self.send_html(self.page("Match with MusicBrainz", "".join(body), active="musicbrainz-match"))

    def render_job_json(self, path):
        job_id = path.rstrip("/").split("/")[-1][:-5]
        job = self.server.workflows.get_job(job_id)
        if not job:
            self.send_error(HTTPStatus.NOT_FOUND, "Job not found")
            return
        self.send_json(
            {
                "job": job,
                "navigation_locked": self._navigation_locked(),
            }
        )

    def render_status_json(self):
        active_import_job = self.server.workflows.get_active_job("discogs-import")
        active_match_job = self.server.workflows.get_active_job("musicbrainz-match")
        self.send_json(
            {
                "navigation_locked": self._navigation_locked(),
                "active_import_job_id": active_import_job["id"] if active_import_job else "",
                "active_match_job_id": active_match_job["id"] if active_match_job else "",
            }
        )

    def render_job_detail(self, path):
        job_id = path.rstrip("/").split("/")[-1]
        job = self.server.workflows.get_job(job_id)
        if not job:
            self.send_error(HTTPStatus.NOT_FOUND, "Job not found")
            return
        body = [
            '<p><a href="/discogs-import">Back to import</a> | <a href="/musicbrainz-match">Back to match</a></p>',
            self.render_job_card(job, expanded=True),
        ]
        active = "discogs-import" if job["job_type"] == "discogs-import" else "musicbrainz-match"
        self.send_html(self.page(job["title"], "".join(body), active=active))

    def render_release_detail(self, path):
        discogs_id = self._parse_discogs_id(path)
        if discogs_id is None:
            self.send_error(HTTPStatus.NOT_FOUND, "Release not found")
            return
        detail = self.server.service.get_release_detail(discogs_id)
        if not detail:
            self.send_error(HTTPStatus.NOT_FOUND, "Release not found")
            return
        body = self.render_release_panel(detail, show_actions=True)
        self.send_html(self.page(detail["title"], body, active=None))

    def render_release_panel(self, detail, show_actions: bool):
        locked_checked = "checked" if detail["release_date_locked"] else ""
        actions = ""
        if show_actions:
            actions = f"""
                <div class="actions">
                    <form method="post" action="/release/{detail['discogs_id']}">
                        <input type="hidden" name="action" value="play">
                        <button type="submit">Playing now</button>
                    </form>
                    <form method="post" action="/release/{detail['discogs_id']}">
                        <input type="hidden" name="action" value="clean">
                        <button type="submit">Clean</button>
                    </form>
                </div>
                <form class="date-form" method="post" action="/release/{detail['discogs_id']}">
                    <input type="hidden" name="action" value="update_date">
                    <label>Release date
                        <input type="text" name="release_date" value="{self.escape(detail['release_date'] or '')}" placeholder="YYYY or YYYY-MM or YYYY-MM-DD">
                    </label>
                    <label class="checkbox">
                        <input type="checkbox" name="release_date_locked" value="1" {locked_checked}>
                        Lock release date
                    </label>
                    <button type="submit">Save date</button>
                </form>
            """

        lock_text = " 🔒" if detail["release_date_locked"] else ""
        image_html = self.release_image_html(detail["discogs_id"], detail["title"], detail["image_exists"], "hero")
        return f"""
            <div class="detail-shell">
                {image_html}
                <div class="detail-copy">
                    <p><a href="/collection">Back to collection</a></p>
                    <h1>{self.escape(detail['title'])}</h1>
                    <p class="muted">{self.escape(detail['artist'])}</p>
                    <dl class="detail-grid">
                        <dt>Format</dt><dd>{self.escape(detail['format'] or '')}</dd>
                        <dt>Country</dt><dd>{self.escape(detail['country'] or '')}</dd>
                        <dt>Release Date</dt><dd>{self.escape(detail['release_date_human'])}{lock_text}<br>{self.escape(detail['release_date_delta'])}</dd>
                        <dt>Discogs Id</dt><dd>{detail['discogs_id']}</dd>
                        <dt>Catalog Numbers</dt><dd>{self.escape(detail['catnos'] or '')}</dd>
                        <dt>Barcodes</dt><dd>{self.escape(detail['barcodes'] or '')}</dd>
                        <dt>Matched</dt><dd>{"Yes" if detail['matched'] else "No"} ({self.escape(detail['match_stars'])})</dd>
                        <dt>Play Count</dt><dd>{detail['play_count']} | {self.escape(detail['last_played'] or 'Never')}</dd>
                        <dt>Clean Count</dt><dd>{detail['clean_count']} | {self.escape(detail['last_cleaned'] or 'Never')}</dd>
                    </dl>
                    {actions}
                </div>
            </div>
        """

    def render_job_card(self, job, expanded=False, show_title=True):
        logs = "".join(f"<li>{self.escape(line)}</li>" for line in job["logs"][-40:])
        cancel_form = ""
        if job["status"] in {"queued", "running"}:
            cancel_form = f"""
                <form method="post" action="/jobs/{job['id']}/cancel">
                    <button type="submit">Cancel Job</button>
                </form>
            """
        detail_link = "" if expanded else f'<p><a href="/jobs/{job["id"]}">Open job</a></p>'
        live_note_text = ""
        if job["status"] in {"queued", "running"}:
            live_note_text = "Live status. Updating automatically."
        is_live = str(job["status"] in {"queued", "running"}).lower()
        title_html = f"<h3>{self.escape(job['title'])}</h3>" if show_title else ""
        return f"""
            <article class="job-card" data-job-id="{self.escape(job['id'])}" data-job-live="{is_live}">
                {title_html}
                <p>Status: <strong data-job-field="status">{self.escape(job['status'])}</strong></p>
                <p data-job-field="summary">{self.escape(self.job_summary(job))}</p>
                <p class="summary" data-job-field="live-note">{self.escape(live_note_text)}</p>
                <p class="summary" data-job-field="updated-at">Last update: {self.escape(job.get('updated_at') or job.get('created_at') or '')}</p>
                <div class="progress-shell"><div class="progress-bar" data-job-field="progress-bar" style="width: {job['progress']}%;"></div></div>
                <p data-job-field="progress-label">Progress: {job['progress']}%</p>
                {detail_link}
                <div data-job-field="cancel-form">{cancel_form}</div>
                <ul class="job-log" data-job-field="logs">{logs}</ul>
            </article>
        """

    def _latest_job(self, job_type):
        jobs = [job for job in self.server.workflows.list_jobs() if job["job_type"] == job_type]
        return jobs[0] if jobs else None

    @staticmethod
    def _job_needs_new_credentials(job):
        if not job or job.get("status") != "failed":
            return False
        error = (job.get("error") or "").lower()
        markers = ("unauthorized", "authentication", "username", "password", "401", "forbidden", "credentials")
        return any(marker in error for marker in markers)

    def start_discogs_authorization(self):
        from discogs import discogs_importer

        client, _, _, url = discogs_importer.begin_discogs_authorization()
        auth_id = secrets.token_urlsafe(12)
        self.server.pending_discogs_auth[auth_id] = {"client": client, "url": url}
        self.redirect(f"/discogs-import?auth_id={auth_id}")

    def complete_discogs_authorization(self, auth_id, verifier):
        from discogs import discogs_importer

        pending = self.server.pending_discogs_auth.get(auth_id)
        if not pending or not verifier:
            self.redirect("/discogs-import")
            return
        discogs_importer.complete_discogs_authorization(
            self.server.cfg.db_path,
            pending["client"],
            verifier,
        )
        self.server.pending_discogs_auth.pop(auth_id, None)
        self.redirect("/discogs-import")

    def discogs_oauth_ready(self):
        from discogs.db_discogs import get_oauth_tokens

        return bool(get_oauth_tokens(self.server.cfg.db_path))

    def _job_from_query(self, query):
        job_id = query.get("job", [""])[0]
        return self.server.workflows.get_job(job_id) if job_id else None

    @staticmethod
    def job_summary(job):
        if job["status"] == "queued":
            return "Queued and waiting to start."
        if job["status"] == "running" and job["progress"] == 0:
            return "Started. Connecting and preparing the import before item-level progress begins."
        if job["status"] == "running" and job["job_type"] == "musicbrainz-match" and job["progress"] >= 95:
            return "Finalizing the last MusicBrainz results."
        if job["status"] == "running":
            return "Running now."
        if job["status"] == "completed":
            return "Completed successfully."
        if job["status"] == "failed":
            return f"Failed: {job['error'] or 'unknown error'}"
        if job["status"] == "cancelled":
            return "Cancelled."
        return job["status"]

    def render_filters(self, filters: CollectionFilters, action="/collection", submit_label="Apply"):
        selected_all = "selected" if not filters.storage_format else ""
        selected_lp = "selected" if filters.storage_format == '12" vinyl' else ""
        selected_single = "selected" if filters.storage_format == '7" vinyl' else ""
        selected_cd = "selected" if filters.storage_format == "Compact Disc" else ""
        qs = urlencode({})
        return f"""
            <form class="filters" method="get" action="{action}">
                <label>Artist <input type="text" name="artist" value="{self.escape(filters.artist)}"></label>
                <label>Title <input type="text" name="title" value="{self.escape(filters.title)}"></label>
                <label>Format
                    <select name="storage_format">
                        <option value="" {selected_all}>All</option>
                        <option value='12" vinyl' {selected_lp}>12" Vinyl</option>
                        <option value='7" vinyl' {selected_single}>7" Vinyl</option>
                        <option value="Compact Disc" {selected_cd}>Compact Disc</option>
                    </select>
                </label>
                <label>Year from <input type="text" name="year_from" value="{self.escape(filters.year_from)}"></label>
                <label>Year to <input type="text" name="year_to" value="{self.escape(filters.year_to)}"></label>
                <button type="submit">{submit_label}</button>
                <a class="button-link" href="{action}{('?' + qs) if qs else ''}">Clear</a>
            </form>
        """

    def serve_image(self, path):
        filename = posixpath.basename(path)
        safe_name = os.path.basename(filename)
        file_path = os.path.join(self.server.cfg.images_folder, safe_name)
        if not os.path.isfile(file_path):
            self.send_error(HTTPStatus.NOT_FOUND, "Image not found")
            return

        with open(file_path, "rb") as handle:
            content = handle.read()

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "image/jpeg")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def page(self, title: str, body: str, active: str | None, refresh_seconds: int | None = None):
        def nav_link(target, label, key):
            class_name = "active" if active == key else ""
            return f'<a class="{class_name}" href="{target}">{label}</a>'
        import_active = self.server.workflows.has_active_job("discogs-import")
        active_import_job = self.server.workflows.get_active_job("discogs-import")
        active_match_job = self.server.workflows.get_active_job("musicbrainz-match")
        active_import_job_id = active_import_job["id"] if active_import_job else ""
        active_match_job_id = active_match_job["id"] if active_match_job else ""
        navigation_locked = self._navigation_locked()

        return f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <title>{self.escape(title)} | GrooveKraft</title>
                <style>
                    :root {{
                        --bg: #f5efe4;
                        --card: #fffaf2;
                        --ink: #1d1c1a;
                        --muted: #6f665b;
                        --line: #d7c9b4;
                        --accent: #176b87;
                        --accent-2: #b85c38;
                    }}
                    * {{ box-sizing: border-box; }}
                    body {{ margin: 0; font-family: Georgia, "Times New Roman", serif; color: var(--ink); background:
                        radial-gradient(circle at top left, #fff7ea, transparent 28%),
                        linear-gradient(180deg, #f7f1e8 0%, var(--bg) 100%);
                    }}
                    a {{ color: var(--accent); text-decoration: none; }}
                    .shell {{ max-width: 1200px; margin: 0 auto; padding: 24px; }}
                    header {{ display: flex; flex-wrap: wrap; gap: 16px; align-items: end; justify-content: space-between; margin-bottom: 24px; }}
                    h1, h2, h3 {{ margin: 0; font-weight: 600; }}
                    nav {{ display: flex; gap: 10px; flex-wrap: wrap; }}
                    nav a, .button-link, button {{
                        border: 1px solid var(--line); background: var(--card); color: var(--ink); padding: 10px 14px;
                        border-radius: 999px; font: inherit; cursor: pointer;
                    }}
                    nav a.active {{ background: var(--accent); color: white; border-color: var(--accent); }}
                    nav a.disabled {{ opacity: 0.45; pointer-events: none; }}
                    .filters {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 18px; }}
                    .match-form {{ display: block; }}
                    .field-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; margin-bottom: 18px; }}
                    label {{ display: grid; gap: 6px; font-size: 14px; color: var(--muted); }}
                    input, select {{ width: 100%; padding: 10px 12px; border: 1px solid var(--line); border-radius: 12px; font: inherit; background: white; }}
                    .option-list {{ display: grid; gap: 10px; margin-bottom: 18px; }}
                    .checkbox-row {{ display: flex; align-items: center; gap: 10px; font-size: 16px; color: var(--ink); }}
                    .checkbox-row input[type="checkbox"] {{ width: 18px; height: 18px; margin: 0; flex: 0 0 auto; }}
                    .checkbox-row span {{ line-height: 1.3; }}
                    .form-actions {{ display: flex; justify-content: center; margin-top: 4px; width: 100%; }}
                    .summary {{ color: var(--muted); margin: 0 0 18px; }}
                    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }}
                    .card {{
                        display: grid; grid-template-columns: 104px 1fr; gap: 14px; align-items: start; padding: 14px;
                        background: rgba(255, 250, 242, 0.88); border: 1px solid var(--line); border-radius: 22px;
                        box-shadow: 0 14px 34px rgba(76, 52, 24, 0.08);
                    }}
                    .card-copy p {{ margin: 0 0 8px; }}
                    .thumb, .hero, .placeholder {{
                        border-radius: 18px; background: linear-gradient(135deg, #eadac1, #d6b694); object-fit: cover;
                        width: 100%;
                    }}
                    .thumb, .placeholder.thumb {{ height: 104px; }}
                    .hero, .placeholder.hero {{ max-width: 360px; aspect-ratio: 1 / 1; }}
                    .placeholder {{ display: grid; place-items: center; color: rgba(29, 28, 26, 0.65); font-size: 14px; }}
                    .detail-shell {{
                        display: grid; grid-template-columns: minmax(220px, 360px) 1fr; gap: 24px;
                        align-items: start; background: rgba(255, 250, 242, 0.9); padding: 20px; border-radius: 24px; border: 1px solid var(--line);
                    }}
                    .detail-grid {{ display: grid; grid-template-columns: 160px 1fr; gap: 10px 14px; }}
                    .detail-grid dt {{ font-weight: 700; }}
                    .detail-grid dd {{ margin: 0; }}
                    .muted {{ color: var(--muted); }}
                    .actions {{ display: flex; gap: 10px; flex-wrap: wrap; margin: 18px 0; }}
                    .date-form {{ display: grid; gap: 12px; max-width: 460px; }}
                    .panel {{ background: rgba(255, 250, 242, 0.9); border: 1px solid var(--line); border-radius: 24px; padding: 20px; margin-bottom: 20px; }}
                    .panel-divider {{ border: 0; border-top: 1px solid var(--line); margin: 20px 0; }}
                    .jobs {{ display: grid; gap: 16px; }}
                    .job-card {{ background: white; border: 1px solid var(--line); border-radius: 18px; padding: 16px; }}
                    .job-log {{ margin: 12px 0 0; padding-left: 20px; max-height: 360px; overflow: auto; }}
                    .progress-shell {{ width: 100%; height: 12px; border-radius: 999px; background: #eadfce; overflow: hidden; margin: 10px 0; }}
                    .progress-bar {{ height: 100%; background: linear-gradient(90deg, var(--accent), var(--accent-2)); }}
                    footer {{ margin-top: 28px; color: var(--muted); font-size: 14px; }}
                    @media (max-width: 720px) {{
                        .detail-shell {{ grid-template-columns: 1fr; }}
                        .detail-grid {{ grid-template-columns: 1fr; }}
                    }}
                </style>
            </head>
            <body data-navigation-locked="{str(navigation_locked).lower()}" data-active-import-job-id="{self.escape(active_import_job_id)}" data-active-match-job-id="{self.escape(active_match_job_id)}">
                <div class="shell">
                    <header>
                        <div>
                            <p class="muted">GrooveKraft web mode</p>
                            <h1>{self.escape(title)}</h1>
                        </div>
                        <nav>
                            {nav_link("/collection", "Collection", "collection")}
                            {nav_link("/on-this-day", "On This Day", "on-this-day")}
                            {nav_link("/randomiser", "Randomiser", "randomiser")}
                            {nav_link("/discogs-import", "Import from Discogs", "discogs-import")}
                            {nav_link("/musicbrainz-match", "Match with MusicBrainz", "musicbrainz-match")}
                        </nav>
                    </header>
                    <main>{body}</main>
                    <footer>Server mode is powered by the same local SQLite database and image cache as the desktop app.</footer>
                </div>
                <script>
                    const bodyEl = document.body;
                    function escapeHtml(value) {{
                        return String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
                    }}
                    function jobSummary(job) {{
                        if (job.status === "queued") return "Queued and waiting to start.";
                        if (job.status === "running" && Number(job.progress || 0) === 0) return "Started. Connecting and preparing the import before item-level progress begins.";
                        if (job.status === "running" && job.job_type === "musicbrainz-match" && Number(job.progress || 0) >= 95) return "Finalizing the last MusicBrainz results.";
                        if (job.status === "running") return "Running now.";
                        if (job.status === "completed") return "Completed successfully.";
                        if (job.status === "failed") return `Failed: ${{job.error || "unknown error"}}`;
                        if (job.status === "cancelled") return "Cancelled.";
                        return job.status;
                    }}
                    function formatLastUpdate(isoString) {{
                        if (!isoString) return "Last update: unknown";
                        const then = new Date(isoString);
                        if (Number.isNaN(then.getTime())) return `Last update: ${{isoString}}`;
                        const diffSeconds = Math.max(0, Math.round((Date.now() - then.getTime()) / 1000));
                        if (diffSeconds < 5) return "Last update: just now";
                        if (diffSeconds < 60) return `Last update: ${{diffSeconds}}s ago`;
                        const diffMinutes = Math.round(diffSeconds / 60);
                        return `Last update: ${{diffMinutes}}m ago`;
                    }}
                    function updateNavigation(locked) {{
                        bodyEl.dataset.navigationLocked = locked ? "true" : "false";
                        document.querySelectorAll("nav a").forEach((link) => {{
                            const href = link.getAttribute("href") || "";
                            const currentPath = window.location.pathname;
                            const isCurrentPage = href === currentPath;
                            if (locked && !isCurrentPage) {{
                                link.classList.add("disabled");
                                link.setAttribute("aria-disabled", "true");
                                link.setAttribute("tabindex", "-1");
                            }} else {{
                                link.classList.remove("disabled");
                                link.removeAttribute("aria-disabled");
                                link.removeAttribute("tabindex");
                            }}
                        }});
                    }}
                    function updateJobCard(card, payload) {{
                        const job = payload.job;
                        card.dataset.jobLive = ["queued", "running"].includes(job.status) ? "true" : "false";
                        const statusEl = card.querySelector('[data-job-field="status"]');
                        const summaryEl = card.querySelector('[data-job-field="summary"]');
                        const noteEl = card.querySelector('[data-job-field="live-note"]');
                        const barEl = card.querySelector('[data-job-field="progress-bar"]');
                        const progressEl = card.querySelector('[data-job-field="progress-label"]');
                        const logsEl = card.querySelector('[data-job-field="logs"]');
                        const cancelEl = card.querySelector('[data-job-field="cancel-form"]');
                        const updatedEl = card.querySelector('[data-job-field="updated-at"]');
                        if (statusEl) statusEl.textContent = job.status;
                        if (summaryEl) summaryEl.textContent = jobSummary(job);
                        if (noteEl) noteEl.textContent = ["queued", "running"].includes(job.status) ? "Live status. Updating automatically." : "";
                        if (updatedEl) updatedEl.textContent = formatLastUpdate(job.updated_at || job.created_at);
                        if (barEl) barEl.style.width = `${{Number(job.progress || 0)}}%`;
                        if (progressEl) progressEl.textContent = `Progress: ${{Number(job.progress || 0)}}%`;
                        if (logsEl) logsEl.innerHTML = (job.logs || []).slice(-40).map((line) => `<li>${{escapeHtml(line)}}</li>`).join("");
                        if (cancelEl) {{
                            cancelEl.innerHTML = ["queued", "running"].includes(job.status)
                                ? `<form method="post" action="/jobs/${{job.id}}/cancel"><button type="submit">Cancel Job</button></form>`
                                : "";
                        }}
                        updateNavigation(Boolean(payload.navigation_locked));
                    }}
                    async function pollJob(card) {{
                        const jobId = card.dataset.jobId;
                        if (!jobId) return;
                        try {{
                            const response = await fetch(`/jobs/${{jobId}}.json`, {{ cache: "no-store" }});
                            if (!response.ok) return;
                            const payload = await response.json();
                            updateJobCard(card, payload);
                            if (["queued", "running"].includes(payload.job.status)) {{
                                window.setTimeout(() => pollJob(card), 2000);
                            }}
                        }} catch (_err) {{
                            window.setTimeout(() => pollJob(card), 3000);
                        }}
                    }}
                    async function pollStatus() {{
                        try {{
                            const response = await fetch("/status.json", {{ cache: "no-store" }});
                            if (response.ok) {{
                                const payload = await response.json();
                                updateNavigation(Boolean(payload.navigation_locked));
                                if (payload.active_import_job_id) {{
                                    bodyEl.dataset.activeImportJobId = payload.active_import_job_id;
                                }}
                                if (payload.active_match_job_id) {{
                                    bodyEl.dataset.activeMatchJobId = payload.active_match_job_id;
                                }}
                            }}
                        }} finally {{
                            window.setTimeout(pollStatus, 3000);
                        }}
                    }}
                    updateNavigation(bodyEl.dataset.navigationLocked === "true");
                    document.querySelectorAll('[data-job-live="true"]').forEach((card) => pollJob(card));
                    pollStatus();
                </script>
            </body>
            </html>
        """

    def release_image_html(self, discogs_id: int, title: str, image_exists: bool, css_class: str):
        if image_exists:
            return f'<img class="{css_class}" src="/images/{discogs_id}.jpg" alt="{self.escape(title)}">'
        return f'<div class="placeholder {css_class}">No artwork</div>'

    def send_html(self, content: str, status: int = HTTPStatus.OK):
        encoded = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def send_json(self, payload: dict, status: int = HTTPStatus.OK):
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _navigation_locked(self):
        return (
            self.server.workflows.has_active_job("discogs-import")
            or self.server.workflows.has_active_job("musicbrainz-match")
        )

    def redirect(self, location: str):
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        self.end_headers()

    def _read_form(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8")
        return parse_qs(raw)

    @staticmethod
    def escape(value):
        return html.escape("" if value is None else str(value))

    @staticmethod
    def _safe_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_discogs_id(path: str):
        try:
            return int(path.rstrip("/").split("/")[-1])
        except (TypeError, ValueError):
            return None
