from PyQt6.QtWidgets import (
    QApplication, QLabel, QWidget, QVBoxLayout, QMainWindow, QTabWidget, QTextEdit, QTableWidget, QTableWidgetItem,
    QLineEdit, QHBoxLayout, QPushButton, QFormLayout, QGroupBox, QProgressBar, QDialog
)
from PyQt6.QtGui import QKeySequence, QShortcut
from PyQt6.QtCore import Qt
from PyQt6.QtCore import QObject, pyqtSignal, QThread

import musicbrainzngs
import sys
from types import SimpleNamespace

from modules import db, utils
from discogs import discogs_importer
from musicbrainz import mb_matcher, mb_auth_gui
from modules.config import AppConfig

import musicbrainz.db_musicbrainz as db_musicbrainz


class ReleaseDetailWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.labels = {}

        outer_layout = QVBoxLayout(self)
        outer_layout.setContentsMargins(10, 10, 10, 10)

        group = QGroupBox("Release Details")
        form = QFormLayout()
        form.setHorizontalSpacing(20)
        form.setVerticalSpacing(10)
        group.setLayout(form)
        outer_layout.addWidget(group)

        for field in ['Artist', 'Title', 'Format', 'Country', 'Release Date',
                      'Discogs Id', 'Catalog Numbers', 'Barcodes', 'Matched']:
            label_widget = QLabel(f"{field}:")
            font = label_widget.font()
            font.setBold(True)
            label_widget.setFont(font)

            value_label = QLabel()
            value_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)

            self.labels[field] = value_label
            form.addRow(label_widget, value_label)

    def update_data(self, data: dict):
        for key, value in data.items():
            if key == 'Matched':
                self.labels[key].setText("Yes" if value else "No")
            else:
                self.labels[key].setText(str(value))


class CollectionViewer(QMainWindow):
    class DiscogsImportWorker(QObject):
        progress_msg = pyqtSignal(str)
        finished = pyqtSignal()
        progress = pyqtSignal(int)

        def __init__(self, client):
            super().__init__()
            self.client = client
            self._cancel_requested = False

        def cancel(self):
            self._cancel_requested = True

        def run(self):
            def emit_msg(msg):
                self.progress_msg.emit(msg)

            from discogs import discogs_importer
            try:
                discogs_importer.import_from_discogs(
                    discogs_client=self.client,
                    callback=emit_msg,
                    should_cancel=lambda: self._cancel_requested,
                    progress_callback=lambda pct: self.progress.emit(pct)
                )
            except Exception as e:
                self.progress_msg.emit(f"Error: {e}")
            self.finished.emit()

    def __init__(self, cfg: AppConfig):
        super().__init__()
        self.setWindowTitle("Collection Viewer")
        self.cfg = cfg
        self.setMinimumSize(800, 600)
        tab_widget = QTabWidget()
        on_this_day_tab = self.create_on_this_day_tab()
        tab_widget.addTab(on_this_day_tab, "On this day")
        collection_tab = self.create_collection_tab()
        tab_widget.addTab(collection_tab, "Collection")
        randomiser_tab = self.create_randomiser_tab()
        tab_widget.addTab(randomiser_tab, "Randomiser")
        importer_tab = self.create_discogs_importer_tab()
        tab_widget.addTab(importer_tab, "Discogs Importer")
        matcher_tab = self.create_musicbrainz_matcher_tab()
        tab_widget.addTab(matcher_tab, "MusicBrainz Matcher")
        self.setCentralWidget(tab_widget)
        self.esc_shortcut = QShortcut(QKeySequence("Escape"), self)
        self.esc_shortcut.activated.connect(self.close)
        self.show()

    def create_on_this_day_tab(self):
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)

        table = QTableWidget()
        layout.addWidget(table)

        # placeholder filters
        find_filter = ''
        format_filter = ''

        with db.context_manager() as cur:

            query = []

            query.append(
                'SELECT d.sort_name AS artist, d.title, d.format, d.release_date, d.discogs_id')
            query.append('FROM discogs_releases d')
            query.append('LEFT JOIN mb_matches m USING(discogs_id)')
            query.append('WHERE d.release_date IS NOT NULL')
            if find_filter:
                query.append(f'AND (d.artist LIKE "%{find_filter}%"')
                query.append(f'OR d.title LIKE "%{find_filter}%"')
                query.append(f'OR d.sort_name LIKE "%{find_filter}%")')
            if format_filter:
                query.append(f'AND d.format LIKE "%{format_filter}%"')
            query.append(
                'ORDER BY length(d.release_date) DESC, d.release_date, d.sort_name, d.title, d.discogs_id')

            cur.execute(' '.join(query))

            items = cur.fetchall()

        # filter rows
        rows = []

        for item in items:
            if item.release_date and len(item.release_date) == 10:
                include = utils.is_today_anniversary(item.release_date)

            elif item.release_date and len(item.release_date) == 7:
                include = utils.is_month_anniversary(item.release_date)
            else:
                include = False

            if include:
                rows.append(item)

        table.setColumnCount(5)
        table.setHorizontalHeaderLabels(
            ['Anniversary', 'Artist', 'Title', 'Format', 'Discogs Id', 'Release Date'])
        table.setRowCount(len(rows))

        for row_idx, (artist, title, format, release_date, discogs_id) in enumerate(rows):

            if release_date:
                table.setItem(row_idx, 0, QTableWidgetItem(utils.humanize_date_delta(release_date)))

            table.setItem(row_idx, 1, QTableWidgetItem(artist))
            table.setItem(row_idx, 2, QTableWidgetItem(title))
            table.setItem(row_idx, 3, QTableWidgetItem(format))
            table.setItem(row_idx, 4, QTableWidgetItem(str(discogs_id)))
            table.item(row_idx, 4).setTextAlignment(
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            if release_date:
                table.setItem(row_idx, 5, QTableWidgetItem(
                    utils.parse_and_humanize_date(release_date)))

        table.resizeColumnsToContents()
        return widget

    def create_collection_tab(self):
        widget = QWidget()
        main_layout = QVBoxLayout(widget)

        # Filter row
        filter_layout = QHBoxLayout()
        main_layout.addLayout(filter_layout)

        # Artist Filter
        filter_layout.addWidget(QLabel("Artist:"))
        artist_input = QLineEdit()
        filter_layout.addWidget(artist_input)

        # Title Filter
        filter_layout.addWidget(QLabel("Title:"))
        title_input = QLineEdit()
        filter_layout.addWidget(title_input)

        # Format Filter
        filter_layout.addWidget(QLabel("Format:"))
        format_input = QLineEdit()
        filter_layout.addWidget(format_input)

        clear_button = QPushButton("Clear Filters")
        filter_layout.addWidget(clear_button)

        # Table
        table = QTableWidget()
        main_layout.addWidget(table)

        def populate_table():
            with db.context_manager() as cur:
                query = []
                query.append(
                    "SELECT d.sort_name, d.artist, d.title, d.format, d.country, d.release_date, d.discogs_id, m.mbid")
                query.append("FROM discogs_releases d")
                query.append("LEFT JOIN mb_matches m USING(discogs_id)")
                filters = []

                if artist_input.text():
                    filters.append(f'd.sort_name LIKE "%{artist_input.text()}%"')
                if title_input.text():
                    filters.append(f'd.title LIKE "%{title_input.text()}%"')
                if format_input.text():
                    filters.append(f'd.format LIKE "%{format_input.text()}%"')

                if filters:
                    query.append("WHERE " + " AND ".join(filters))

                query.append("ORDER BY d.sort_name, d.release_date, d.title, d.discogs_id")
                cur.execute(' '.join(query))
                rows = cur.fetchall()

            table.setColumnCount(7)
            table.setHorizontalHeaderLabels(
                ['Artist', 'Title', 'Format', 'Country', 'Release Date', 'Discogs Id', 'Matched'])
            table.setRowCount(len(rows))

            for row_idx, (sort_name, artist, title, format, country, release_date, discogs_id, mbid) in enumerate(rows):
                table.setItem(row_idx, 0, QTableWidgetItem(artist))
                table.setItem(row_idx, 1, QTableWidgetItem(title))
                table.setItem(row_idx, 2, QTableWidgetItem(format))
                table.setItem(row_idx, 3, QTableWidgetItem(country))
                table.setItem(row_idx, 4, QTableWidgetItem(release_date))
                table.setItem(row_idx, 5, QTableWidgetItem(str(discogs_id)))
                table.item(row_idx, 5).setTextAlignment(
                    Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                table.setItem(row_idx, 6, QTableWidgetItem("Yes" if mbid else "No"))

            table.resizeColumnsToContents()

        # Connect filter changes to repopulate the table
        artist_input.textChanged.connect(populate_table)
        title_input.textChanged.connect(populate_table)
        format_input.textChanged.connect(populate_table)

        def clear_filters():
            artist_input.clear()
            title_input.clear()
            format_input.clear()

        clear_button.clicked.connect(clear_filters)

        populate_table()
        return widget

    def create_randomiser_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        self.detail_widget = ReleaseDetailWidget()
        layout.addWidget(self.detail_widget)

        def load_random_item():
            with db.context_manager() as cur:
                cur.execute(
                    "SELECT d.artist, d.title, d.format, d.country, d.release_date, d.discogs_id, d.catnos, d.barcodes, m.mbid "
                    "FROM discogs_releases d "
                    "LEFT JOIN mb_matches m USING(discogs_id) "
                    "ORDER BY RANDOM() LIMIT 1")
                row = cur.fetchone()
                if row:
                    data = dict(zip(self.detail_widget.labels.keys(), row))
                    self.detail_widget.update_data(data)

        random_button = QPushButton("Randomise")
        random_button.clicked.connect(load_random_item)
        layout.addWidget(random_button)

        load_random_item()

        return widget

    def create_discogs_importer_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        log_output = QTextEdit()
        log_output.setReadOnly(True)
        layout.addWidget(log_output)

        progress_bar = QProgressBar()
        progress_bar.setRange(0, 100)
        progress_bar.setValue(0)
        layout.addWidget(progress_bar)

        import_button = QPushButton("Import from Discogs")
        layout.addWidget(import_button)

        def run_import():
            try:
                client, access_token, access_secret = discogs_importer.connect_to_discogs(self.cfg)
            except Exception as e:
                log_output.append(f"Authentication failed: {e}")
                return

            import_button.setText("Cancel Import")

            self.thread = QThread()
            worker = CollectionViewer.DiscogsImportWorker(client)
            self.worker = worker  # keep reference
            worker.moveToThread(self.thread)

            worker.progress_msg.connect(lambda msg: log_output.append(msg))
            worker.finished.connect(lambda: import_button.setText("Import from Discogs"))
            worker.finished.connect(self.thread.quit)
            worker.finished.connect(worker.deleteLater)
            self.thread.finished.connect(self.thread.deleteLater)
            worker.progress.connect(progress_bar.setValue)

            self.thread.started.connect(worker.run)
            self.thread.start()

        def on_import_button_clicked():
            if import_button.text() == "Import from Discogs":
                run_import()
            else:
                if self.worker:
                    self.worker.cancel()
                    log_output.append("Cancelling import...")

        import_button.clicked.connect(on_import_button_clicked)
        return widget

    def create_musicbrainz_matcher_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        log_output = QTextEdit()
        log_output.setReadOnly(True)
        layout.addWidget(log_output)

        progress_bar = QProgressBar()
        progress_bar.setRange(0, 100)
        layout.addWidget(progress_bar)

        match_button = QPushButton("Match in MusicBrainz")
        layout.addWidget(match_button)

        # --- Helper functions to enable/disable all tabs and adjust Escape key ---
        def disable_tabs_and_escape():
            tab_widget = self.centralWidget()
            current_index = tab_widget.currentIndex()
            for i in range(tab_widget.count()):
                if i != current_index:
                    tab_widget.setTabEnabled(i, False)
            match_button.setEnabled(True)
            self.esc_shortcut.setEnabled(False)

        def enable_tabs_and_escape():
            tab_widget = self.centralWidget()
            for i in range(tab_widget.count()):
                tab_widget.setTabEnabled(i, True)
            self.esc_shortcut.setEnabled(True)

        class MBMatcherWorker(QObject):
            progress_msg = pyqtSignal(str)
            progress = pyqtSignal(int)
            finished = pyqtSignal()

            def __init__(self, cfg):
                super().__init__()
                self.cfg = cfg
                self._cancel_requested = False

            def cancel(self):
                self._cancel_requested = True

            def run(self):
                try:
                    musicbrainzngs.set_useragent(
                        app=self.cfg.user_agent, version=self.cfg.app_version)
                    musicbrainzngs.auth(self.cfg.username, self.cfg.password)
                    musicbrainzngs.set_rate_limit(1, 1)

                    mb_matcher.match_discogs_against_mb(
                        callback=self.progress_msg.emit,
                        should_cancel=lambda: self._cancel_requested,
                        progress_callback=lambda pct: self.progress.emit(pct)
                    )
                except Exception as e:
                    self.progress_msg.emit(f"Error: {e}")
                self.finished.emit()

        def run_match():
            import_button_label = "Match in MusicBrainz"
            cancel_button_label = "Cancel match"

            if match_button.text() == import_button_label:
                creds = db_musicbrainz.get_credentials()
                if creds:
                    username, password = creds
                    try:
                        musicbrainzngs.set_useragent(app=self.cfg.user_agent,
                                                     version=self.cfg.app_version)
                        musicbrainzngs.auth(username, password)
                    except Exception:
                        creds = None

                if not creds:
                    dlg = mb_auth_gui.MBAuthDialog()
                    if dlg.exec() == QDialog.DialogCode.Accepted:
                        username, password = dlg.get_credentials()
                        db_musicbrainz.set_credentials(username, password)
                    else:
                        return

                cfg = SimpleNamespace()
                cfg.user_agent = self.cfg.user_agent
                cfg.app_version = self.cfg.app_version
                cfg.username = username
                cfg.password = password

                self.mb_thread = QThread()
                worker = MBMatcherWorker(cfg)
                self.mb_worker = worker
                worker.moveToThread(self.mb_thread)

                worker.progress_msg.connect(lambda msg: log_output.append(msg))
                worker.progress.connect(progress_bar.setValue)
                worker.finished.connect(self.mb_thread.quit)
                worker.finished.connect(worker.deleteLater)
                self.mb_thread.finished.connect(self.mb_thread.deleteLater)
                worker.finished.connect(lambda: match_button.setText(import_button_label))
                # Enable tabs and Escape when match is finished
                worker.finished.connect(enable_tabs_and_escape)

                self.mb_thread.started.connect(worker.run)
                self.mb_thread.start()

                match_button.setText(cancel_button_label)
                # Disable tabs and Escape when match is started
                disable_tabs_and_escape()

            else:
                if self.mb_worker:
                    self.mb_worker.cancel()
                    log_output.append("Cancelling match...")
                    enable_tabs_and_escape()

        match_button.clicked.connect(run_match)
        return widget


def run_gui(cfg: AppConfig) -> None:
    app = QApplication(sys.argv)
    viewer = CollectionViewer(cfg)
    sys.exit(app.exec())
