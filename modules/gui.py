from PyQt6.QtWidgets import QDialog, QVBoxLayout, QLineEdit, QComboBox, QDialogButtonBox
from modules import db, utils
import musicbrainz.db_musicbrainz as db_musicbrainz
from modules.config import AppConfig
from musicbrainz import mb_matcher, mb_auth_gui
from discogs import discogs_importer
from PyQt6.QtWidgets import (
    QApplication, QLabel, QWidget, QVBoxLayout, QMainWindow, QTabWidget, QTextEdit, QTableWidget, QTableWidgetItem,
    QLineEdit, QHBoxLayout, QPushButton, QFormLayout, QGroupBox, QProgressBar, QDialog, QCheckBox
)
from PyQt6.QtGui import QKeySequence, QShortcut
from PyQt6.QtCore import Qt
from PyQt6.QtCore import QObject, pyqtSignal, QThread

import os

import musicbrainzngs
import sys
from types import SimpleNamespace

# Helper function to check if running under debugger


def is_debugging():
    return hasattr(sys, 'gettrace') and sys.gettrace()


# Set DEBUG_MODE once at module load for easy access throughout the app
DEBUG_MODE = is_debugging()


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


# --- ReleaseDateEditDialog for editing release date in Collection tab ---


# Redesigned ReleaseDateEditDialog with dynamic input controls
class ReleaseDateEditDialog(QDialog):
    def __init__(self, initial_date, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Release Date")
        self.resize(350, 250)

        layout = QVBoxLayout(self)

        self.date_type_combo = QComboBox()
        self.date_type_combo.addItems([
            "Full Date (YYYY-MM-DD)",
            "Month and Year (YYYY-MM)",
            "Year Only (YYYY)"
        ])
        layout.addWidget(self.date_type_combo)

        # Calendar for full date selection
        from PyQt6.QtWidgets import QCalendarWidget
        self.calendar = QCalendarWidget()
        # Restrict calendar to 1900-01-01 .. 2100-12-31
        from PyQt6.QtCore import QDate
        self.calendar.setMinimumDate(QDate(1900, 1, 1))
        self.calendar.setMaximumDate(QDate(2100, 12, 31))
        layout.addWidget(self.calendar)

        # Month and Year dropdowns
        self.month_combo = QComboBox()
        self.year_combo = QComboBox()
        for year in range(1900, 2101):
            self.year_combo.addItem(str(year))
        for month in range(1, 13):
            self.month_combo.addItem(f"{month:02}")
        # Group Year and Month dropdowns horizontally
        self.year_month_layout = QHBoxLayout()
        self.year_month_layout.addWidget(self.year_combo)
        self.year_month_layout.addWidget(self.month_combo)
        self.year_combo.hide()
        self.month_combo.hide()
        layout.addLayout(self.year_month_layout)

        # Year only dropdown
        self.year_only_combo = QComboBox()
        for year in range(1900, 2101):
            self.year_only_combo.addItem(str(year))
        self.year_only_combo.hide()
        layout.addWidget(self.year_only_combo)

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok |
                                      QDialogButtonBox.StandardButton.Cancel)
        layout.addWidget(button_box)

        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)

        self.date_type_combo.currentIndexChanged.connect(self.update_visible_widgets)

        # Initialize the controls based on initial_date
        self.init_with_date(initial_date)

    def update_visible_widgets(self):
        mode = self.date_type_combo.currentIndex()

        from PyQt6.QtCore import QDate

        # If switching into Full Date mode, prefill the calendar
        if mode == 0:
            if self.year_only_combo.isVisible():
                year = int(self.year_only_combo.currentText())
                self.calendar.setSelectedDate(QDate(year, 1, 1))  # Jan 1st
            elif self.year_combo.isVisible() and self.month_combo.isVisible():
                year = int(self.year_combo.currentText())
                month = int(self.month_combo.currentText())
                self.calendar.setSelectedDate(QDate(year, month, 1))  # 1st of selected month

        # Preserve year selection when switching from Year Only to Month-Year
        if mode == 1 and self.year_only_combo.isVisible():
            self.year_combo.setCurrentText(self.year_only_combo.currentText())

        self.calendar.setVisible(mode == 0)
        self.year_combo.setVisible(mode == 1)
        self.month_combo.setVisible(mode == 1)
        self.year_only_combo.setVisible(mode == 2)

    def init_with_date(self, initial_date):
        if len(initial_date) == 10 and '-' in initial_date:
            self.date_type_combo.setCurrentIndex(0)
            try:
                from PyQt6.QtCore import QDate
                year, month, day = map(int, initial_date.split('-'))
                self.calendar.setSelectedDate(QDate(year, month, day))
            except Exception:
                pass
        elif len(initial_date) == 7 and '-' in initial_date:
            self.date_type_combo.setCurrentIndex(1)
            year, month = initial_date.split('-')
            self.year_combo.setCurrentText(year)
            self.month_combo.setCurrentText(month)
        elif len(initial_date) == 4:
            self.date_type_combo.setCurrentIndex(2)
            self.year_only_combo.setCurrentText(initial_date)
        else:
            self.date_type_combo.setCurrentIndex(0)

        self.update_visible_widgets()

    def get_date(self):
        mode = self.date_type_combo.currentIndex()
        if mode == 0:
            return self.calendar.selectedDate().toString("yyyy-MM-dd")
        elif mode == 1:
            return f"{self.year_combo.currentText()}-{self.month_combo.currentText()}"
        elif mode == 2:
            return self.year_only_combo.currentText()
        return ""


class CollectionViewer(QMainWindow):
    class DiscogsImportWorker(QObject):
        progress_msg = pyqtSignal(str)
        finished = pyqtSignal()
        progress = pyqtSignal(int)

        def __init__(self, client, cfg):
            super().__init__()
            self.client = client
            self.cfg = cfg
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
                    images_folder=self.cfg.images_folder,
                    callback=emit_msg,
                    should_cancel=lambda: self._cancel_requested,
                    progress_callback=lambda pct: self.progress.emit(pct)
                )
            except Exception as e:
                self.progress_msg.emit(f"Error: {e}")
            self.finished.emit()

    def __init__(self, cfg: AppConfig):
        super().__init__()
        self.setWindowTitle("GrooveKraft")
        self.cfg = cfg
        if not hasattr(self.cfg, "images_folder"):
            self.cfg.images_folder = os.path.join(self.cfg.root_folder, "images")
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
                'SELECT d.sort_name AS artist, d.title, d.format, d.country, d.release_date, d.discogs_id')
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

        # Set up table to match Collection tab structure
        table.setColumnCount(5)
        table.setHorizontalHeaderLabels(
            ['Thumbnail', 'Details', 'Release Date', 'Discogs Id', 'Matched'])
        table.setRowCount(len(rows))
        table.verticalHeader().setDefaultSectionSize(110)

        for row_idx, (artist, title, format, country, release_date, discogs_id) in enumerate(rows):
            # Column 0: Thumbnail
            image_path = os.path.join(self.cfg.images_folder, f"{discogs_id}.jpg")
            if os.path.exists(image_path):
                from PyQt6.QtGui import QPixmap
                pixmap = QPixmap(image_path)
                pixmap = pixmap.scaled(100, 100, Qt.AspectRatioMode.KeepAspectRatio,
                                       Qt.TransformationMode.SmoothTransformation)
                thumbnail_item = QTableWidgetItem()
                thumbnail_item.setData(Qt.ItemDataRole.DecorationRole, pixmap)
                thumbnail_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                table.setItem(row_idx, 0, thumbnail_item)

            # Column 1: Details (QLabel with HTML, similar to Collection tab)
            details_html = f"<b>{title}</b><br>{artist}<br>{format}<br>{country}"
            details_label = QLabel()
            details_label.setText(details_html)
            details_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            details_label.setWordWrap(True)
            details_label.setContentsMargins(10, 0, 10, 0)
            table.setCellWidget(row_idx, 1, details_label)

            # Column 2: Release Date (humanized, bold + delta)
            release_text = f"<b>{utils.parse_and_humanize_date(release_date)}</b><br>{utils.humanize_date_delta(release_date)}"
            release_label = QLabel()
            release_label.setText(release_text)
            release_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            release_label.setWordWrap(True)
            release_label.setContentsMargins(10, 0, 10, 0)
            table.setCellWidget(row_idx, 2, release_label)

            # Column 3: Discogs Id (right aligned)
            table.setItem(row_idx, 3, QTableWidgetItem(str(discogs_id)))
            table.item(row_idx, 3).setTextAlignment(
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            # Column 4: Matched (show stars)
            mb_row = db_musicbrainz.fetch_row(discogs_id=discogs_id)
            score = mb_row.score if mb_row and mb_row.score is not None else 0
            match_star = mb_matcher.score_stars(score)
            match_item = QTableWidgetItem(match_star)
            match_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            table.setItem(row_idx, 4, match_item)

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

            # Update table columns and headers to new format
            table.setColumnCount(5)
            table.setHorizontalHeaderLabels(
                ['Artwork', 'Details', 'Release Date', 'Discogs Id', 'Matched'])
            table.setRowCount(len(rows))
            # Set row height to fit thumbnails
            table.verticalHeader().setDefaultSectionSize(110)

            for row_idx, (sort_name, artist, title, format, country, release_date, discogs_id, mbid) in enumerate(rows):
                # Column 0: Thumbnail
                image_path = os.path.join(self.cfg.images_folder, f"{discogs_id}.jpg")
                if os.path.exists(image_path):
                    from PyQt6.QtGui import QPixmap
                    pixmap = QPixmap(image_path)
                    pixmap = pixmap.scaled(100, 100, Qt.AspectRatioMode.KeepAspectRatio,
                                           Qt.TransformationMode.SmoothTransformation)
                    thumbnail_item = QTableWidgetItem()
                    thumbnail_item.setData(Qt.ItemDataRole.DecorationRole, pixmap)
                    thumbnail_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    table.setItem(row_idx, 0, thumbnail_item)

                # Column 1: Details (QLabel with HTML)
                details_html = f"<b>{title}</b><br>{artist}<br>{format}<br>{country}"
                details_label = QLabel()
                details_label.setText(details_html)
                details_label.setAlignment(Qt.AlignmentFlag.AlignLeft |
                                           Qt.AlignmentFlag.AlignVCenter)
                details_label.setWordWrap(True)
                details_label.setContentsMargins(10, 0, 10, 0)
                table.setCellWidget(row_idx, 1, details_label)

                # Column 2: Release Date (QLabel, bold humanized + delta)
                release_text = f"<b>{utils.parse_and_humanize_date(release_date)}</b><br>{utils.humanize_date_delta(release_date)}"
                release_label = QLabel()
                release_label.setText(release_text)
                release_label.setAlignment(Qt.AlignmentFlag.AlignLeft |
                                           Qt.AlignmentFlag.AlignVCenter)
                release_label.setWordWrap(True)
                release_label.setContentsMargins(10, 0, 10, 0)
                table.setCellWidget(row_idx, 2, release_label)

                # Column 3: Discogs Id
                table.setItem(row_idx, 3, QTableWidgetItem(str(discogs_id)))
                table.item(row_idx, 3).setTextAlignment(
                    Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

                # Column 4: Matched (centered)
                if mbid:
                    mb_row = db_musicbrainz.fetch_row(discogs_id=discogs_id)
                    score = mb_row.score if mb_row and mb_row.score is not None else 0
                else:
                    score = 0

                match_star = mb_matcher.score_stars(score)
                match_item = QTableWidgetItem(match_star)
                match_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                table.setItem(row_idx, 4, match_item)

            table.resizeColumnsToContents()

            # Hook up double-click for editing release date
            table.cellDoubleClicked.connect(lambda row, col: on_table_double_click(row, col, table))

        # Define double-click handler for release date edit
        def on_table_double_click(row, column, table):
            if column == 2:  # Release Date column
                label = table.cellWidget(row, column)
                if label:
                    current_text = label.text()
                    # Extract the date part from HTML
                    import re
                    match = re.search(r"<b>(.*?)</b>", current_text)
                    if match:
                        initial_date = match.group(1)
                    else:
                        initial_date = ""

                    dialog = ReleaseDateEditDialog(initial_date)
                    if dialog.exec() == QDialog.DialogCode.Accepted:
                        new_date = dialog.get_date()
                        if new_date:
                            # Update database
                            discogs_id_item = table.item(row, 3)
                            if discogs_id_item:
                                discogs_id = int(discogs_id_item.text())
                                with db.context_manager() as cur:
                                    cur.execute(
                                        "UPDATE discogs_releases SET release_date = ? WHERE discogs_id = ?",
                                        (new_date, discogs_id)
                                    )
                                db.commit()

                            # Update table visually
                            label.setText(
                                f"<b>{utils.parse_and_humanize_date(new_date)}</b><br>{utils.humanize_date_delta(new_date)}")

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
        main_layout = QVBoxLayout(widget)

        # Title centered at the very top
        title_label = QLabel("🎲 Random Release")
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setStyleSheet("font-size: 18px; font-weight: bold; margin-bottom: 10px;")
        main_layout.addWidget(title_label)

        layout = QHBoxLayout()
        main_layout.addLayout(layout)

        # Left side (Image only)
        left_layout = QVBoxLayout()

        self.image_label = QLabel()
        self.image_label.setFixedSize(400, 400)
        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        left_layout.addWidget(self.image_label)

        layout.addLayout(left_layout)

        # Right side (Details only)
        right_layout = QVBoxLayout()

        self.detail_widget = ReleaseDetailWidget()
        right_layout.addWidget(self.detail_widget)

        # random_button will be moved outside right_layout
        random_button = QPushButton("🎲 Randomise")
        # Improved appearance: button with rounded corners, blue background, white text, and hover effect
        random_button.setStyleSheet("""
            QPushButton {
                font-size: 16px;
                padding: 10px 20px;
                border: 2px solid #3498db;
                border-radius: 10px;
                background-color: #2980b9;
                color: white;
            }
            QPushButton:hover {
                background-color: #3498db;
            }
        """)
        # right_layout.addWidget(random_button)  # Removed from right_layout

        wrapper_layout = QVBoxLayout()
        wrapper_layout.addStretch()
        wrapper_layout.addLayout(right_layout)
        wrapper_layout.addStretch()

        layout.addLayout(wrapper_layout)

        # Add Randomise button centered at the bottom
        random_button_layout = QHBoxLayout()
        random_button_layout.addStretch()
        random_button_layout.addWidget(random_button)
        random_button_layout.addStretch()
        main_layout.addLayout(random_button_layout)

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

                    # Load image
                    discogs_id = row.discogs_id
                    image_path = os.path.join(self.cfg.images_folder, f"{discogs_id}.jpg")
                    if os.path.exists(image_path):
                        from PyQt6.QtGui import QPixmap
                        pixmap = QPixmap(image_path)
                        pixmap = pixmap.scaled(self.image_label.width(), self.image_label.height(
                        ), Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                        self.image_label.setPixmap(pixmap)
                    else:
                        self.image_label.clear()

        random_button.clicked.connect(load_random_item)
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
        import_button.setStyleSheet("""
            QPushButton {
                font-size: 16px;
                padding: 10px 20px;
                border: 2px solid #3498db;
                border-radius: 10px;
                background-color: #2980b9;
                color: white;
            }
            QPushButton:hover {
                background-color: #3498db;
            }
        """)
        layout.addWidget(import_button)

        # --- Helper functions to enable/disable all tabs and adjust Escape key ---
        def disable_tabs_and_escape():
            tab_widget = self.centralWidget()
            current_index = tab_widget.currentIndex()
            for i in range(tab_widget.count()):
                if i != current_index:
                    tab_widget.setTabEnabled(i, False)
            import_button.setEnabled(True)
            self.esc_shortcut.activated.disconnect()
            self.esc_shortcut.activated.connect(lambda: on_import_button_clicked(cancel=True))

        def enable_tabs_and_escape():
            tab_widget = self.centralWidget()
            for i in range(tab_widget.count()):
                tab_widget.setTabEnabled(i, True)
            self.esc_shortcut.activated.disconnect()
            self.esc_shortcut.activated.connect(self.close)

        def run_import():
            log_output.clear()
            try:
                client, access_token, access_secret = discogs_importer.connect_to_discogs(self.cfg)
            except Exception as e:
                log_output.append(f"Authentication failed: {e}")
                return

            import_button.setText("Cancel Import")

            self.thread = QThread()
            worker = CollectionViewer.DiscogsImportWorker(client, self.cfg)
            self.worker = worker  # keep reference
            worker.moveToThread(self.thread)

            worker.progress_msg.connect(lambda msg: log_output.append(msg))

            def restore_import_button():
                import_button.setText("Import from Discogs")
                import_button.setEnabled(True)
                import_button.setStyleSheet("")
                enable_tabs_and_escape()
            worker.finished.connect(restore_import_button)
            worker.finished.connect(self.thread.quit)
            worker.finished.connect(worker.deleteLater)
            self.thread.finished.connect(self.thread.deleteLater)
            worker.progress.connect(progress_bar.setValue)

            self.thread.started.connect(worker.run)
            self.thread.start()

            # Disable tabs and Escape when import is started
            disable_tabs_and_escape()

        def on_import_button_clicked(cancel=False):
            if import_button.text() == "Import from Discogs" and not cancel:
                run_import()
            else:
                if self.worker:
                    self.worker.cancel()
                    log_output.append("Cancelling import...")
                    import_button.setText("Cancelling...")
                    import_button.setEnabled(False)
                    import_button.setStyleSheet("color: gray; font-style: italic;")

        import_button.clicked.connect(lambda: on_import_button_clicked())
        return widget

    def create_musicbrainz_matcher_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # Add Match All checkbox at the very start, centered horizontally
        match_all_checkbox = QCheckBox("Match all items")
        match_all_layout = QHBoxLayout()
        match_all_layout.addStretch()
        match_all_layout.addWidget(match_all_checkbox)
        match_all_layout.addStretch()
        layout.addLayout(match_all_layout)

        log_output = QTextEdit()
        log_output.setReadOnly(True)
        layout.addWidget(log_output)

        progress_bar = QProgressBar()
        progress_bar.setRange(0, 100)
        layout.addWidget(progress_bar)

        match_button = QPushButton("Match in MusicBrainz")
        match_button.setStyleSheet("""
            QPushButton {
                font-size: 16px;
                padding: 10px 20px;
                border: 2px solid #3498db;
                border-radius: 10px;
                background-color: #2980b9;
                color: white;
            }
            QPushButton:hover {
                background-color: #3498db;
            }
        """)
        layout.addWidget(match_button)

        # --- Helper functions to enable/disable all tabs and adjust Escape key ---
        def disable_tabs_and_escape():
            tab_widget = self.centralWidget()
            current_index = tab_widget.currentIndex()
            for i in range(tab_widget.count()):
                if i != current_index:
                    tab_widget.setTabEnabled(i, False)
            match_button.setEnabled(True)
            self.esc_shortcut.activated.disconnect()
            self.esc_shortcut.activated.connect(lambda: run_match(cancel=True))

        def enable_tabs_and_escape():
            tab_widget = self.centralWidget()
            for i in range(tab_widget.count()):
                tab_widget.setTabEnabled(i, True)
            self.esc_shortcut.activated.disconnect()
            self.esc_shortcut.activated.connect(self.close)

        class MBMatcherWorker(QObject):
            progress_msg = pyqtSignal(str)
            progress = pyqtSignal(int)
            finished = pyqtSignal()

            def __init__(self, cfg):
                super().__init__()
                self.cfg = cfg
                self._cancel_requested = False
                self.match_all = cfg.match_all

            def cancel(self):
                self._cancel_requested = True

            def run(self):
                if is_debugging():
                    try:
                        import debugpy
                        debugpy.debug_this_thread()
                    except ImportError:
                        pass

                try:
                    musicbrainzngs.set_useragent(
                        app=self.cfg.user_agent, version=self.cfg.app_version)
                    musicbrainzngs.auth(self.cfg.username, self.cfg.password)
                    musicbrainzngs.set_rate_limit(1, 1)

                    mb_matcher.match_discogs_against_mb(
                        callback=self.progress_msg.emit,
                        should_cancel=lambda: self._cancel_requested,
                        progress_callback=lambda pct: self.progress.emit(pct),
                        match_all=self.match_all
                    )
                except Exception as e:
                    self.progress_msg.emit(f"Error: {e}")
                self.finished.emit()

        def run_match(cancel=False):
            import_button_label = "Match in MusicBrainz"
            cancel_button_label = "Cancel match"

            if match_button.text() == import_button_label:
                log_output.clear()
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
                cfg.match_all = match_all_checkbox.isChecked()

                worker = MBMatcherWorker(cfg)
                self.mb_worker = worker

                # Insert debugging check: if debugging, run worker.run() directly; else, use QThread
                if is_debugging():
                    # Disable tabs and Escape when match is started
                    match_button.setText(cancel_button_label)
                    disable_tabs_and_escape()
                    # Connect signals (simulate, since no thread)
                    worker.progress_msg.connect(lambda msg: log_output.append(msg))
                    worker.progress.connect(progress_bar.setValue)

                    def restore_button():
                        match_button.setText(import_button_label)
                        match_button.setEnabled(True)
                        match_button.setStyleSheet("")
                    worker.finished.connect(restore_button)
                    worker.finished.connect(enable_tabs_and_escape)
                    worker.run()
                else:
                    self.mb_thread = QThread()
                    worker.moveToThread(self.mb_thread)

                    worker.progress_msg.connect(lambda msg: log_output.append(msg))
                    worker.progress.connect(progress_bar.setValue)
                    worker.finished.connect(self.mb_thread.quit)
                    worker.finished.connect(worker.deleteLater)
                    self.mb_thread.finished.connect(self.mb_thread.deleteLater)

                    def restore_button():
                        match_button.setText(import_button_label)
                        match_button.setEnabled(True)
                        match_button.setStyleSheet("")
                    worker.finished.connect(restore_button)
                    worker.finished.connect(enable_tabs_and_escape)

                    self.mb_thread.started.connect(worker.run)
                    self.mb_thread.start()

                    match_button.setText(cancel_button_label)
                    disable_tabs_and_escape()

            else:
                if self.mb_worker:
                    self.mb_worker.cancel()
                    log_output.append("Cancelling match...")
                    match_button.setText("Cancelling...")
                    match_button.setEnabled(False)
                    match_button.setStyleSheet("color: gray; font-style: italic;")

        match_button.clicked.connect(run_match)
        return widget


def run_gui(cfg: AppConfig) -> None:
    app = QApplication(sys.argv)
    viewer = CollectionViewer(cfg)
    sys.exit(app.exec())
