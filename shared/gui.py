from PyQt6.QtWidgets import QDialog, QVBoxLayout, QLineEdit, QComboBox, QDialogButtonBox
from shared.utils import is_today_anniversary, is_month_anniversary, parse_and_humanize_date, humanize_date_delta
from shared.db import context_manager
import musicbrainz.db_musicbrainz as db_musicbrainz
from shared.config import AppConfig, GROOVEKRAFT_USER_AGENT, GROOVEKRAFT_VERSION
from musicbrainz import mb_matcher, mb_auth_gui
from discogs import discogs_importer
from PyQt6.QtWidgets import (
    QApplication, QLabel, QWidget, QVBoxLayout, QMainWindow, QTabWidget, QTextEdit, QTableWidget, QTableWidgetItem,
    QLineEdit, QHBoxLayout, QPushButton, QFormLayout, QGroupBox, QProgressBar, QDialog, QCheckBox
)
from PyQt6.QtGui import QKeySequence, QShortcut, QIcon
from PyQt6.QtCore import Qt
from PyQt6.QtCore import QObject, pyqtSignal, QThread


import os

# Ensure SSL certificate bundle is available for HTTPS requests (e.g., MusicBrainz)
try:
    import certifi
    # Only set if not already provided by the environment
    if not os.environ.get('SSL_CERT_FILE'):
        os.environ['SSL_CERT_FILE'] = certifi.where()
    if not os.environ.get('REQUESTS_CA_BUNDLE'):
        os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
except Exception:
    # If certifi is missing, we proceed; connections may fail on systems without system CA store
    pass

import musicbrainzngs
import sys
from types import SimpleNamespace

# Helper function to check if running under debugger


def is_debugging():
    return hasattr(sys, 'gettrace') and sys.gettrace()


# Centralized button stylesheet for consistent look
def get_default_button_stylesheet():
    return """
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
    """


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


class ReleaseDateEditDialog(QDialog):
    def __init__(self, initial_date, parent=None, locked=False):
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

        # Add lock checkbox at the bottom
        self.lock_checkbox = QCheckBox("Lock this release date (prevent automatic updates)")
        self.lock_checkbox.setChecked(locked)
        layout.addWidget(self.lock_checkbox)

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
        from PyQt6.QtCore import QDate
        # Try to detect valid date forms
        valid = False
        if len(initial_date) == 10 and '-' in initial_date:
            try:
                year, month, day = map(int, initial_date.split('-'))
                qdate = QDate(year, month, day)
                if qdate.isValid():
                    self.date_type_combo.setCurrentIndex(0)
                    self.calendar.setSelectedDate(qdate)
                    valid = True
            except Exception:
                valid = False
        elif len(initial_date) == 7 and '-' in initial_date:
            try:
                year, month = initial_date.split('-')
                year = int(year)
                month = int(month)
                if 1900 <= year <= 2100 and 1 <= month <= 12:
                    self.date_type_combo.setCurrentIndex(1)
                    self.year_combo.setCurrentText(str(year))
                    self.month_combo.setCurrentText(f"{month:02}")
                    valid = True
            except Exception:
                valid = False
        elif len(initial_date) == 4:
            try:
                year = int(initial_date)
                if 1900 <= year <= 2100:
                    self.date_type_combo.setCurrentIndex(2)
                    self.year_only_combo.setCurrentText(str(year))
                    valid = True
            except Exception:
                valid = False
        if not valid:
            # If invalid, set to "Year Only" and leave year blank
            self.date_type_combo.setCurrentIndex(2)
            self.year_only_combo.setCurrentIndex(-1)
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

    def is_locked(self):
        return self.lock_checkbox.isChecked()


class CollectionViewer(QMainWindow):
    def refresh_views(self):
        if getattr(self, "populate_collection_table_fn", None):
            self.populate_collection_table_fn()
        if getattr(self, "populate_on_this_day_table_fn", None):
            self.populate_on_this_day_table_fn()

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
                    cfg=self.cfg,
                    callback=emit_msg,
                    should_cancel=lambda: self._cancel_requested,
                    progress_callback=lambda pct: self.progress.emit(pct)
                )
            except Exception as e:
                self.progress_msg.emit(f"Error: {e}")
            self.finished.emit()

    def __init__(self, cfg: AppConfig):
        super().__init__()
        self.cfg = cfg
        self.setWindowTitle(f"{self.cfg.app_name} v{self.cfg.app_version}")
        if not hasattr(self.cfg, "images_folder"):
            self.cfg.images_folder = os.path.join(self.cfg.root_folder, "images")
        self.setMinimumSize(800, 600)
        tab_widget = QTabWidget()

        # Initialize references for later use
        self.collection_table = None
        self.on_this_day_table = None
        self.populate_collection_table_fn = None
        self.populate_on_this_day_table_fn = None

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
        self.tab_widget = tab_widget  # Store reference for later

        def handle_tab_changed(idx):
            if self.tab_widget.tabText(idx) == "On this day" and self.populate_on_this_day_table_fn:
                self.populate_on_this_day_table_fn()
        tab_widget.currentChanged.connect(handle_tab_changed)
        self.esc_shortcut = QShortcut(QKeySequence("Escape"), self)
        self.esc_shortcut.activated.connect(self.close)
        self.show()

    def create_on_this_day_tab(self):
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)

        # --- Date header with navigation ---
        from PyQt6.QtCore import QDate
        # Outer layout to center a fixed-width container
        outer_header_layout = QHBoxLayout()

        # Inner container that will be centered and constrained to half the parent width
        header_container = QWidget()
        header_layout = QHBoxLayout(header_container)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(8)

        prev_btn = QPushButton("◀")
        next_btn = QPushButton("▶")

        date_label = QLabel()
        date_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        date_label.setStyleSheet("font-size: 18px; font-weight: bold; margin: 6px 0;")

        # Extra controls: Today + Pick (single set)
        from PyQt6.QtCore import QSize
        from PyQt6.QtWidgets import QStyle

        today_btn = QPushButton()
        pick_btn = QPushButton()

        today_btn.setToolTip("Today")
        pick_btn.setToolTip("Pick date")

        # Try to use theme icons, fallback to standard pixmaps
        icon_today = QIcon.fromTheme("view-calendar-today")
        if icon_today.isNull():
            icon_today = self.style().standardIcon(QStyle.StandardPixmap.SP_BrowserReload)
        today_btn.setIcon(icon_today)
        today_btn.setIconSize(QSize(18, 18))

        icon_pick = QIcon.fromTheme("x-office-calendar")
        if icon_pick.isNull():
            icon_pick = self.style().standardIcon(QStyle.StandardPixmap.SP_DialogOpenButton)
        pick_btn.setIcon(icon_pick)
        pick_btn.setIconSize(QSize(18, 18))

        # Unified nav button look (applies to ◀ ▶ Today Pick)
        btn_style = (
            "QPushButton {"
            "  font-size: 16px;"
            "  padding: 4px 8px;"
            "  min-height: 28px;"
            "  border: 1px solid #A9A9A9;"
            "  border-radius: 6px;"
            "  background: #F2F2F2;"
            "}"
            "QPushButton:hover { background: #E6E6E6; }"
            "QPushButton:pressed { background: #DCDCDC; }"
        )
        for btn in (prev_btn, next_btn, today_btn, pick_btn):
            btn.setFixedWidth(40)
            btn.setStyleSheet(btn_style)
            btn.setFlat(False)

        # Track just month and day (year is irrelevant). Use a leap-year anchor only for formatting.
        today = QDate.currentDate()
        current_month = today.month()
        current_day = today.day()

        def update_date_label():
            # Display only day and month, e.g. "10 August"
            date_label.setText(QDate(2000, current_month, current_day).toString("d MMMM"))

        # Buttons at the ends, date centered within the inner container
        header_layout.addWidget(prev_btn)
        header_layout.addWidget(today_btn)
        header_layout.addStretch(1)
        header_layout.addWidget(date_label)
        header_layout.addStretch(1)
        header_layout.addWidget(pick_btn)
        header_layout.addWidget(next_btn)

        # Center the inner container and make it half the width of the parent widget
        outer_header_layout.addStretch(1)
        outer_header_layout.addWidget(header_container)
        outer_header_layout.addStretch(1)
        layout.addLayout(outer_header_layout)

        # Helper to keep header_container at ~50% of the parent width (with a sensible minimum)
        from PyQt6.QtCore import QEvent

        def _update_header_width():
            try:
                header_container.setFixedWidth(max(360, widget.width() // 2))
            except Exception:
                pass
        _update_header_width()

        # Install an event filter to react to resize events and keep width at 50%
        class _HeaderSizer(QObject):
            def eventFilter(self, obj, event):
                if obj is widget and event.type() == QEvent.Type.Resize:
                    _update_header_width()
                return False
        self._on_this_day_header_sizer = _HeaderSizer()
        widget.installEventFilter(self._on_this_day_header_sizer)

        # Days per month with Feb=29 to allow Feb 29 selection
        DAYS_IN_MONTH = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31,
                         6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

        def goto_today():
            nonlocal current_month, current_day
            t = QDate.currentDate()
            current_month, current_day = t.month(), t.day()
            populate_on_this_day_table()

        def open_pick_dialog():
            nonlocal current_month, current_day
            dlg = QDialog(self)
            dlg.setWindowTitle("Select day and month")
            lay = QVBoxLayout(dlg)

            form = QFormLayout()
            month_combo = QComboBox()
            # Month names 1..12
            month_names = [QDate(2000, m, 1).toString("MMMM") for m in range(1, 13)]
            month_combo.addItems(month_names)
            day_combo = QComboBox()

            def refill_days(m):
                day_combo.clear()
                for d in range(1, DAYS_IN_MONTH[m] + 1):
                    day_combo.addItem(str(d))

            # init with current
            month_combo.setCurrentIndex(current_month - 1)
            refill_days(current_month)
            # clamp current_day to available days in month
            d0 = min(current_day, DAYS_IN_MONTH[current_month])
            day_combo.setCurrentIndex(d0 - 1)

            def on_month_changed(idx):
                m = idx + 1
                prev_day = int(day_combo.currentText()) if day_combo.currentText() else 1
                refill_days(m)
                day_combo.setCurrentIndex(min(prev_day, DAYS_IN_MONTH[m]) - 1)
            month_combo.currentIndexChanged.connect(on_month_changed)

            form.addRow("Month:", month_combo)
            form.addRow("Day:", day_combo)
            lay.addLayout(form)

            buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok |
                                       QDialogButtonBox.StandardButton.Cancel)
            lay.addWidget(buttons)
            buttons.accepted.connect(dlg.accept)
            buttons.rejected.connect(dlg.reject)

            if dlg.exec() == QDialog.DialogCode.Accepted:
                current_month = month_combo.currentIndex() + 1
                current_day = int(day_combo.currentText())
                populate_on_this_day_table()

        update_date_label()

        table = QTableWidget()
        layout.addWidget(table)
        self.on_this_day_table = table

        # The logic for populating the table
        def populate_on_this_day_table():
            # placeholder filters
            find_filter = ''
            format_filter = ''

            with context_manager(self.cfg.db_path, namedtuple=False) as cur:
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

            # Filter rows using the currently selected month/day only
            from PyQt6.QtCore import QDate
            target_month = current_month
            target_day = current_day
            update_date_label()

            rows = []
            for item in items:
                rd = item["release_date"]
                include = False
                if rd:
                    if len(rd) == 10 and '-' in rd:
                        # YYYY-MM-DD: match month & day with target
                        try:
                            y, m, d = map(int, rd.split('-'))
                            qd = QDate(y, m, d)
                            if qd.isValid() and m == target_month and d == target_day:
                                include = True
                        except Exception:
                            include = False
                    elif len(rd) == 7 and '-' in rd:
                        # YYYY-MM: match month with target
                        try:
                            y, m = rd.split('-')
                            y = int(y)
                            m = int(m)
                            if 1 <= m <= 12 and 1900 <= y <= 2100 and m == target_month:
                                include = True
                        except Exception:
                            include = False
                if include:
                    rows.append(item)

            # Set up table to match Collection tab structure
            table.setColumnCount(5)
            table.setHorizontalHeaderLabels(
                ['Thumbnail', 'Details', 'Release Date', 'Discogs Id', 'Matched'])
            table.setRowCount(len(rows))
            table.verticalHeader().setDefaultSectionSize(110)

            for row_idx, item in enumerate(rows):
                artist = item["artist"]
                title = item["title"]
                format = item["format"]
                country = item["country"]
                release_date = item["release_date"]
                discogs_id = item["discogs_id"]
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
                details_label.setAlignment(Qt.AlignmentFlag.AlignLeft |
                                           Qt.AlignmentFlag.AlignVCenter)
                details_label.setWordWrap(True)
                details_label.setContentsMargins(10, 0, 10, 0)
                table.setCellWidget(row_idx, 1, details_label)

                # Column 2: Release Date (two lines, no wrapping within a line)
                release_text = f"<b>{parse_and_humanize_date(release_date)}</b><br>{humanize_date_delta(release_date)}"
                release_label = QLabel()
                release_label.setText(release_text)
                release_label.setAlignment(Qt.AlignmentFlag.AlignLeft |
                                           Qt.AlignmentFlag.AlignVCenter)
                release_label.setWordWrap(False)
                release_label.setContentsMargins(10, 0, 10, 0)
                table.setCellWidget(row_idx, 2, release_label)

                # Column 3: Discogs Id (right aligned)
                table.setItem(row_idx, 3, QTableWidgetItem(str(discogs_id)))
                table.item(row_idx, 3).setTextAlignment(
                    Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

                # Column 4: Matched (show stars)
                mb_row = db_musicbrainz.fetch_row(self.cfg.db_path, discogs_id=discogs_id)
                score = mb_row.score if mb_row and mb_row.score is not None else 0
                match_star = mb_matcher.score_stars(score)
                match_item = QTableWidgetItem(match_star)
                match_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                table.setItem(row_idx, 4, match_item)

            table.resizeColumnsToContents()

        # Navigation handlers: move selected month/day (no year) and repopulate
        def goto_prev_day():
            nonlocal current_month, current_day
            days_in_month = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31,
                             6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
            if current_day > 1:
                current_day -= 1
            else:
                # move to previous month
                if current_month == 1:
                    current_month = 12
                else:
                    current_month -= 1
                current_day = days_in_month[current_month]
            populate_on_this_day_table()

        def goto_next_day():
            nonlocal current_month, current_day
            days_in_month = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31,
                             6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
            if current_day < days_in_month[current_month]:
                current_day += 1
            else:
                # move to next month
                if current_month == 12:
                    current_month = 1
                else:
                    current_month += 1
                current_day = 1
            populate_on_this_day_table()

        prev_btn.clicked.connect(goto_prev_day)
        next_btn.clicked.connect(goto_next_day)
        today_btn.clicked.connect(goto_today)
        pick_btn.clicked.connect(open_pick_dialog)

        # Initial population
        populate_on_this_day_table()
        # Store for refresh_views
        self.populate_on_this_day_table_fn = populate_on_this_day_table
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

        # Year From Filter
        filter_layout.addWidget(QLabel("Year from:"))
        year_from_input = QLineEdit()
        year_from_input.setPlaceholderText("e.g. 1975")
        filter_layout.addWidget(year_from_input)

        # Year To Filter
        filter_layout.addWidget(QLabel("Year to:"))
        year_to_input = QLineEdit()
        year_to_input.setPlaceholderText("e.g. 1986")
        filter_layout.addWidget(year_to_input)

        clear_button = QPushButton("Clear Filters")
        filter_layout.addWidget(clear_button)

        # Table
        table = QTableWidget()
        main_layout.addWidget(table)
        self.collection_table = table

        from PyQt6.QtCore import QTimer
        # Debounce filter changes using a single-shot QTimer
        filter_timer = QTimer()
        filter_timer.setSingleShot(True)
        filter_timer.setInterval(500)

        # Track last filter values to avoid redundant refreshes
        last_filter_values = None

        def populate_table():
            nonlocal resize_done, last_filter_values
            current_filters = {
                "artist": artist_input.text(),
                "title": title_input.text(),
                "format": format_input.text(),
                "year_from": year_from_input.text(),
                "year_to": year_to_input.text()
            }
            if last_filter_values is not None and current_filters == last_filter_values:
                return  # No real change, skip repopulating
            last_filter_values = current_filters
            resize_done = False
            table.setUpdatesEnabled(False)
            with context_manager(self.cfg.db_path) as cur:
                query = []
                params = []

                # Now also fetch d.release_date_locked
                query.append(
                    "SELECT d.sort_name, d.artist, d.title, d.format, d.country, d.release_date, d.release_date_locked, d.discogs_id, m.mbid")
                query.append("FROM discogs_releases d")
                query.append("LEFT JOIN mb_matches m USING(discogs_id)")
                filters = []

                if artist_input.text():
                    filters.append('d.sort_name LIKE ?')
                    params.append(f"%{artist_input.text()}%")
                if title_input.text():
                    filters.append('d.title LIKE ?')
                    params.append(f"%{title_input.text()}%")
                if format_input.text():
                    filters.append('d.format LIKE ?')
                    params.append(f"%{format_input.text()}%")
                if year_from_input.text():
                    filters.append('substr(d.release_date, 1, 4) >= ?')
                    params.append(year_from_input.text())
                if year_to_input.text():
                    filters.append('substr(d.release_date, 1, 4) <= ?')
                    params.append(year_to_input.text())

                if filters:
                    query.append("WHERE " + " AND ".join(filters))

                query.append("ORDER BY d.sort_name, d.release_date, d.title, d.discogs_id")
                cur.execute(' '.join(query), params)
                rows = cur.fetchall()

            # Update table columns and headers to new format
            table.setColumnCount(5)
            table.setHorizontalHeaderLabels(
                ['Artwork', 'Details', 'Release Date', 'Discogs Id', 'Matched'])
            table.setRowCount(len(rows))
            # Set row height to fit thumbnails
            table.verticalHeader().setDefaultSectionSize(110)

            for row_idx, row in enumerate(rows):
                # Unpack and handle release_date_locked
                # (sort_name, artist, title, format, country, release_date, release_date_locked, discogs_id, mbid)
                (sort_name, artist, title, format, country, release_date,
                 release_date_locked, discogs_id, mbid) = row

                # Column 0: Artwork (empty QTableWidgetItem for lazy thumbnail loading)
                thumbnail_item = QTableWidgetItem()
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

                # Column 2: Release Date (two lines, no wrapping within a line)
                release_human = parse_and_humanize_date(release_date)
                release_delta = humanize_date_delta(release_date)
                locked = bool(release_date_locked) if release_date_locked is not None else False
                if locked:
                    release_text = f"<b>{release_human} 🔒</b><br>{release_delta}"
                else:
                    release_text = f"<b>{release_human}</b><br>{release_delta}"
                release_label = QLabel()
                release_label.setText(release_text)
                release_label.setAlignment(Qt.AlignmentFlag.AlignLeft |
                                           Qt.AlignmentFlag.AlignVCenter)
                release_label.setWordWrap(False)
                release_label.setContentsMargins(10, 0, 10, 0)
                if locked:
                    release_label.setToolTip("Release date is locked; cannot be changed by import.")
                table.setCellWidget(row_idx, 2, release_label)

                # Column 3: Discogs Id
                table.setItem(row_idx, 3, QTableWidgetItem(str(discogs_id)))
                table.item(row_idx, 3).setTextAlignment(
                    Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

                # Column 4: Matched (centered)
                if mbid:
                    mb_row = db_musicbrainz.fetch_row(self.cfg.db_path, discogs_id=discogs_id)
                    score = mb_row.score if mb_row and mb_row.score is not None else 0
                else:
                    score = 0

                match_star = mb_matcher.score_stars(score)
                match_item = QTableWidgetItem(match_star)
                match_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                table.setItem(row_idx, 4, match_item)

            table.resizeColumnsToContents()
            table.setUpdatesEnabled(True)
            table.repaint()

            # Ensure double-click handler is not connected multiple times
            try:
                table.cellDoubleClicked.disconnect()
            except TypeError:
                pass
            # Hook up double-click for editing release date
            table.cellDoubleClicked.connect(lambda row, col: on_table_double_click(row, col, table))

            # Call lazy thumbnail loader after populating table
            load_visible_thumbnails()

        # Connect filter_timer to call populate_table (debounced)
        filter_timer.timeout.connect(populate_table)

        # Lazy load thumbnails for visible rows only (with column resize on first load)
        resize_done = False

        def load_visible_thumbnails():
            nonlocal resize_done
            viewport_rect = table.viewport().rect()
            visible_rows_loaded = 0

            for row in range(table.rowCount()):
                item = table.item(row, 0)
                if not item:
                    continue
                rect = table.visualItemRect(item)
                if viewport_rect.intersects(rect):
                    if item.data(Qt.ItemDataRole.DecorationRole) is None:
                        discogs_id_item = table.item(row, 3)
                        if discogs_id_item:
                            discogs_id = discogs_id_item.text()
                            image_path = os.path.join(self.cfg.images_folder, f"{discogs_id}.jpg")
                            if os.path.exists(image_path):
                                from PyQt6.QtGui import QPixmap
                                pixmap = QPixmap(image_path)
                                pixmap = pixmap.scaled(100, 100, Qt.AspectRatioMode.KeepAspectRatio,
                                                       Qt.TransformationMode.SmoothTransformation)
                                item.setData(Qt.ItemDataRole.DecorationRole, pixmap)
                                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                                visible_rows_loaded += 1

            if not resize_done and visible_rows_loaded > 0:
                table.resizeColumnsToContents()
                resize_done = True

        # Connect vertical scrollbar to lazy thumbnail loader
        def connect_scrollbar():
            scrollbar = table.verticalScrollBar()
            scrollbar.valueChanged.connect(load_visible_thumbnails)

        # Define double-click handler for release date edit
        def on_table_double_click(row, column, table):
            if column == 2:  # Release Date column
                discogs_id_item = table.item(row, 3)
                if discogs_id_item:
                    discogs_id = int(discogs_id_item.text())
                    with context_manager(self.cfg.db_path) as cur:
                        cur.execute(
                            "SELECT release_date, release_date_locked FROM discogs_releases WHERE discogs_id = ?",
                            (discogs_id,))
                        row_db = cur.fetchone()
                        if row_db:
                            initial_date = row_db.release_date if hasattr(
                                row_db, "release_date") else ""
                            locked = bool(row_db.release_date_locked) if hasattr(
                                row_db, "release_date_locked") and row_db.release_date_locked is not None else False
                        else:
                            initial_date = ""
                            locked = False
                    dialog = ReleaseDateEditDialog(initial_date, locked=locked)
                    if dialog.exec() == QDialog.DialogCode.Accepted:
                        new_date = dialog.get_date()
                        new_locked = dialog.is_locked()
                        if new_date:
                            with context_manager(self.cfg.db_path) as cur:
                                cur.execute(
                                    "UPDATE discogs_releases SET release_date = ?, release_date_locked = ? WHERE discogs_id = ?",
                                    (new_date, int(new_locked), discogs_id)
                                )
                            # Update table visually
                            label = table.cellWidget(row, column)
                            if label:
                                release_human = parse_and_humanize_date(new_date)
                                release_delta = humanize_date_delta(new_date)
                                if new_locked:
                                    release_text = f"<b>{release_human} 🔒</b><br>{release_delta}"
                                    label.setText(release_text)
                                    label.setToolTip(
                                        "Release date is locked; cannot be changed by import.")
                                else:
                                    release_text = f"<b>{release_human}</b><br>{release_delta}"
                                    label.setText(release_text)
                                    label.setToolTip("")

        # Connect filter changes to start the debounce timer instead of calling populate_table directly
        artist_input.textChanged.connect(lambda: filter_timer.start())
        title_input.textChanged.connect(lambda: filter_timer.start())
        format_input.textChanged.connect(lambda: filter_timer.start())
        year_from_input.textChanged.connect(lambda: filter_timer.start())
        year_to_input.textChanged.connect(lambda: filter_timer.start())

        def clear_filters():
            nonlocal last_filter_values
            artist_input.clear()
            title_input.clear()
            format_input.clear()
            year_from_input.clear()
            year_to_input.clear()
            last_filter_values = None  # Force repopulation
            filter_timer.start()

        clear_button.clicked.connect(clear_filters)

        # Connect scrollbar after widget is shown and table is created
        QTimer.singleShot(500, populate_table)
        QTimer.singleShot(600, connect_scrollbar)
        # Store the populate function for refresh_views
        self.populate_collection_table_fn = populate_table
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
        random_button.setStyleSheet(get_default_button_stylesheet())
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
            with context_manager(self.cfg.db_path) as cur:
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
        import_button.setStyleSheet(get_default_button_stylesheet())
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
                client, access_token, access_secret = discogs_importer.connect_to_discogs(
                    self.cfg.db_path)
            except Exception as e:
                log_output.append(f"Authentication failed: {e}")
                return

            import_button.setText("Cancel Import")

            worker = CollectionViewer.DiscogsImportWorker(client, self.cfg)
            self.worker = worker  # keep reference
            self.import_thread = QThread()
            worker.moveToThread(self.import_thread)

            worker.progress_msg.connect(lambda msg: log_output.append(msg))
            worker.progress.connect(progress_bar.setValue)
            worker.finished.connect(self.import_thread.quit)
            worker.finished.connect(worker.deleteLater)
            self.import_thread.finished.connect(self.import_thread.deleteLater)

            def restore_import_button():
                import_button.setText("Import from Discogs")
                import_button.setEnabled(True)
                import_button.setStyleSheet(get_default_button_stylesheet())
                enable_tabs_and_escape()
            worker.finished.connect(restore_import_button)
            worker.finished.connect(self.refresh_views)

            self.import_thread.started.connect(worker.run)
            self.import_thread.start()

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
                    import_button.setStyleSheet(get_default_button_stylesheet())

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
        match_button.setStyleSheet(get_default_button_stylesheet())
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
                # Only enable debugpy when running under a debugger AND not in a frozen (PyInstaller) app
                if DEBUG_MODE and not getattr(sys, 'frozen', False):
                    try:
                        import debugpy
                        debugpy.debug_this_thread()
                    except Exception:
                        # Silently ignore any debug attachment failures in dev
                        pass

                try:
                    musicbrainzngs.set_useragent(
                        app=GROOVEKRAFT_USER_AGENT, version=GROOVEKRAFT_VERSION)
                    musicbrainzngs.auth(self.cfg.username, self.cfg.password)
                    musicbrainzngs.set_rate_limit(1, 1)

                    mb_matcher.match_discogs_against_mb(
                        self.cfg.db_path,
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
                creds = db_musicbrainz.get_credentials(self.cfg.db_path)
                if creds:
                    username, password = creds
                    try:
                        musicbrainzngs.set_useragent(
                            app=GROOVEKRAFT_USER_AGENT,
                            version=GROOVEKRAFT_VERSION)
                        musicbrainzngs.auth(username, password)
                    except Exception:
                        creds = None

                if not creds:
                    dlg = mb_auth_gui.MBAuthDialog()
                    if dlg.exec() == QDialog.DialogCode.Accepted:
                        username, password = dlg.get_credentials()
                        db_musicbrainz.set_credentials(self.cfg.db_path, username, password)
                    else:
                        return

                cfg = SimpleNamespace()
                cfg.db_path = self.cfg.db_path
                cfg.username = username
                cfg.password = password
                cfg.match_all = match_all_checkbox.isChecked()

                worker = MBMatcherWorker(cfg)
                self.mb_worker = worker

                # Always use QThread, signals, and GUI updates (no is_debugging() branch)
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
                    match_button.setStyleSheet(get_default_button_stylesheet())
                worker.finished.connect(restore_button)
                worker.finished.connect(enable_tabs_and_escape)
                worker.finished.connect(self.refresh_views)

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
                    match_button.setStyleSheet(get_default_button_stylesheet())

        match_button.clicked.connect(run_match)
        return widget


def run_gui(cfg: AppConfig) -> None:
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon("assets/groovekraft_icon.png"))
    viewer = CollectionViewer(cfg)
    sys.exit(app.exec())
