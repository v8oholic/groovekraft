from PyQt6.QtWidgets import QApplication, QLabel, QWidget, QVBoxLayout, QMainWindow, QTabWidget, QTextEdit, QTableWidget, QTableWidgetItem
from PyQt6.QtWidgets import QLineEdit, QHBoxLayout
from PyQt6.QtGui import QKeySequence, QShortcut
from PyQt6.QtCore import Qt

from modules import db, utils

import sys
from modules.config import AppConfig


class CollectionViewer(QMainWindow):
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
        self.setCentralWidget(tab_widget)
        esc_shortcut = QShortcut(QKeySequence("Escape"), self)
        esc_shortcut.activated.connect(self.close)
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

        # Table
        table = QTableWidget()
        main_layout.addWidget(table)

        def populate_table():
            with db.context_manager() as cur:
                query = []
                query.append(
                    "SELECT d.sort_name AS artist, d.title, d.format, d.country, d.release_date, d.discogs_id, m.mbid")
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

            for row_idx, (artist, title, format, country, release_date, discogs_id, mbid) in enumerate(rows):
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

        populate_table()
        return widget


def run_gui(cfg: AppConfig) -> None:
    app = QApplication(sys.argv)
    viewer = CollectionViewer(cfg)
    sys.exit(app.exec())
