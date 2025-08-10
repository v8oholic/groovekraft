# tools/browse_qt_icons.py
from PyQt6.QtWidgets import (
    QApplication, QWidget, QGridLayout, QLabel, QScrollArea, QVBoxLayout, QStyle
)
from PyQt6.QtCore import Qt, QSize
import sys
import inspect


def main():
    app = QApplication(sys.argv)

    # Collect StandardPixmap enum values directly (avoids non-enum attributes)
    members = list(QStyle.StandardPixmap)
    members.sort(key=lambda e: e.name.lower())

    # Build a grid of icons + names
    grid = QGridLayout()
    grid.setSpacing(10)
    icon_size = QSize(24, 24)

    style = app.style()
    cols = 4  # tweak layout width here
    for i, enum_val in enumerate(members):
        row, col = divmod(i, cols)

        # Icon preview
        icon = style.standardIcon(enum_val)
        pm = icon.pixmap(icon_size)
        pic = QLabel()
        pic.setPixmap(pm)
        pic.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # Name label
        lbl = QLabel(enum_val.name)
        lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        cell = QVBoxLayout()
        w = QWidget()
        cell.setSpacing(4)
        cell.addWidget(pic)
        cell.addWidget(lbl)
        w.setLayout(cell)
        grid.addWidget(w, row, col)

    # Scroll container
    content = QWidget()
    content.setLayout(grid)
    scroll = QScrollArea()
    scroll.setWidgetResizable(True)
    scroll.setWidget(content)

    root = QWidget()
    root_layout = QVBoxLayout(root)
    root_layout.addWidget(scroll)
    root.setWindowTitle("Qt Standard Icons (QStyle.StandardPixmap)")
    root.resize(720, 600)
    root.show()

    # Also print a plain list to the console
    print("Available QStyle.StandardPixmap constants:")
    for e in members:
        print(" -", e.name)

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
