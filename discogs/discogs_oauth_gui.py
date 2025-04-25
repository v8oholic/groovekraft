from PyQt6.QtWidgets import (
    QApplication, QDialog, QVBoxLayout, QLabel, QLineEdit, QPushButton
)
import webbrowser
import sys


def prompt_oauth_verifier_gui(auth_url):
    app = QApplication.instance()
    if not app:
        app = QApplication(sys.argv)

    dialog = QDialog()
    dialog.setWindowTitle("Discogs Authorization")
    layout = QVBoxLayout(dialog)

    instruction_label = QLabel("1. Click the button below to open the Discogs authorization page.\n"
                               "2. Authorize the app and you will receive a verification code.\n"
                               "3. Paste that code into the box below and click Submit.")
    layout.addWidget(instruction_label)

    open_browser_button = QPushButton("Open Discogs Authorization Page")
    layout.addWidget(open_browser_button)
    open_browser_button.clicked.connect(lambda: webbrowser.open(auth_url))

    verifier_input = QLineEdit()
    verifier_input.setPlaceholderText("Enter verification code here")
    layout.addWidget(verifier_input)

    error_label = QLabel()
    error_label.setStyleSheet("color: red")
    layout.addWidget(error_label)

    submit_button = QPushButton("Submit")
    layout.addWidget(submit_button)

    def on_submit():
        if not verifier_input.text().strip():
            error_label.setText("Please enter the verification code.")
        else:
            dialog.accept()

    submit_button.clicked.connect(on_submit)
    verifier_input.returnPressed.connect(on_submit)
    verifier_input.textChanged.connect(lambda: error_label.setText(""))

    dialog.exec()

    return verifier_input.text().strip()
