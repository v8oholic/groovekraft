from PyQt6.QtWidgets import (
    QDialog, QLineEdit, QFormLayout, QDialogButtonBox
)


class MBAuthDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("MusicBrainz Login")
        self.layout = QFormLayout(self)

        self.username_input = QLineEdit()
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)

        self.layout.addRow("Username:", self.username_input)
        self.layout.addRow("Password:", self.password_input)

        self.buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)

        self.layout.addWidget(self.buttons)

    def get_credentials(self):
        return self.username_input.text(), self.password_input.text()
