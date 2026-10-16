from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox, QFrame, QHBoxLayout, QHeaderView, QInputDialog,
    QLabel, QMessageBox, QPushButton, QTableWidget, QTableWidgetItem,
    QVBoxLayout, QWidget,
)

from models import QUEUES, STATUSES


class ManagerConsole(QWidget):
    def __init__(self, store):
        super().__init__()
        self.store = store
        self.setWindowTitle("Manager Console")
        self._building = False

        root = QVBoxLayout(self)
        root.setContentsMargins(20, 20, 20, 20)
        root.setSpacing(14)

        title = QLabel("Manager Console")
        title.setStyleSheet("font-size:20px; font-weight:700; color:#e2e8f0;")
        root.addWidget(title)

        # mass action bar
        bar = QHBoxLayout()
        bar.addWidget(self._btn("Login All → All Queues", lambda: self._mass(["French", "English", "Delivery Line"], True)))
        bar.addWidget(self._btn("Logout All ← All Queues", lambda: self._mass(["French", "English", "Delivery Line"], False), danger=True))
        for q in QUEUES:
            bar.addWidget(self._btn(f"{q}: Login All", lambda _=False, qq=q: self._mass([qq], True)))
            bar.addWidget(self._btn(f"{q}: Logout All", lambda _=False, qq=q: self._mass([qq], False), danger=True))
        bar.addStretch()
        root.addLayout(bar)

        # agent table
        self.table = QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels(
            ["Agent", "Status", "Duration", "French", "English", "Delivery", "Set Status", "Queue Toggles"]
        )
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        root.addWidget(self.table, 3)

        # audit log
        audit_label = QLabel("Audit Log")
        audit_label.setStyleSheet("font-size:16px; font-weight:600; color:#e2e8f0; margin-top:8px;")
        root.addWidget(audit_label)
        self.audit = QTableWidget(0, 5)
        self.audit.setHorizontalHeaderLabels(["Timestamp", "Manager", "Action", "Target", "Result"])
        self.audit.verticalHeader().setVisible(False)
        self.audit.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        root.addWidget(self.audit, 2)

        self.store.agents_changed.connect(self._refresh_agents)
        self.store.audit_changed.connect(self._refresh_audit)

        self._refresh_agents()
        self._refresh_audit()

    def _btn(self, label, cb, danger=False):
        b = QPushButton(label)
        if danger:
            b.setStyleSheet("background:#7f1d1d; color:#fee2e2; padding:8px 12px; border-radius:6px;")
        else:
            b.setStyleSheet("background:#1e293b; color:#e2e8f0; padding:8px 12px; border-radius:6px;")
        b.clicked.connect(cb)
        return b

    def _mass(self, queues: list[str], on: bool):
        if not on:
            text, ok = QInputDialog.getText(
                self, "Confirm mass logout",
                f"Type CONFIRM to logout all agents from: {', '.join(queues)}",
            )
            if not ok or text.strip() != "CONFIRM":
                self.store.append_audit("Logout All (cancelled)", ", ".join(queues), "Cancelled")
                return
        else:
            if QMessageBox.question(
                self, "Confirm",
                f"Login all agents to: {', '.join(queues)}?",
            ) != QMessageBox.Yes:
                return
        label = "Login All" if on else "Logout All"
        self.store.mass_set_queue(queues, on, label)

    def _refresh_agents(self):
        self._building = True
        self.table.setRowCount(len(self.store.agents))
        for r, a in enumerate(self.store.agents):
            self.table.setItem(r, 0, QTableWidgetItem(a.name))
            self.table.setItem(r, 1, QTableWidgetItem(a.status))
            self.table.setItem(r, 2, QTableWidgetItem(a.duration_str()))

            for col, flag in ((3, a.french), (4, a.english), (5, a.delivery)):
                item = QTableWidgetItem("●" if flag else "○")
                item.setTextAlignment(Qt.AlignCenter)
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                self.table.setItem(r, col, item)

            combo = QComboBox()
            combo.addItems(STATUSES)
            combo.setCurrentText(a.status)
            combo.currentTextChanged.connect(lambda s, aid=a.id: self.store.set_agent_status(aid, s))
            self.table.setCellWidget(r, 6, combo)

            toggles = QWidget()
            hb = QHBoxLayout(toggles); hb.setContentsMargins(0, 0, 0, 0); hb.setSpacing(6)
            for q, flag in (("French", a.french), ("English", a.english), ("Delivery Line", a.delivery)):
                b = QPushButton(f"{q[:2]} {'ON' if flag else 'OFF'}")
                b.setFixedWidth(60)
                b.setStyleSheet(
                    "background:%s; color:white; border-radius:4px; padding:4px;" %
                    ("#166534" if flag else "#334155")
                )
                b.clicked.connect(lambda _=False, aid=a.id, qq=q, cur=flag: self.store.set_agent_queue(aid, qq, not cur))
                hb.addWidget(b)
            self.table.setCellWidget(r, 7, toggles)
        self._building = False

    def _refresh_audit(self):
        rows = self.store.audit_rows()
        self.audit.setRowCount(len(rows))
        for r, row in enumerate(rows):
            for c, val in enumerate(row):
                self.audit.setItem(r, c, QTableWidgetItem(str(val)))