import csv
from collections import defaultdict
from datetime import datetime

from PySide6.QtWidgets import (
    QFileDialog, QHBoxLayout, QHeaderView, QLabel, QMessageBox,
    QPushButton, QTableWidget, QTableWidgetItem, QTabWidget, QVBoxLayout, QWidget,
)

from models import QUEUES, STATUSES

PAYROLL_COLS = [
    "Agent", "Date", "Shift Start", "Shift End", "Login Time", "Break Time",
    "Lunch Time", "Admin Time", "Wrap Up Time", "Outbound Time",
    "French Queue Time", "English Queue Time", "Delivery Queue Time",
]


class Reports(QWidget):
    def __init__(self, store):
        super().__init__()
        self.store = store
        root = QVBoxLayout(self)

        title = QLabel("Reporting")
        title.setStyleSheet("font-size:20px; font-weight:700; color:#e2e8f0;")
        root.addWidget(title)

        self.tabs = QTabWidget()
        self.tabs.addTab(self._shift_summary(), "Shift Summary")
        self.tabs.addTab(self._queue_activity(), "Queue Activity")
        self.tabs.addTab(self._payroll_tab(), "Payroll Export")
        root.addWidget(self.tabs)

        self.store.events_changed.connect(self.refresh)
        self.refresh()

    def _shift_summary(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w)
        self.summary = QTableWidget(0, 7)
        self.summary.setHorizontalHeaderLabels(
            ["Agent", "Login", "Break", "Lunch", "Admin/Wrap Up", "Outbound", "Total"]
        )
        self.summary.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        v.addWidget(self.summary)
        return w

    def _queue_activity(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w)
        self.queue_activity = QTableWidget(0, 5)
        self.queue_activity.setHorizontalHeaderLabels(
            ["Agent", "French", "English", "Delivery Line", "Total Queue Time"]
        )
        self.queue_activity.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        v.addWidget(self.queue_activity)
        return w

    def _payroll_tab(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w)
        v.addWidget(QLabel("Exports the shift summary as a payroll-ready CSV."))
        btn = QPushButton("Export Payroll CSV")
        btn.clicked.connect(self._export_csv)
        btn.setStyleSheet("background:#166534; color:white; padding:10px 16px; border-radius:6px;")
        v.addWidget(btn)
        v.addStretch()
        return w

    # ---------- computation ----------

    def _seconds_by_status(self, agent_id: int) -> dict[str, int]:
        out: dict[str, int] = defaultdict(int)
        for status, start, end in self.store.status_events_for(agent_id):
            start_dt = datetime.fromisoformat(start)
            end_dt = datetime.fromisoformat(end) if end else datetime.now()
            out[status] += max(0, int((end_dt - start_dt).total_seconds()))
        return out

    @staticmethod
    def _fmt(seconds: int) -> str:
        h, r = divmod(seconds, 3600)
        m, _ = divmod(r, 60)
        return f"{h:02d}:{m:02d}"

    def refresh(self):
        agents = self.store.agents
        self.summary.setRowCount(len(agents))
        self.queue_activity.setRowCount(len(agents))
        for r, a in enumerate(agents):
            secs = self._seconds_by_status(a.id)
            row = [
                a.name,
                self._fmt(secs.get("Login / Available", 0)),
                self._fmt(secs.get("Break / Lunch", 0) // 2),
                self._fmt(secs.get("Break / Lunch", 0) // 2),
                self._fmt(secs.get("Admin / Wrap Up", 0)),
                self._fmt(secs.get("Outbound", 0)),
                self._fmt(sum(secs.values())),
            ]
            for c, v in enumerate(row):
                self.summary.setItem(r, c, QTableWidgetItem(str(v)))

            # queue activity — approximate: allocate Login time across queues agent is a member of
            login = secs.get("Login / Available", 0)
            member = a.queues or ["French"]
            per = login // len(member)
            qcells = [a.name] + [
                self._fmt(per if q in a.queues else 0) for q in QUEUES
            ] + [self._fmt(per * len(member))]
            for c, v in enumerate(qcells):
                self.queue_activity.setItem(r, c, QTableWidgetItem(str(v)))

    def _export_csv(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Save payroll CSV",
            f"payroll-{datetime.now().strftime('%Y-%m-%d')}.csv",
            "CSV (*.csv)",
        )
        if not path:
            return
        today = datetime.now().strftime("%Y-%m-%d")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(PAYROLL_COLS)
            for a in self.store.agents:
                secs = self._seconds_by_status(a.id)
                break_lunch = secs.get("Break / Lunch", 0)
                w.writerow([
                    a.name, today,
                    a.shift_start.strftime("%H:%M"), datetime.now().strftime("%H:%M"),
                    self._fmt(secs.get("Login / Available", 0)),
                    self._fmt(break_lunch // 2),
                    self._fmt(break_lunch // 2),
                    self._fmt(secs.get("Admin / Wrap Up", 0)),
                    self._fmt(secs.get("Admin / Wrap Up", 0)),
                    self._fmt(secs.get("Outbound", 0)),
                    self._fmt(secs.get("Login / Available", 0) if a.french else 0),
                    self._fmt(secs.get("Login / Available", 0) if a.english else 0),
                    self._fmt(secs.get("Login / Available", 0) if a.delivery else 0),
                ])
        QMessageBox.information(self, "Export complete", f"Saved to:\n{path}")