from datetime import datetime

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QFrame, QGridLayout, QHBoxLayout, QLabel, QTableWidget,
    QTableWidgetItem, QVBoxLayout, QWidget,
)

from models import QUEUES, STATUS_COLORS


class QueueCard(QFrame):
    def __init__(self, queue: str):
        super().__init__()
        self.queue = queue
        self.level = "normal"
        self._pulse_on = False
        self.setMinimumSize(320, 220)

        root = QVBoxLayout(self)
        root.setContentsMargins(20, 16, 20, 16)
        root.setSpacing(8)

        self.title = QLabel(queue)
        f = QFont(); f.setPointSize(20); f.setBold(True)
        self.title.setFont(f)

        self.waiting = QLabel("0")
        f = QFont(); f.setPointSize(72); f.setBold(True)
        self.waiting.setFont(f)
        self.waiting.setAlignment(Qt.AlignCenter)

        self.waiting_caption = QLabel("waiting")
        self.waiting_caption.setAlignment(Qt.AlignCenter)

        self.meta = QLabel()
        self.meta.setAlignment(Qt.AlignCenter)

        root.addWidget(self.title, alignment=Qt.AlignCenter)
        root.addWidget(self.waiting, alignment=Qt.AlignCenter)
        root.addWidget(self.waiting_caption, alignment=Qt.AlignCenter)
        root.addWidget(self.meta, alignment=Qt.AlignCenter)
        self._apply_style()

    def update_metric(self, m):
        self.waiting.setText(str(m.waiting))
        self.meta.setText(
            f"longest {m.longest_str()}   |   avail {m.available_agents}/{m.logged_in_agents}   |   SLA {m.sla:.0f}%"
        )
        new_level = m.level()
        if new_level != self.level:
            self.level = new_level
            if new_level == "normal":
                self._pulse_timer.stop()
                self._pulse_on = False
            else:
                if not self._pulse_timer.isActive():
                    self._pulse_timer.start(600)
        self._apply_style()

    def _apply_style(self):
        base = "QFrame { background: #0f172a; border-radius: 14px; border: 4px solid %s; } QLabel { color: #f8fafc; }"
        if self.level == "critical":
            border = "#ef4444" if self._pulse_on else "#7f1d1d"
        elif self.level == "warning":
            border = "#eab308" if self._pulse_on else "#713f12"
        else:
            border = "#1e293b"
        self.setStyleSheet(base % border)
        if self.level == "critical":
            self.waiting.setStyleSheet("color: #fecaca;")
        elif self.level == "warning":
            self.waiting.setStyleSheet("color: #fde68a;")
        else:
            self.waiting.setStyleSheet("color: #f8fafc;")

    def _on_pulse(self):
        self._pulse_on = not self._pulse_on
        self._apply_style()

    _pulse_timer = None


# attach the pulse timer lazily (avoids double-start in __init__ ordering)
def _attach_timer(card: QueueCard):
    card._pulse_timer = QTimer(card)
    card._pulse_timer.timeout.connect(card._on_pulse)


class Wallboard(QWidget):
    def __init__(self, store, fullscreen: bool = False):
        super().__init__()
        self.store = store
        self.setWindowTitle("Contact Center Wallboard")
        self.setStyleSheet("background: #020617;")
        if fullscreen:
            self.showFullScreen()

        root = QVBoxLayout(self)
        root.setContentsMargins(24, 20, 24, 20)
        root.setSpacing(16)

        # header
        header = QHBoxLayout()
        self.title = QLabel("Contact Center Command Center")
        f = QFont(); f.setPointSize(20); f.setBold(True)
        self.title.setFont(f)
        self.title.setStyleSheet("color: #e2e8f0;")
        self.clock = QLabel()
        f = QFont(); f.setPointSize(18)
        self.clock.setFont(f)
        self.clock.setStyleSheet("color: #94a3b8;")
        header.addWidget(self.title)
        header.addStretch()
        header.addWidget(self.clock)
        root.addLayout(header)

        # alert banner
        self.banner = QLabel()
        self.banner.setAlignment(Qt.AlignCenter)
        f = QFont(); f.setPointSize(16); f.setBold(True)
        self.banner.setFont(f)
        self.banner.setVisible(False)
        self.banner.setStyleSheet(
            "background:#7f1d1d; color:#fee2e2; padding:12px; border-radius:10px;"
        )
        root.addWidget(self.banner)

        # queue cards
        cards = QHBoxLayout()
        cards.setSpacing(16)
        self.cards: dict[str, QueueCard] = {}
        for q in QUEUES:
            card = QueueCard(q)
            _attach_timer(card)
            self.cards[q] = card
            cards.addWidget(card)
        root.addLayout(cards)

        # agent grid
        self.table = QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels(
            ["Agent", "Status", "Duration", "French", "English", "Delivery", "Shift Start"]
        )
        self.table.verticalHeader().setVisible(False)
        self.table.setStyleSheet(
            "QTableWidget { background:#0f172a; color:#e2e8f0; gridline-color:#1e293b; font-size:16px; }"
            "QHeaderView::section { background:#1e293b; color:#cbd5e1; padding:8px; border:none; }"
        )
        self.table.horizontalHeader().setStretchLastSection(True)
        root.addWidget(self.table, 1)

        self.store.metrics_changed.connect(self._refresh_metrics)
        self.store.agents_changed.connect(self._refresh_agents)

        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._tick)
        self._clock_timer.start(1000)

        self._refresh_metrics()
        self._refresh_agents()

    def _tick(self):
        self.clock.setText(datetime.now().strftime("%a %b %d  %H:%M:%S"))
        self._refresh_agents()

    def _refresh_metrics(self):
        for q, card in self.cards.items():
            m = self.store.metrics.get(q)
            if m:
                card.update_metric(m)
        worst = max(self.store.metrics.values(), key=lambda m: (m.level() != "normal", m.waiting))
        if worst.level() == "critical":
            self.banner.setText(
                f"⚠  {worst.queue}: {worst.waiting} waiting, longest {worst.longest_str()} — CRITICAL"
            )
            self.banner.setVisible(True)
        elif worst.level() == "warning":
            self.banner.setText(
                f"{worst.queue}: {worst.waiting} waiting, longest {worst.longest_str()}"
            )
            self.banner.setStyleSheet(
                "background:#713f12; color:#fef3c7; padding:12px; border-radius:10px;"
            )
            self.banner.setVisible(True)
        else:
            self.banner.setVisible(False)

    def _refresh_agents(self):
        now = datetime.now()
        self.table.setRowCount(len(self.store.agents))
        for r, a in enumerate(self.store.agents):
            self.table.setItem(r, 0, QTableWidgetItem(a.name))
            st = QTableWidgetItem(a.status)
            st.setForeground(Qt.white)
            st.setBackground(Qt.GlobalColor.transparent)
            self.table.setItem(r, 1, st)
            self.table.setItem(r, 2, QTableWidgetItem(a.duration_str(now)))
            self.table.setItem(r, 3, QTableWidgetItem("●" if a.french else ""))
            self.table.setItem(r, 4, QTableWidgetItem("●" if a.english else ""))
            self.table.setItem(r, 5, QTableWidgetItem("●" if a.delivery else ""))
            self.table.setItem(r, 6, QTableWidgetItem(a.shift_start.strftime("%H:%M")))