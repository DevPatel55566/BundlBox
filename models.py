from dataclasses import dataclass
from datetime import datetime

STATUSES = [
    "Login / Available",
    "Logout / Offline",
    "Admin / Wrap Up",
    "Break / Lunch",
    "Outbound",
]
QUEUES = ["French", "English", "Delivery Line"]

STATUS_COLORS = {
    "Login / Available": "#22c55e",
    "Logout / Offline": "#64748b",
    "Admin / Wrap Up": "#eab308",
    "Break / Lunch": "#f97316",
    "Outbound": "#a855f7",
}

QUEUE_COLORS = {
    "French": "#38bdf8",
    "English": "#a78bfa",
    "Delivery Line": "#fb7185",
}


@dataclass
class Agent:
    id: int
    name: str
    status: str
    status_since: datetime
    shift_start: datetime
    french: bool = False
    english: bool = False
    delivery: bool = False

    @property
    def queues(self) -> list[str]:
        return [
            q
            for q, on in (
                ("French", self.french),
                ("English", self.english),
                ("Delivery Line", self.delivery),
            )
            if on
        ]

    def duration_str(self, now: datetime | None = None) -> str:
        now = now or datetime.now()
        s = max(0, int((now - self.status_since).total_seconds()))
        h, r = divmod(s, 3600)
        m, s = divmod(r, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"


@dataclass
class QueueMetric:
    queue: str
    waiting: int = 0
    longest_wait_seconds: int = 0
    available_agents: int = 0
    logged_in_agents: int = 0
    sla: float = 100.0

    def level(self) -> str:
        if self.waiting >= 5 or self.longest_wait_seconds >= 90:
            return "critical"
        if self.waiting >= 2 or self.longest_wait_seconds >= 45:
            return "warning"
        return "normal"

    def longest_str(self) -> str:
        m, s = divmod(self.longest_wait_seconds, 60)
        return f"{m:02d}:{s:02d}"