import random
from datetime import datetime, timedelta

from PySide6.QtCore import QObject, QTimer, Signal

from models import STATUSES, QUEUES, QueueMetric

NAMES = [
    "Amélie Roy", "Benoit Tremblay", "Chloé Gagnon", "Daniel Okafor",
    "Élodie Côté", "Farid Haddad", "Grace Liu", "Hugo Bouchard",
    "Inès Martin", "Jordan Peters", "Karim Nasr", "Léa Bergeron",
]


class Simulator(QObject):
    """Seeds the DB once and drives mock real-time updates."""

    def __init__(self, store):
        super().__init__()
        self.store = store
        self._seed_if_empty()
        self._metrics_timer = QTimer(self)
        self._metrics_timer.timeout.connect(self._tick_metrics)
        self._metrics_timer.start(3000)
        self._agents_timer = QTimer(self)
        self._agents_timer.timeout.connect(self._tick_agents)
        self._agents_timer.start(12000)

    def _seed_if_empty(self) -> None:
        count = self.store.conn.execute("SELECT COUNT(*) FROM agents").fetchone()[0]
        if count:
            return
        now = datetime.now()
        for i, name in enumerate(NAMES, start=1):
            status = random.choice(STATUSES)
            shift_start = now - timedelta(hours=random.randint(1, 6))
            self.store.conn.execute(
                """INSERT INTO agents (id, name, status, status_since, shift_start,
                   french, english, delivery) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    i, name, status, now.isoformat(), shift_start.isoformat(),
                    int(random.random() < 0.6),
                    int(random.random() < 0.7),
                    int(random.random() < 0.5),
                ),
            )
            self.store.conn.execute(
                "INSERT INTO status_events (agent_id, status, started_at) VALUES (?, ?, ?)",
                (i, status, now.isoformat()),
            )
        for q in QUEUES:
            self.store.conn.execute(
                """INSERT OR REPLACE INTO queue_metrics
                   (queue, waiting, longest_wait_seconds, available_agents,
                    logged_in_agents, sla, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (q, 0, 0, 0, 0, 100.0, now.isoformat()),
            )
        self.store.conn.commit()
        self.store._load_agents()
        self.store._load_metrics()

    def _recompute_counts(self) -> None:
        for q in QUEUES:
            col = {"French": "french", "English": "english", "Delivery Line": "delivery"}[q]
            logged = self.store.conn.execute(
                f"SELECT COUNT(*) FROM agents WHERE {col} = 1"
            ).fetchone()[0]
            avail = self.store.conn.execute(
                f"SELECT COUNT(*) FROM agents WHERE {col} = 1 AND status = 'Login / Available'"
            ).fetchone()[0]
            m = self.store.metrics[q]
            m.logged_in_agents = logged
            m.available_agents = avail
            self.store.update_metric(m)

    def _tick_metrics(self) -> None:
        self._recompute_counts()
        for q in QUEUES:
            m = self.store.metrics[q]
            drift = random.choice([-1, 0, 0, 1, 1, 2])
            m.waiting = max(0, m.waiting + drift)
            if m.waiting == 0:
                m.longest_wait_seconds = 0
            else:
                m.longest_wait_seconds = min(
                    600, m.longest_wait_seconds + random.randint(5, 20)
                )
            m.sla = max(40.0, min(100.0, 100 - m.waiting * 4.5))
            self.store.update_metric(m)

    def _tick_agents(self) -> None:
        if not self.store.agents:
            return
        agent = random.choice(self.store.agents)
        new_status = random.choice(STATUSES)
        self.store.set_agent_status(agent.id, new_status)