from datetime import datetime

from PySide6.QtCore import QObject, Signal

import db
from models import Agent, QueueMetric, QUEUES, STATUSES


class Store(QObject):
    agents_changed = Signal()
    metrics_changed = Signal()
    audit_changed = Signal()
    events_changed = Signal()

    def __init__(self, manager_name: str = "Manager User"):
        super().__init__()
        self.conn = db.init_db()
        self.manager_name = manager_name
        self.agents: list[Agent] = []
        self.metrics: dict[str, QueueMetric] = {}
        self._load_agents()
        self._load_metrics()

    # ---------- loaders ----------

    def _load_agents(self) -> None:
        rows = self.conn.execute("SELECT * FROM agents ORDER BY name").fetchall()
        self.agents = [
            Agent(
                id=r["id"],
                name=r["name"],
                status=r["status"],
                status_since=datetime.fromisoformat(r["status_since"]),
                shift_start=datetime.fromisoformat(r["shift_start"]),
                french=bool(r["french"]),
                english=bool(r["english"]),
                delivery=bool(r["delivery"]),
            )
            for r in rows
        ]

    def _load_metrics(self) -> None:
        rows = self.conn.execute("SELECT * FROM queue_metrics").fetchall()
        self.metrics = {
            r["queue"]: QueueMetric(
                queue=r["queue"],
                waiting=r["waiting"],
                longest_wait_seconds=r["longest_wait_seconds"],
                available_agents=r["available_agents"],
                logged_in_agents=r["logged_in_agents"],
                sla=r["sla"],
            )
            for r in rows
        }

    def agent_by_id(self, agent_id: int) -> Agent | None:
        return next((a for a in self.agents if a.id == agent_id), None)

    # ---------- mutations ----------

    def set_agent_status(self, agent_id: int, new_status: str) -> None:
        agent = self.agent_by_id(agent_id)
        if not agent or new_status not in STATUSES or agent.status == new_status:
            return
        now = datetime.now()
        self.conn.execute(
            "UPDATE status_events SET ended_at = ? WHERE agent_id = ? AND ended_at IS NULL",
            (now.isoformat(), agent_id),
        )
        self.conn.execute(
            "INSERT INTO status_events (agent_id, status, started_at) VALUES (?, ?, ?)",
            (agent_id, new_status, now.isoformat()),
        )
        self.conn.execute(
            "UPDATE agents SET status = ?, status_since = ? WHERE id = ?",
            (new_status, now.isoformat(), agent_id),
        )
        self.conn.commit()
        agent.status = new_status
        agent.status_since = now
        self.agents_changed.emit()
        self.events_changed.emit()

    def set_agent_queue(self, agent_id: int, queue: str, on: bool) -> None:
        agent = self.agent_by_id(agent_id)
        if not agent or queue not in QUEUES:
            return
        col = {"French": "french", "English": "english", "Delivery Line": "delivery"}[queue]
        self.conn.execute(f"UPDATE agents SET {col} = ? WHERE id = ?", (int(on), agent_id))
        self.conn.commit()
        setattr(agent, col, on)
        self.agents_changed.emit()

    def mass_set_queue(self, queues: list[str], on: bool, action_label: str) -> None:
        for queue in queues:
            col = {"French": "french", "English": "english", "Delivery Line": "delivery"}[queue]
            self.conn.execute(f"UPDATE agents SET {col} = ?", (int(on),))
        self.conn.commit()
        for agent in self.agents:
            for queue in queues:
                setattr(
                    agent,
                    {"French": "french", "English": "english", "Delivery Line": "delivery"}[queue],
                    on,
                )
        self.append_audit(action_label, ", ".join(queues) or "All queues", "OK")
        self.agents_changed.emit()

    def update_metric(self, m: QueueMetric) -> None:
        self.conn.execute(
            """INSERT INTO queue_metrics (queue, waiting, longest_wait_seconds,
               available_agents, logged_in_agents, sla, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(queue) DO UPDATE SET
                 waiting=excluded.waiting,
                 longest_wait_seconds=excluded.longest_wait_seconds,
                 available_agents=excluded.available_agents,
                 logged_in_agents=excluded.logged_in_agents,
                 sla=excluded.sla,
                 updated_at=excluded.updated_at""",
            (
                m.queue, m.waiting, m.longest_wait_seconds,
                m.available_agents, m.logged_in_agents, m.sla,
                datetime.now().isoformat(),
            ),
        )
        self.conn.commit()
        self.metrics[m.queue] = m
        self.metrics_changed.emit()

    def append_audit(self, action: str, target: str, result: str) -> None:
        self.conn.execute(
            "INSERT INTO audit_log (timestamp, manager, action, target, result) VALUES (?, ?, ?, ?, ?)",
            (datetime.now().isoformat(timespec="seconds"), self.manager_name, action, target, result),
        )
        self.conn.commit()
        self.audit_changed.emit()

    def audit_rows(self) -> list[tuple]:
        return self.conn.execute(
            "SELECT timestamp, manager, action, target, result FROM audit_log ORDER BY id DESC LIMIT 200"
        ).fetchall()

    def status_events_for(self, agent_id: int) -> list[tuple]:
        return self.conn.execute(
            "SELECT status, started_at, ended_at FROM status_events WHERE agent_id = ? ORDER BY id",
            (agent_id,),
        ).fetchall()