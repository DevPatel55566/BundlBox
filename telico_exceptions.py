"""Dedicated module for syncing Telico exception groups (Closed Early / Holidays)."""
import logging
import telico_client

log = logging.getLogger("telico_exceptions")

def sync_closed_exception(group_id: int, name: str, date_str: str, existing_id: str | None = None) -> bool:
    """Pushes a 00:00-23:59 closure to the specified exception group."""
    log.info(f"Pushing closed exception '{name}' on {date_str} to group {group_id}")
    return telico_client.save_time_period(
        group_id,
        existing_id,
        {
            "name": name,
            "start": "00:00",
            "end": "23:59",
            "repeats": False,
            "date": date_str,
        }
    )