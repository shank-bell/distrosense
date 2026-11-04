import time
from collections import OrderedDict
from typing import Callable, Tuple


class IncidentFilter:
    """
    Decides whether an incident should be acted on, so the orchestrator is
    safe under at-least-once delivery and doesn't page on every re-detection
    of the same fault:

      1. duplicate  - the same incident_id was already seen (Kafka redelivery
                      after a crash/rebalance).
      2. stale      - closed_at is older than max_age (replay after downtime,
                      or the first start with auto_offset_reset="earliest").
      3. cooldown   - the same root-cause service was already acted on within
                      `cooldown_seconds`. A *higher* severity still gets
                      through, so an escalation is never swallowed.
    """

    def __init__(self, cooldown_seconds: int, max_age_seconds: int,
                 max_seen: int = 2000, clock: Callable[[], float] = time.time):
        self._cooldown = cooldown_seconds
        self._max_age = max_age_seconds
        self._max_seen = max_seen
        self._clock = clock
        self._seen: "OrderedDict[str, None]" = OrderedDict()   # bounded, oldest evicted first
        self._last: dict = {}                                  # root_cause -> (acted_at, severity)

    def check(self, incident: dict) -> Tuple[bool, str]:
        """Returns (should_act, reason_if_skipped)."""
        now = self._clock()

        incident_id = incident.get("incident_id")
        if incident_id in self._seen:
            return False, "duplicate incident_id"
        self._seen[incident_id] = None
        while len(self._seen) > self._max_seen:
            self._seen.popitem(last=False)

        closed_at = incident.get("closed_at")
        if closed_at is not None and now - closed_at > self._max_age:
            return False, f"stale ({int(now - closed_at)}s old)"

        root_cause = incident.get("root_cause_service")
        severity = incident.get("severity") or 0
        last = self._last.get(root_cause)
        if last is not None:
            acted_at, last_severity = last
            if now - acted_at < self._cooldown and severity <= last_severity:
                return False, (f"cooldown: {root_cause} already handled {int(now - acted_at)}s ago "
                               f"at severity {last_severity}")

        self._last[root_cause] = (now, severity)
        return True, ""
