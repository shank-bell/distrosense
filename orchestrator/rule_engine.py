from pathlib import Path
from typing import Optional
import yaml

RULES_PATH = Path(__file__).parent / "rules.yaml"


class RuleEngine:
    """
    LLD 8.2: rule engine evaluates each incident against a YAML-defined
    rule set. Rules are evaluated in priority order (= file order); the
    first matching rule executes.
    """

    def __init__(self, rules_path: Path = RULES_PATH):
        with open(rules_path) as f:
            data = yaml.safe_load(f) or {}   # an empty rules.yaml must not crash startup
        self._rules = data.get("rules") or []

    def _matches(self, condition: dict, incident: dict) -> bool:
        if "min_severity" in condition:
            if incident.get("severity", 0) < condition["min_severity"]:
                return False
        if "anomaly_types" in condition:
            incident_types = set(incident.get("anomaly_types", []))
            if not incident_types & set(condition["anomaly_types"]):
                return False
        if "min_member_services" in condition:
            if len(incident.get("member_service_ids", [])) < condition["min_member_services"]:
                return False
        return True

    def evaluate(self, incident: dict) -> Optional[dict]:
        for rule in self._rules:
            if self._matches(rule.get("condition", {}), incident):
                return rule
        return None