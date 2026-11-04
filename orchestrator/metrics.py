from prometheus_client import Counter

INCIDENTS     = Counter("distrosense_orchestrator_incidents_total", "Incidents consumed, by what happened to them", ["outcome"])
RULE_MATCHES  = Counter("distrosense_orchestrator_rule_matches_total", "Incidents matched to a rule", ["rule"])
ACTIONS       = Counter("distrosense_orchestrator_actions_total", "Action executions", ["action", "result"])
