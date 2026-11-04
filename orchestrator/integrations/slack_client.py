from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from orchestrator.config import SLACK_BOT_TOKEN, SLACK_CHANNEL


class SlackClient:
    """
    LLD 8.4: posts a structured incident message to #incidents — incident
    ID, root cause service, affected services, severity, anomaly types,
    and (when generated) the LLM runbook.

    If SLACK_BOT_TOKEN isn't set, posts are logged and skipped instead of
    raising, so the rest of the orchestrator works before a real Slack
    workspace/bot token exists.
    """

    def __init__(self):
        # timeout: the consumer loop is blocking, a hung Slack call must not stall it
        self._client = WebClient(token=SLACK_BOT_TOKEN, timeout=10) if SLACK_BOT_TOKEN else None

    def post_incident(self, incident: dict, runbook: str = None) -> bool:
        text = self._format_message(incident, runbook)

        if not self._client:
            print(f"[SlackClient] Not configured (no SLACK_BOT_TOKEN) — would post:\n{text}")
            return False

        try:
            self._client.chat_postMessage(channel=SLACK_CHANNEL, text=text)
            print(f"[SlackClient] Posted incident {incident['incident_id'][:8]} to {SLACK_CHANNEL}")
            return True
        except SlackApiError as e:
            print(f"[SlackClient] Failed to post: {e.response['error']}")
            return False

    def _format_message(self, incident: dict, runbook: str = None) -> str:
        lines = [
            f"*Incident {incident['incident_id'][:8]}* — severity {incident.get('severity', '?')}",
            f"Root cause: `{incident['root_cause_service']}` (method: {incident['causality_method']})",
            f"Affected services ({len(incident['member_service_ids'])}): {', '.join(incident['member_service_ids'])}",
            f"Anomaly types: {', '.join(incident.get('anomaly_types', []))}",
        ]
        if runbook:
            lines.append(f"\n*Suggested runbook:*\n{runbook}")
        return "\n".join(lines)