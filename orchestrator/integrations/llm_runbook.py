import time
from typing import List, Optional
import requests
from orchestrator.config import LLM_API_KEY, LLM_MODEL, LLM_PROVIDER

XAI_CHAT_URL = "https://api.x.ai/v1/chat/completions"
LLM_TIMEOUT_SECONDS = 20     # worst case blocks the consumer loop for ATTEMPTS * this + delay
LLM_ATTEMPTS = 2
LLM_RETRY_DELAY_SECONDS = 2
_PLACEHOLDER_KEYS = {"", "your-key-here"}


class RunbookSuggester:
    """
    LLD 8.3: LLM-powered runbook suggester. Builds a prompt from the
    incident summary, affected services, and the relevant Neo4j subgraph,
    calls the configured LLM, and returns a natural-language runbook for
    the Slack alert.

    Providers (LLM_PROVIDER, or inferred from the key prefix):
      - "gemini": the current `google-genai` SDK (not the deprecated
        `google-generativeai` package).
      - "xai":    Grok via xAI's OpenAI-compatible chat-completions API.
    """

    def __init__(self):
        self._provider = LLM_PROVIDER
        self._gemini = None
        self._configured = LLM_API_KEY not in _PLACEHOLDER_KEYS and self._provider in ("gemini", "xai")
        if self._configured and self._provider == "gemini":
            from google import genai  # lazy: not needed when using xAI
            from google.genai import types
            # The consumer loop is single-threaded and blocking: without a
            # timeout one hung LLM call would stall every incident behind it.
            self._gemini = genai.Client(
                api_key=LLM_API_KEY,
                http_options=types.HttpOptions(timeout=LLM_TIMEOUT_SECONDS * 1000),
            )

    def generate(self, incident: dict, related_services: Optional[List[str]] = None) -> str:
        if not self._configured:
            return "(LLM runbook suggester not configured — set LLM_API_KEY)"

        prompt = self._build_prompt(incident, related_services or [])
        call = self._call_xai if self._provider == "xai" else self._call_gemini
        for attempt in range(1, LLM_ATTEMPTS + 1):
            try:
                return call(prompt)
            except Exception as e:
                # Never let the key leak into logs via an echoed request/error.
                msg = str(e).replace(LLM_API_KEY, "<redacted>")
                print(f"[RunbookSuggester] {self._provider} call failed "
                      f"(attempt {attempt}/{LLM_ATTEMPTS}): {msg}")
                if attempt < LLM_ATTEMPTS:
                    time.sleep(LLM_RETRY_DELAY_SECONDS)   # 503 "high demand" spikes are transient
        return "(Runbook generation failed — see logs)"

    def _call_gemini(self, prompt: str) -> str:
        response = self._gemini.models.generate_content(model=LLM_MODEL, contents=prompt)
        return response.text.strip()

    def _call_xai(self, prompt: str) -> str:
        r = requests.post(
            XAI_CHAT_URL,
            headers={"Authorization": f"Bearer {LLM_API_KEY}"},
            json={
                "model": LLM_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.2,
            },
            timeout=LLM_TIMEOUT_SECONDS,
        )
        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
        return r.json()["choices"][0]["message"]["content"].strip()

    def _build_prompt(self, incident: dict, related_services: List[str]) -> str:
        prompt = (
            "You are an SRE on-call assistant. A monitoring system detected the "
            "following incident in a microservices platform. Write a short, "
            "actionable runbook (3-5 numbered steps) for the on-call engineer to "
            "investigate and mitigate it.\n\n"
            f"Root cause service: {incident['root_cause_service']}\n"
            f"Root cause method: {incident['causality_method']}\n"
            f"Severity: {incident.get('severity', 'unknown')}/5\n"
            f"Anomaly types observed: {', '.join(incident.get('anomaly_types', []))}\n"
            f"Affected services ({len(incident['member_service_ids'])}): "
            f"{', '.join(incident['member_service_ids'])}\n"
        )
        if related_services:
            prompt += (
                f"\nServices directly connected to the root cause in the "
                f"dependency graph: {', '.join(related_services)}\n"
            )
        return prompt
