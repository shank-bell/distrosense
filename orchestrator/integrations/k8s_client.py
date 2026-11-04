import time
from kubernetes import client, config

# Maps a correlation-engine service_id to the real Kubernetes Rollout
# deployed for it (see orchestrator/k8s/*.yaml). Only these four services
# have a real workload behind them — the other ~96 simulated services have
# no k8s deployment to act on, so actions against them are skipped rather
# than faked.
SERVICE_ID_TO_ROLLOUT = {
    "svc-a0bc923b": "payment-service",
    "svc-be899d1f": "order-service",
    "svc-02c8974b": "inventory-service",
    "svc-bc56eed1": "auth-service",
}

ROLLOUT_GROUP = "argoproj.io"
ROLLOUT_VERSION = "v1alpha1"
ROLLOUT_PLURAL = "rollouts"
REVERT_RETRY_SECONDS = 60


class DisabledK8sClient:
    """
    Stand-in used when no kubeconfig / cluster is available, so the orchestrator
    still runs Slack-only (rules, Slack alerts, LLM runbooks) instead of
    refusing to start. k8s actions are reported as skipped, never faked.
    """

    def __init__(self, reason: str):
        self.reason = reason

    def rollout_name_for(self, service_id: str):
        return None

    def trigger_hpa(self, service_id: str, min_replicas: int) -> bool:
        print(f"[K8sClient] k8s unavailable ({self.reason}) — skipping HPA trigger for {service_id}")
        return False

    def rollback_canary(self, service_id: str) -> bool:
        print(f"[K8sClient] k8s unavailable ({self.reason}) — skipping canary rollback for {service_id}")
        return False

    def revert_expired(self):
        pass


class K8sClient:
    """
    LLD 8.4: k8s HPA trigger and canary rollback, against a real cluster.

    HPA trigger patches the HorizontalPodAutoscaler's minReplicas directly
    (LLD's "forcing an immediate scale-up"), remembers the original value,
    and puts it back via revert_expired() once `revert_after_seconds` passes.
    Canary rollback sets the Argo Rollout's status.abort field, which is how
    `kubectl argo rollouts abort` itself works — it tells the controller to
    immediately revert traffic to the last stable ReplicaSet rather than
    continuing the canary steps.

    Every API call has a timeout: the orchestrator's consumer loop is
    single-threaded, so a hung API server must not stall all incident handling.
    """

    def __init__(self, namespace: str, context: str = None,
                 revert_after_seconds: int = 900, request_timeout: int = 10):
        config.load_kube_config(context=context)
        self._namespace = namespace
        self._revert_after = revert_after_seconds
        self._timeout = request_timeout
        self._autoscaling = client.AutoscalingV2Api()
        self._custom = client.CustomObjectsApi()
        # rollout name -> (original minReplicas, epoch time to put it back).
        # In-memory: an orchestrator restart forgets pending reverts.
        self._raised: dict = {}

    def rollout_name_for(self, service_id: str):
        return SERVICE_ID_TO_ROLLOUT.get(service_id)

    def _patch_hpa_min(self, name: str, min_replicas: int):
        self._autoscaling.patch_namespaced_horizontal_pod_autoscaler(
            name=name,
            namespace=self._namespace,
            body={"spec": {"minReplicas": min_replicas}},
            _request_timeout=self._timeout,
        )

    def trigger_hpa(self, service_id: str, min_replicas: int) -> bool:
        rollout_name = self.rollout_name_for(service_id)
        if not rollout_name:
            print(f"[K8sClient] No real deployment for {service_id} — skipping HPA trigger")
            return False

        revert_at = time.time() + self._revert_after
        if rollout_name in self._raised:
            # Already raised by an earlier incident: just extend the hold.
            original, _ = self._raised[rollout_name]
            self._raised[rollout_name] = (original, revert_at)
            print(f"[K8sClient] {rollout_name}: HPA already raised, revert pushed back")
            return True

        current = self._autoscaling.read_namespaced_horizontal_pod_autoscaler(
            name=rollout_name, namespace=self._namespace, _request_timeout=self._timeout,
        ).spec.min_replicas or 1
        if current >= min_replicas:
            print(f"[K8sClient] {rollout_name}: HPA minReplicas already {current} (>= {min_replicas})")
            return True

        self._patch_hpa_min(rollout_name, min_replicas)
        self._raised[rollout_name] = (current, revert_at)
        print(f"[K8sClient] {rollout_name}: HPA minReplicas {current} -> {min_replicas} "
              f"(reverts in {self._revert_after}s)")
        return True

    def revert_expired(self):
        """Put back minReplicas for any HPA whose hold has expired. Call regularly."""
        now = time.time()
        for rollout_name, (original, revert_at) in list(self._raised.items()):
            if now < revert_at:
                continue
            try:
                self._patch_hpa_min(rollout_name, original)
                del self._raised[rollout_name]
                print(f"[K8sClient] {rollout_name}: HPA minReplicas reverted to {original}")
            except Exception as e:
                self._raised[rollout_name] = (original, now + REVERT_RETRY_SECONDS)
                print(f"[K8sClient] {rollout_name}: HPA revert failed ({type(e).__name__}: {e}) — retrying")

    def rollback_canary(self, service_id: str) -> bool:
        rollout_name = self.rollout_name_for(service_id)
        if not rollout_name:
            print(f"[K8sClient] No real deployment for {service_id} — skipping canary rollback")
            return False

        # Abort only means something while a canary is in flight. On a fully
        # promoted ("Healthy") rollout the controller just ignores the flag, so
        # don't claim a rollback happened.
        rollout = self._custom.get_namespaced_custom_object(
            group=ROLLOUT_GROUP, version=ROLLOUT_VERSION, namespace=self._namespace,
            plural=ROLLOUT_PLURAL, name=rollout_name, _request_timeout=self._timeout,
        )
        phase = (rollout.get("status") or {}).get("phase")
        if phase == "Healthy":
            print(f"[K8sClient] {rollout_name}: no canary in flight (phase Healthy) — nothing to roll back")
            return False

        # Rollout has a status subresource, so the patch must go through
        # the *_status API — patching the main object won't touch .status.
        self._custom.patch_namespaced_custom_object_status(
            group=ROLLOUT_GROUP,
            version=ROLLOUT_VERSION,
            namespace=self._namespace,
            plural=ROLLOUT_PLURAL,
            name=rollout_name,
            body={"status": {"abort": True}},
            _request_timeout=self._timeout,
        )
        print(f"[K8sClient] {rollout_name}: canary rollback (abort) triggered")
        return True
