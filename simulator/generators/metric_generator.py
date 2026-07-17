import numpy as np
from simulator.services.base_service import BaseService

AUTOCORR = 0.9                   # AR(1) coefficient — same value used in the training data generator
SEASONAL_PERIOD_SECONDS = 500    # one "cycle" every ~8 minutes (EMIT_INTERVAL is 1s) — same period used in training
SEASONAL_AMPLITUDE = 0.3

# An AR(1) series driven by noise of scale S has stationary std = S / sqrt(1 - rho^2).
# Setting S = sqrt(1 - rho^2) makes the AR(1) series itself unit-variance, so it composes
# cleanly with the (roughly unit-scale) seasonal term. This is the live-stream equivalent
# of the post-hoc np.std() normalization used in the batch training data generator — that
# approach isn't available here since there's no full series to normalize against ahead of time.
_AR1_NOISE_SCALE = np.sqrt(1 - AUTOCORR ** 2)


def _step_ar1(prev_state: float, scale: float = 1.0) -> float:
    noise = np.random.normal(0, _AR1_NOISE_SCALE * scale)
    return AUTOCORR * prev_state + noise


def _init_state(service: BaseService):
    service._sim_tick      = 0
    service._load_state    = 0.0
    service._cpu_state     = 0.0
    service._latency_state = 0.0
    service._error_state   = 0.0
    service._req_state     = 0.0


def generate_metrics(service: BaseService) -> dict:
    """
    Generate realistic metrics with autocorrelation, seasonality, and
    cross-metric correlation — mirrors the realism added to the ML
    training data generator (ml_service/training/dataset.py), so the
    live simulator and the offline training data now share the same
    statistical structure instead of the simulator staying pure i.i.d.
    Gaussian noise while training used something more realistic.

    State (AR1 memory, tick counter) is stored directly on the service
    object, since `service` is the same long-lived instance reused every
    tick in main.py's run loop — not recreated per call.
    """
    if not hasattr(service, "_sim_tick"):
        _init_state(service)

    cfg = service.config

    service._sim_tick += 1
    seasonal = SEASONAL_AMPLITUDE * np.sin(2 * np.pi * service._sim_tick / SEASONAL_PERIOD_SECONDS)
    service._load_state = _step_ar1(service._load_state)
    shared_load = seasonal + 0.5 * service._load_state

    # Each metric = baseline scaled by the shared load signal (this is what
    # creates cross-metric correlation — when load rises, all four metrics
    # move together) + its own small autocorrelated idiosyncratic noise.
    service._cpu_state     = _step_ar1(service._cpu_state, scale=cfg.base_cpu * 0.05)
    service._latency_state = _step_ar1(service._latency_state, scale=cfg.base_latency * 0.08)
    service._error_state   = _step_ar1(service._error_state, scale=cfg.base_error_rate * 0.1)
    service._req_state     = _step_ar1(service._req_state, scale=cfg.base_request_rate * 0.05)

    cpu = max(0.0, min(100.0,
        cfg.base_cpu * (1 + 0.2 * shared_load) + service._cpu_state
    ))
    latency = max(0.0,
        cfg.base_latency * (1 + 0.25 * shared_load) + service._latency_state
    )
    error_rate = max(0.0, min(1.0,
        cfg.base_error_rate * (1 + 0.4 * shared_load) + service._error_state
    ))
    request_rate = max(0.0,
        cfg.base_request_rate * (1 + 0.2 * shared_load) + service._req_state
    )

    return {
        "cpu_percent": round(cpu, 4),
        "latency_p99": round(latency, 4),
        "error_rate": round(error_rate, 6),
        "request_rate": round(request_rate, 4),
    }