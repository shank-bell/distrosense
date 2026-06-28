import asyncio
import random
import signal
from simulator.config import (
    NUM_SERVICES,
    EMIT_INTERVAL_SECONDS,
    ANOMALY_PROBABILITY,
    CASCADE_PROBABILITY,
)
from simulator.services.service_registry import build_registry
from simulator.services.cascade_map import build_cascade_map, get_affected_services
from simulator.generators.metric_generator import generate_metrics
from simulator.generators.anomaly_injector import maybe_inject
from simulator.generators.trace_generator import generate_span
from simulator.publisher.kafka_publisher import KafkaPublisher


async def run_service(
    service,
    publisher: KafkaPublisher,
    cascade_map: dict,
    all_services: dict,
):
    while True:
        try:
            metrics = generate_metrics(service)
            metrics, anomaly_type = maybe_inject(metrics, ANOMALY_PROBABILITY)
            span = generate_span(service.service_id, metrics, anomaly_type)
            await publisher.publish(span, service.service_id)

            if anomaly_type and random.random() < CASCADE_PROBABILITY:
                affected = get_affected_services(service.service_id, cascade_map)
                for affected_id in affected:
                    if affected_id in all_services:
                        affected_svc = all_services[affected_id]
                        affected_metrics = generate_metrics(affected_svc)
                        affected_metrics, _ = maybe_inject(affected_metrics, 0.8)
                        affected_span = generate_span(affected_id, affected_metrics, "CASCADE")
                        await publisher.publish(affected_span, affected_id)

            await asyncio.sleep(EMIT_INTERVAL_SECONDS)

        except Exception as e:
            print(f"[{service.service_id}] Error: {e}")
            await asyncio.sleep(2)


async def main():
    print(f"[Simulator] Starting {NUM_SERVICES} services...")

    registry = build_registry()
    service_ids = list(registry.keys())
    cascade_map = build_cascade_map(service_ids)

    publisher = KafkaPublisher()
    await publisher.start()

    tasks = [
        asyncio.create_task(
            run_service(service, publisher, cascade_map, registry)
        )
        for service in registry.values()
    ]

    print(f"[Simulator] {len(tasks)} asyncio tasks running. Ctrl+C to stop.")

    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def shutdown():
        print("\n[Simulator] Shutting down...")
        stop_event.set()

    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)

    await stop_event.wait()

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await publisher.stop()
    print("[Simulator] Stopped.")


if __name__ == "__main__":
    asyncio.run(main())