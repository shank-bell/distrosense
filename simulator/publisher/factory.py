from simulator.config import PUBLISH_MODE


def create_publisher():
    """grpc (default): via the ingestion service.  kafka: direct JSON, no ingestion."""
    if PUBLISH_MODE == "kafka":
        from simulator.publisher.kafka_publisher import KafkaPublisher
        print("[Simulator] SIM_PUBLISH_MODE=kafka — publishing JSON straight to Kafka (bypassing ingestion)")
        return KafkaPublisher()
    if PUBLISH_MODE == "grpc":
        from simulator.publisher.grpc_publisher import GrpcPublisher
        return GrpcPublisher()
    raise ValueError(f"SIM_PUBLISH_MODE must be 'grpc' or 'kafka', got {PUBLISH_MODE!r}")
