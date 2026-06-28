import uuid
import random
from simulator.services.base_service import BaseService, ServiceConfig
from simulator.config import NUM_SERVICES, SERVICE_TIERS, SERVICE_TEAMS

SERVICE_NAMES = [
    "api-gateway", "auth-service", "user-service", "payment-service",
    "order-service", "inventory-service", "cart-service", "checkout-service",
    "notification-service", "email-service", "sms-service", "search-service",
    "recommendation-service", "pricing-service", "discount-service",
    "shipping-service", "tracking-service", "warehouse-service",
    "supplier-service", "catalog-service", "review-service", "rating-service",
    "media-service", "upload-service", "cdn-service", "cache-service",
    "session-service", "token-service", "oauth-service", "profile-service",
    "address-service", "billing-service", "invoice-service", "tax-service",
    "currency-service", "fraud-service", "risk-service", "kyc-service",
    "reporting-service", "analytics-service", "metrics-service", "logging-service",
    "audit-service", "config-service", "feature-flag-service", "ab-test-service",
    "experiment-service", "ml-service", "inference-service", "training-service",
    "data-pipeline", "etl-service", "stream-processor", "batch-processor",
    "scheduler-service", "job-service", "worker-service", "queue-service",
    "event-bus", "pubsub-service", "webhook-service", "integration-service",
    "crm-service", "erp-service", "support-service", "chat-service",
    "feed-service", "timeline-service", "social-service", "follow-service",
    "like-service", "comment-service", "share-service", "post-service",
    "story-service", "live-service", "video-service", "audio-service",
    "transcoding-service", "thumbnail-service", "ocr-service", "vision-service",
    "nlp-service", "translation-service", "sentiment-service", "classification-service",
    "matching-service", "scoring-service", "ranking-service", "filter-service",
    "moderation-service", "compliance-service", "legal-service", "gdpr-service",
    "backup-service", "restore-service", "archive-service", "purge-service",
    "health-service", "readiness-service", "liveness-service", "monitor-service",
]


def build_registry() -> dict[str, BaseService]:
    registry = {}
    for i, name in enumerate(SERVICE_NAMES[:NUM_SERVICES]):
        config = ServiceConfig(
            service_id=f"svc-{str(uuid.uuid4())[:8]}",
            name=name,
            tier=random.choice(SERVICE_TIERS),
            team=random.choice(SERVICE_TEAMS),
            base_cpu=random.uniform(10, 60),
            base_latency=random.uniform(20, 200),
            base_error_rate=random.uniform(0.001, 0.05),
            base_request_rate=random.uniform(50, 500),
        )
        service = BaseService(config)
        registry[config.service_id] = service
    return registry