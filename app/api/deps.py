from app.services.jobs import Publisher
from app.tasks.worker import publish_job


def get_publisher() -> Publisher:
    """Dependency that returns the function used to enqueue jobs.

    Tests override this to simulate a broker outage without touching Redis.
    """
    return publish_job
