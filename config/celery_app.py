import os
from celery import Celery
from django.conf import settings

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

app = Celery('kidbea_ml')

# Load configuration from Django settings, all keys in it that are in uppercase
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks from forecasting app
app.autodiscover_tasks(['forecasting'])

app.conf.update(
    # Broker settings
    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    # Result backend settings
    result_expires=3600,  # Results expire after 1 hour
    # Task settings
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    # Worker settings - ML tasks are CPU intensive
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=500,  # Lower than standard (more frequent cleanup)
    # Task timeout (in seconds)
    task_soft_time_limit=1800,  # 30 minutes soft limit (ML tasks are longer)
    task_time_limit=2700,  # 45 minutes hard limit
)

# Task routing - only listen to ml_tasks queue
app.conf.task_queues = (
    ('ml_tasks', {'exchange': 'ml', 'routing_key': 'ml_tasks'}),
)


@app.task(bind=True)
def debug_task(self):
    print(f'ML Worker Request: {self.request!r}')
