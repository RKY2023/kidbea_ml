FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Set working directory
WORKDIR /app

# Install system dependencies (including build tools for scipy, scikit-learn)
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    gcc \
    g++ \
    gfortran \
    libopenblas-dev \
    liblapack-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies (ML packages may take longer to build)
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 mlworker && \
    chown -R mlworker:mlworker /app && \
    mkdir -p /tmp/ml_models && \
    chown -R mlworker:mlworker /tmp/ml_models

USER mlworker

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD celery -A config.celery_app inspect active || exit 1

# Default command (can be overridden by docker-compose or Railway)
CMD ["celery", "-A", "config.celery_app", "worker", "-Q", "ml_tasks", "--loglevel=info", "--concurrency=2"]
