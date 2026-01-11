FROM python:3.11-alpine

WORKDIR /app

# Install system dependencies
RUN apk add --no-cache \
    postgresql-client \
    gcc \
    musl-dev \
    postgresql-dev

# Copy requirements and install Python dependencies
COPY services/cadastral-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy common modules
COPY services/common /app/common
COPY services/task-queue /app/task-queue

# Copy application code
COPY services/cadastral-api/*.py /app/

# Create compatibility symlinks for legacy imports
RUN ln -sf /app/common /common && ln -sf /app/task-queue /task-queue

# Create non-root user
RUN adduser -D -u 1000 cadastral && chown -R cadastral:cadastral /app
USER cadastral

EXPOSE 5000

CMD ["python", "cadastral_api.py"]

