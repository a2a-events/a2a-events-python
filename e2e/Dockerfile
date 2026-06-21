# Image for the multi-container end-to-end test: the a2a-events package plus the
# server/client/postgres extras, running one of the e2e service modules.
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir uv

# Install dependencies first (cached) from the project metadata. The build
# context is the repo root; pyproject.toml resolves its readme = "README.md"
# locally, so copy it alongside.
COPY README.md ./README.md
COPY pyproject.toml ./pyproject.toml
COPY src ./src
RUN uv pip install --system --no-cache ".[server,client,postgres]"

COPY e2e ./e2e

# The service module (publisher_service / subscriber_service) is chosen per run.
CMD ["uvicorn", "e2e.publisher_service:app", "--host", "0.0.0.0", "--port", "8000"]
