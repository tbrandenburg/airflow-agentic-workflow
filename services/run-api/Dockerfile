FROM python:3.12.13-slim-bookworm
WORKDIR /app
COPY pyproject.toml /tmp/pyproject.toml
RUN python -c 'import subprocess, tomllib; dependencies = tomllib.load(open("/tmp/pyproject.toml", "rb"))["project"]["dependencies"]; subprocess.run(["pip", "install", "--no-cache-dir", *dependencies], check=True, timeout=180)'
COPY services/run-api/run_api /app/run_api
USER 1000:1000
CMD ["uvicorn", "run_api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8081", "--no-access-log"]
