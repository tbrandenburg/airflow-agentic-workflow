FROM node:24.15.0-bookworm-slim AS node
WORKDIR /runtime
COPY packages/agent-runtime/package*.json ./
RUN npm ci
COPY packages/agent-runtime/ ./
COPY packages/agent-contracts/schemas/v1/role-response.schema.json /agent-contracts/schemas/v1/role-response.schema.json
RUN npm run build && npm prune --omit=dev
FROM apache/airflow:3.3.2-python3.12
USER root
COPY --from=node /usr/local/ /usr/local/
COPY --from=node /runtime /deployment/agent-runtime
RUN apt-get update && apt-get install -y --no-install-recommends git bash curl jq shellcheck \
    && rm -rf /var/lib/apt/lists/* \
    && npm install --global opencode-ai@1.18.32 \
    && npm cache clean --force
ENV OPENCODE_DISABLE_AUTOUPDATE=true
USER airflow
RUN pip install --no-cache-dir boto3==1.43.75 'boto3-stubs[s3]==1.43.75' ruff==0.15.6 pytest==9.0.2
ENV PYTHONPATH=/deployment/airflow-core
RUN node --version && opencode --version && git --version && bash --version \
    && python -m compileall -q /opt/airflow
