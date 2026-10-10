FROM python:3.12.13-slim-bookworm
WORKDIR /proxy
COPY proxy.py policy.json ./
USER 65532:65532
CMD ["python", "-B", "/proxy/proxy.py"]
