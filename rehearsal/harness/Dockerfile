FROM python:3.12-slim
# The SDK version robin's server pins (server/requirements.txt).
RUN pip install --no-cache-dir temporalio==1.5.0
WORKDIR /harness
ENV PYTHONUNBUFFERED=1
CMD ["python", "/harness/harness.py", "worker"]
