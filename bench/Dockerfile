FROM python:3.12-slim-bookworm
WORKDIR /bench
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY benchmark.py .
ENTRYPOINT ["python", "/bench/benchmark.py"]
