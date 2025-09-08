FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gfortran libopenblas-dev liblapack-dev pkg-config \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "-u", "main_mpt.py"]
