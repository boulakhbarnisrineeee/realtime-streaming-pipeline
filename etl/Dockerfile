FROM python:3.11-slim

WORKDIR /app

COPY etl/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY etl/etl.py .

CMD ["python", "etl.py"]