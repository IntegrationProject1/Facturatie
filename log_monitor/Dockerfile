FROM python:3.10-slim

WORKDIR /app
COPY logger.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "logger.py"]
