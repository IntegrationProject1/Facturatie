FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt requirements.txt
COPY logger.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "logger.py"]
