# invoice-processor/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY invoice_processor.py .

CMD ["python", "invoice_processor.py"]