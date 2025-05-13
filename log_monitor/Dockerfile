FROM docker:24.0-cli

WORKDIR /app

# Installeer Python + pip
RUN apk add --no-cache python3 py3-pip

# Kopieer scripts en requirements
COPY logger.py .
COPY requirements.txt .

# Installeer Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "logger.py"]
