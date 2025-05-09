# Dockerfile
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy the application files
COPY . /app

# Install the required dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV RABBITMQ_HOST=integrationproject-2425s2-001.westeurope.cloudapp.azure.com
ENV RABBITMQ_PORT=30020
ENV RABBITMQ_USER=ehbstudent
ENV RABBITMQ_PASSWORD=your_password_here
ENV DB_HOST=integrationproject-2425s2-001.westeurope.cloudapp.azure.com
ENV DB_USER=admin
ENV DB_PASSWORD=Admin123!
ENV DB_NAME=facturatie

# Run the message processor
CMD ["python", "message_processor.py"]
