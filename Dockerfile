# Use the official Python base image
FROM python:3.11

# Set the working directory
WORKDIR /app

# Copy requirements.txt and install required packages
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set the entry point to main.py
ENTRYPOINT [ "python", "/app/main.py" ]
