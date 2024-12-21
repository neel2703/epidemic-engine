# Use the official Python slim image
FROM python:3.10-slim

# Install necessary system packages, including Java for PySpark
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME and include it in PATH
#CHECK THIS AMD THING, TRY IT WITH BUILDX --PLATFORM AMD AND SHIT AND THEN SEE IF IT WILL WORK WITH THIS JAVA_HOME AND IT CAN RUN!!!
#then if it works, it should work on the google cloud too :D
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app

# Expose the application port
EXPOSE 5001

# Command to run the application
CMD ["python", "app.py"]
