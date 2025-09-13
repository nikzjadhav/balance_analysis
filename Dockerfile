FROM bitnami/spark:3.3.1

# Set working directory
WORKDIR /app

# Switch to root to install packages
USER root

# Install Python3, pip and unzip (if not already installed)
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    unzip

# Install Streamlit and any other Python dependencies from requirements.txt
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Copy your source code
COPY src/ ./src/

# Create directories for data and output
RUN mkdir -p /app/data /app/output

# Expose Streamlit port
EXPOSE 8501

# By default run your Spark job; to run Streamlit override entrypoint in docker-compose or CLI
CMD ["spark-submit", "--master", "local[*]", "src/main.py"]
