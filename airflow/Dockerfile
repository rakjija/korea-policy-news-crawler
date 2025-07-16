FROM apache/airflow:3.0.3

USER root

# Install system-level dependencies if needed (e.g., for certain Python libraries)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#         build-essential \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Copy the requirements file into the container
COPY requirements.txt /opt/airflow/requirements.txt

# Install Python packages
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

