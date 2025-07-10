FROM apache/airflow:3.0.2

# Copy your requirements
COPY requirements.txt .
# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt
