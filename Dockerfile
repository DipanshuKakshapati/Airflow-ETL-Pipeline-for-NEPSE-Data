# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.9.2

# Copy requirements.txt to the image
COPY requirements.txt /requirements.txt

# Install any additional dependencies you need
RUN pip install --no-cache-dir -r /requirements.txt

# Set the default command
CMD ["webserver"]


