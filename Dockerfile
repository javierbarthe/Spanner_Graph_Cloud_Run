# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file to the working directory
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Add a build argument for the application file
ARG APP_FILE=spanner_graph_run_DQ.py
ENV APP_FILE=${APP_FILE}

# Copy the content of the local src directory to the working directory
COPY ${APP_FILE} .

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV PORT 8080

# Run app.py when the container launches
CMD ["/bin/sh", "-c", "exec gunicorn --bind 0.0.0.0:8080 --timeout 0 ${APP_FILE%.py}:app"]
