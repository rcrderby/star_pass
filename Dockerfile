# star_pass Container
FROM python:3.11-slim-buster

# Set the working directory
WORKDIR /app

# Update OS package list and install git
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

# Copy the Python pip requirements file
COPY requirements/requirements.txt requirements/requirements.txt

# Upgrade pip and install requirements from the requirements file
RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements/requirements.txt && \
    rm -rf requirements

# Set the PYTHONPATH environment variable
ENV PYTHONPATH=/app

# Add other environment variables
ENV BASE_URL=https://api.galaxydigital.com/api

# Copy the /app directory to the image
COPY /app /app

# Start the bash prompt
CMD ["/bin/bash"]
