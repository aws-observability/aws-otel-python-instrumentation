# Use an official Python runtime as a parent image
FROM python:3.10

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory
WORKDIR /image-service-app

# Install dependencies
COPY ImageServiceApp/requirements.txt /image-service-app/
RUN pip install -r requirements.txt

# Copy the project code into the container
COPY ImageServiceApp/. /image-service-app/