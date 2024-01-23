# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# Use an official Python runtime as a parent image
FROM python:3.10

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory
WORKDIR /vehicle-inventory-app

# Install dependencies
COPY VehicleInventoryApp/requirements.txt /vehicle-inventory-app/
RUN pip install -r requirements.txt

# Copy the project code into the container
COPY VehicleInventoryApp/. /vehicle-inventory-app/