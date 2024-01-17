# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
password=$1

rm VehicleInventoryApp/.env
rm ImageServiceApp/.env

echo "MYSQL_ROOT_PASSWORD=${password}" >> VehicleInventoryApp/.env
echo "MYSQL_DATABASE=vehicle_inventory" >> VehicleInventoryApp/.env
echo "MYSQL_USER=djangouser" >> VehicleInventoryApp/.env
echo "MYSQL_PASSWORD=${password}" >> VehicleInventoryApp/.env
echo "DB_SERVICE_HOST=db" >> VehicleInventoryApp/.env
echo "DB_SERVICE_PORT=3306" >> VehicleInventoryApp/.env
echo "IMAGE_BACKEND_SERVICE_HOST=0.0.0.0" >> VehicleInventoryApp/.env
echo "IMAGE_BACKEND_SERVICE_PORT=8000" >> VehicleInventoryApp/.env

echo "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" >> ImageServiceApp/.env
echo "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" >> ImageServiceApp/.env
echo "AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}" >> ImageServiceApp/.env


docker compose -f VehicleInventoryApp/docker-compose.yaml up --build&
docker compose -f ImageServiceApp/docker-compose.yaml up --build&
