# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
password=$1
s3_bucket=$2

if [ -n "$1" ]; then
    password="$1"
else
    echo "password can't be empty!"
    exit 1
fi

if [ -n "$2" ]; then
    s3_bucket="$2"
else
    echo "s3 bucket cannot be empty!"
    exit 1
fi

rm VehicleInventoryApp/.env
rm ImageServiceApp/.env
rm .env

echo "MYSQL_ROOT_PASSWORD=${password}" >> VehicleInventoryApp/.env
echo "MYSQL_DATABASE=vehicle_inventory" >> VehicleInventoryApp/.env
echo "MYSQL_USER=djangouser" >> VehicleInventoryApp/.env
echo "MYSQL_PASSWORD=${password}" >> VehicleInventoryApp/.env
echo "DB_SERVICE_HOST=db" >> VehicleInventoryApp/.env
echo "DB_SERVICE_PORT=3306" >> VehicleInventoryApp/.env
echo "IMAGE_BACKEND_SERVICE_HOST=image-service-backend" >> VehicleInventoryApp/.env
echo "IMAGE_BACKEND_SERVICE_PORT=8000" >> VehicleInventoryApp/.env

echo "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" >> ImageServiceApp/.env
echo "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" >> ImageServiceApp/.env
echo "AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}" >> ImageServiceApp/.env
echo "S3_BUCKET=${s3_bucket}" >> ImageServiceApp/.env

echo "MYSQL_ROOT_PASSWORD=${password}" >> .env
echo "MYSQL_DATABASE=vehicle_inventory" >> .env
echo "MYSQL_USER=djangouser" >> .env
echo "MYSQL_PASSWORD=${password}" >> .env

docker-compose up --build
