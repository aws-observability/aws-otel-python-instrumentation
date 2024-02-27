# Python Sample App 

This directory contains code for a microservices based sample app that is used to test Python Enablement. Currently, there are two services:
1. Image Service
2. Vehicle Inventory Service


## Deployment

### Prerequisite
* A docker installation that is set up to build images for AMD64 architecture:
  * Working on Mac with intel / AMD64 architecture: 
    * Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) on your laptop for building the application container images.
    * (Make sure to open Docker Desktop and complete the setup)
  * Working on Mac with Apple M1 chip / ARM64 architecture:
    * Recommended to use Cloud Desktop as Docker desktop will default to creating images for local architecture
  * Cloud Desktop
    * Docker is installed by default

* kubectl - https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html
* eksctl - https://docs.aws.amazon.com/eks/latest/userguide/eksctl.html
* python3 (>=3.10) - https://www.python.org/downloads/

### EKS
To get started with AWS EKS, you can run the one touch script as below.
`bash script.sh <account_id> <cluster_name> <region> <postgres_password> <s3_bucket_name>`

This will create the docker images, upload them to ECR and then create pods on EKS with those images. 

Second, run the following command and wait for a munite until ALB is ready, you should find a public ALB endpoint to access your service
```
kubectl get svc -n ingress-nginx

ingress-nginx               LoadBalancer   10.100.160.24    xxx99c97703dc47b198a8609290e59e2-2108118875.us-east-1.elb.amazonaws.com   80:32080/TCP,443:32081/TCP,9113:30959/TCP   3d19h
```
You will be able to access the app through this endpoint: xxx99c97703dc47b198a8609290e59e2-2108118875.us-east-1.elb.amazonaws.com


### EC2
To deploy to EC2, you will have to go through the following steps.

1. Create an S3 bucket to upload the python code to. 
2. Create a zip file of this directory and upload it to S3.
3. Set up a VPC with a public subnet and a security group accepting all traffic. 
4. Set up 2 EC2 instances all with the following configuration:
    - Running on Amazon Linux 2
    - Instance type t2.small or larger
    - A key-pair you save to your computer. When creating the first EC2 instance you will get a choice to create a new one and reuse it for the other instance.
    - Use the VPC, public subnet, and security group created in step 3
    - Enable auto-assign public IP
    - An IAM instance profile with the following permissions:
      - AmazonS3FullAccess 
      - AmazonSQSFullAccess
    - Name one vehicle-service and the other image-service
5. Go to RDS and create a Postgres DB with the following configurations:
    - Use the Dev/Test template
    - Update the Master username to `root` and create a password of your choosing. Write it down since you will need it later. 
    - In the Connectivity settings, choose the VPC and security group created in step 3. 
    - Switch to Connect to an EC2 compute instance and choose the vehicle-service EC2 instance and then create the DB.
6. Connect to the `vehicle-service` EC2 instance and run the following:
```
sudo dnf install python3.11
sudo dnf install python3.11-pip
sudo dnf install postgresql15

createdb vehicle_inventory -h <rds_url> -U root
createuser djangouser -h <rds_url> -U root

psql -h <rds_url> -d vehicle_inventory -U root

alter user djangouser with encrypted password '<password_of_your_choosing>';
grant all privileges on database vehicle_inventory to djangouser;
ALTER DATABASE vehicle_inventory OWNER TO djangouser;

aws s3 sync s3://<s3_bucket_that_has_python_code> .

cd to the vehicle microservice directory and run: 

python3.11 -m pip install -r requirements.txt

Create a .env file with the following: 
POSTGRES_ROOT_PASSWORD=<password_from_RDS_setup>
POSTGRES_DATABASE=vehicle_inventory
POSTGRES_USER=djangouser
POSTGRES_PASSWORD=<password_from_this_step>
DB_SERVICE_HOST=<RDS_DB_endpoint>
DB_SERVICE_PORT=5432
IMAGE_BACKEND_SERVICE_HOST=<image-service_ec2_public_IP>
IMAGE_BACKEND_SERVICE_PORT=8000

python3.11 manage.py migrate --noinput && python3.11 manage.py runserver 0.0.0.0:8001
```
7. Go to the EC2 console and select the `image-service`, Go Actions -> Networking -> Connect RDS database.
8. Connect to the `image-service` EC2 instance and run the following:
```
sudo dnf install python3.11
sudo dnf install python3.11-pip

aws s3 sync s3://<s3_bucket_that_has_python_code> .

cd to the image microservice directory and run: 

python3.11 -m pip install -r requirements.txt

Create a .env file with the following: 
S3_BUCKET=<s3_bucket_to_host_images>

python3.11 manage.py migrate --noinput && python3.11 manage.py runserver 0.0.0.0:8000
```

Now you should be able to access the APIs below through the EC2 addr:port of each service.

### Locally with Docker
To get started, make sure you either have you AWS creds in `$HOME/.aws` or the following: `AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN` are exported.
1. Run `bash local_script.sh <postgres_pass> <s3_bucket_name>`. 
This will create docker containers, move the requirement env variables there and start them up. 

They should be accessible through `0.0.0.0:8000` for the image service and `0.0.0.0:8001` for the vehicle service. 

## APIs

The following are the APIs and what they do:
1. `GET /vehicle-inventory/heatlh-check/`: returns 200 if the vehicle service is up and running.
2. `GET /vehicle-inventory/`: returns all the vehicles entries for postgres db
3. `POST /vehicle-inventory/`: puts vehicle into db. For example: `curl -X POST http://0.0.0.0:8001/vehicle-inventory/ 
   -d '{"make": "BMW","model": "M340","year": 2022,"image_name": "newCar.jpg"}'`
4. `GET /vehicle-inventory/<int>`: returns vehicle entry with id = <int>
5. `DELETE /vehicle-inventory/<int>`: deletes vehicle entry with id = <int>
6. `GET /vehicle-inventory/name/<str>`: returns vehicle entries with name = <str>
7. `GET /vehicle-inventory/<int>/image`: returns image file information from S3 for the specific vehicle by calling 
   the image microservice
8. `GET /vehicle-inventory/image/<image_name>`: returns image information for <image_name> from S3 if present through 
   the image service.
9. `POST /vehicle-inventory/image/<image_name>`: Calls the image service API `POST /images/name/<image_name>`
10. `GET /vehicle-inventory/history/`: returns all the vehicle purchase history entries from postgres db
11. `POST /vehicle-inventory/history/`: puts vehicle purchase history into db. For example: `curl -X POST http://0.0.0.
   0:8001/vehicle-inventory/history/ -d '{"vehicle_id": "1","purchase_price": "66000"}'`
12. `GET /vehicle-inventory/history/<int>`: returns vehicle purchase history entry with id = <int>
13. `DELETE /vehicle-inventory/history/<int>`: deletes vehicle purchase history entry entry with id = <int>
14. `GET /images/name/<image_name>`: returns image information for <image_name> from S3 if present. 
15. `POST /images/name/<image_name>`: creates an empty file in S3. This is an async endpoint since it will put image 
   name in an SQS queue and not wait for the file to be created in S3. Instead, a long running thread will poll SQS 
   and then create the image file later. 
16. `GET /image/remote-image`: makes a remote http call to google.com. 