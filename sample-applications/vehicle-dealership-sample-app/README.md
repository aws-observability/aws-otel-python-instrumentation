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
`bash script.sh <account_id> <cluster_name> <region> <mysql_password> <s3_bucket_name>`

This will create the docker images, upload them to ECR and then create pods on EKS with those images. 

Second, run the following command and wait for a munite until ALB is ready, you should find a public ALB endpoint to access your service
```
kubectl get svc -n ingress-nginx

ingress-nginx               LoadBalancer   10.100.160.24    xxx99c97703dc47b198a8609290e59e2-2108118875.us-east-1.elb.amazonaws.com   80:32080/TCP,443:32081/TCP,9113:30959/TCP   3d19h
```
You will be able to access the app through this endpoint: xxx99c97703dc47b198a8609290e59e2-2108118875.us-east-1.elb.amazonaws.com


### EC2
TODO

### Locally with Docker
To get started, make sure you either have you AWS creds in `$HOME/.aws` or the following: `AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN` are exported.
1. Run `bash local_script.sh <mysql_pass> <s3_bucket_name>`. 
This will create docker containers, move the requirement env variables there and start them up. 

They should be accessible through `0.0.0.0:8000` for the image service and `0.0.0.0:8001` for the vehicle service. 

## APIs

The following are the APIs and what they do:
1. GET /vehicle-inventory/: returns all the vehicles entries for mysql db
2. PUT /vehicle-inventory/: puts vehicle into db. For example: `curl -X POST http://0.0.0.0:8001/vehicle-inventory/ -d '{"make": "BMW","model": "M340","year": 2022,"image_name": "newCar.jpg"}'`
3. GET /vehicle-inventory/<int>: returns vehicle entry with id = <int>
4. GET /vehicle-inventory/<int>/image: returns image file information from S3 for the specific vehicle by calling the image microservice
5. GET /images/name/<image_name>: returns image information for <image_name> from S3 if present. 
6. PUT /images/name/<image_name>: creates an empty file in S3. This is an async endpoint since it will put image name in an SQS queue and not wait for the file to be created in S3. Instead, a long running thread will poll SQS and then create the image file later. 
7. GET /image/remote-image: makes a remote http call to google.com. 