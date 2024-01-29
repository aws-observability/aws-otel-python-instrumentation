# Random Traffic Generator
The traffic generator generates the following traffic:
1. Every minute, sends a single POST request to the VehicleInventoryApp and sends a single GET request.
2. Every hour, sends a burst of requests: 5 POST requests to the VehicleInventoryApp and 5 GET requests.
3. Every 5 minutes, sleeps for random amount of time between 30-60 seconds and then sends a GET request to the VehicleInventoryApp with a random throttle param between 5-20 seconds. The backend reads that throttle param and simulates throttling for that amount of time before responding to the request.
4. Every 5 minutes, sleeps for random amount of time between 30-60 seconds and then sends a GET request to the VehicleInventoryApp with an invalid car id to trigger 404 error.
5. Every 5 minutes, sleeps for random amount of time between 30-60 seconds and then sends a GET request to the ImageServiceApp with a non existent image name to trigger 500 error due to S3 Error: "An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist."

## Running locally
1. Run `npm install`
2. Run locally:
    - If you are running against you application locally, just run `node index.js`. The default endpoint is `0.0.0.0:8000` for the ImageServiceApp and `0.0.0.0:8001` for VehicleInventoryApp.
    - If you want to run against the application on EKS, before running the `node index.js`, run `export <EKS_URL>`.

## Deploying to EKS
Run `bash build.sh <account_id> <region>`. This does the following:
1. This will retrieve the endpoint from EKS ingress-nginx pod 
2. Build docker image of the traffic
3. Push the docker image to ECR 
4. Deploy the image to EKS

