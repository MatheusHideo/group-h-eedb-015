# PECE-POLI Data Engineering & BIG Data Repository 🚀

This repository contains the code for the Data Engineering and BIG Data class from the PECE-POLI **eEDB-015 Integrated Project**. This project integrates Docker containers with AWS services to build, push, and deploy containerized applications.

## Requirements 🛠️

- **Docker** 🐳
- **Docker Compose V2**

## Getting Started 🎯

Follow the steps below to set up your project environment:

### 1. AWS Configuration ☁️

- **Set Up AWS Credentials:**  
  Make sure your AWS credentials are configured on your machine.

- **Create an AWS ECR Repository:**  
  Create an AWS ECR repository and copy its URL for later use.

### 2. Environment Variables 📝

Set the environment variable `AWS_ECR_URL` with your ECR repository URL. This variable is used for building and deploying the containers.

- **Linux:**
  ```bash
  export AWS_ECR_URL=${your_ecr_url}
  ```

- **Windows:**  
  Create the `AWS_ECR_URL` environment variable via System Properties or your preferred method.

### 3. Environment File 📄

Inside the `app` directory, create a `.env` file and add the following line:
```
AWS_ECR_URL=${your_ecr_url}
```

### 4. Build the Containers 🏗️

Build the containers defined in the repository using Docker Compose:

- **Linux:**
  ```bash
  docker compose -f ./app/docker-compose.yaml build
  ```

- **Windows:**  
  Use the equivalent Docker Compose command according to your system setup.

### 5. Push the Containers 📦

Push the built containers to your AWS ECR repository:

- **Linux:**
  ```bash
  docker compose push
  ```

- **Windows:**  
  Use the appropriate command for Docker Compose on your system.

### 6. Deploy with CloudFormation 🚀

Deploy the AWS resources using CloudFormation. Replace the placeholders with your actual values before executing the command:

```bash
aws cloudformation deploy --template-file ./app/cloud_formation.yaml --stack-name ProjectIntegrator --parameter-overrides LabRole=arn:aws:iam::${your_aws_id}:role/LabRole --parameter-overrides EcrUrl=$AWS_ECR_URL --capabilities CAPABILITY_NAMED_IAM
```

**Note:**  
- Replace `${your_aws_id}` with your actual AWS account ID.  
- Ensure your `AWS_ECR_URL` environment variable is correctly set.  

### 7. Initiate the Step Function 🔄

After deploying, navigate to the AWS Step Functions console and initiate the workflow steps as configured in your project.

## Repository Structure 📁

