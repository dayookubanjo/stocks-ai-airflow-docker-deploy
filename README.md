# Steps to startup Airflow: 


## Build the Spark image: 
docker build -f Dockerfile.Spark . -t spark-air

## Build the Airflow image: 
docker build -f Dockerfile.Airflow . -t airflow-spark

## Run the commands below
mkdir ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

## Enter your environment variables into .env file
AIRFLOW_UID=501
AIRFLOW_GID=0
AWS_ACCESS_KEY=‘XXXXXXXXXXX’
AWS_SECRET_KEY=‘XXXXXXXXXXX’

## Initialize Airflow: 
docker-compose -f docker-compose.Spark.yaml -f docker-compose.Airflow.yaml up airflow-init

## Run Spark and Airflow Containers:

docker-compose -f docker-compose.Spark.yaml -f docker-compose.Airflow.yaml up

## Access Airflow (User: airflow , Password: airflow):

http://localhost:8080/ 

## Access Spark:

http://localhost:8090/   
