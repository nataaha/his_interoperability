import requests
import json

# Set the Spark Master URL
spark_master_url = "http://your-spark-master-url:6066"

# Define the Python code you want to run
python_code = """
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("PythonScript").getOrCreate()

# Your Python code here
result = [1, 2, 3, 4, 5]

# Stop the Spark session
spark.stop()

result
"""

# Create a Spark job request payload with the Python code
job_request = {
    "action": "CreateSubmissionRequest",
    "appResource": "local:/path/to/your/python/script.py",
    "appArgs": [],
    "clientSparkVersion": "3.0.0",  # Set to your Spark version
    "environmentVariables": {
        "SPARK_HOME": "/path/to/your/spark/home",
        "JAVA_HOME": "/path/to/your/java/home",
    },
    "mainClass": "org.apache.spark.deploy.SparkSubmit",
    "sparkProperties": {
        "spark.submit.pyFiles": "/path/to/your/python/script.py",
        "spark.driver.supervise": "false",
        "spark.app.name": "PythonScript",
    },
    "sparkConf": {},
}

# Send the job submission request
response = requests.post(f"{spark_master_url}/v1/submissions/create", json=job_request)

# Check if the job submission was successful
if response.status_code == 200:
    submission_response = response.json()
    submission_id = submission_response["submissionId"]
    print(f"Job submitted successfully. Submission ID: {submission_id}")

    # Wait for the job to complete (you may implement this part based on your requirements)

    # Get the job result
    result_response = requests.get(f"{spark_master_url}/v1/submissions/status/{submission_id}")
    result_json = result_response.json()
    if "driverState" in result_json and result_json["driverState"] == "FINISHED":
        result_url = result_json["driverLogs"]
        result_response = requests.get(result_url)
        result = result_response.text
        print(f"Job result: {result}")
    else:
        print(f"Job failed. Driver state: {result_json.get('driverState')}")
else:
    print(f"Job submission failed. Status code: {response.status_code}")
    print(response.text)


import requests
import json
from kubernetes import client, config

# Load Kubernetes configuration from the default location (e.g., ~/.kube/config)
config.load_kube_config()

# Define the Kubernetes namespace where the Spark application is running
namespace = "your-spark-namespace"

# Define the Spark application name and namespace
spark_app_name = "your-spark-app-name"
spark_app_namespace = "your-spark-app-namespace"

# Define the Spark REST API service name and port (default is 4040)
spark_api_service_name = "spark-webui"
spark_api_service_port = 4040

# Set up the Kubernetes API client
api_instance = client.CoreV1Api()

# Get the Spark application driver pod name
pod_list = api_instance.list_namespaced_pod(
    namespace=spark_app_namespace,
    label_selector=f"spark-app-name={spark_app_name}",
)

if len(pod_list.items) == 0:
    print("No Spark application pods found.")
    exit(1)

driver_pod_name = pod_list.items[0].metadata.name

# Construct the URL for the Spark REST API from the driver pod's service
api_url = f"http://{driver_pod_name}.{spark_api_service_name}:{spark_api_service_port}"

# Define the Python code you want to run
python_code = """
# Your Python code here
result = [1, 2, 3, 4, 5]
result
"""

# Create a Spark job request payload with the Python code
job_request = {
    "action": "CreateSubmissionRequest",
    "appResource": "local:/path/to/your/python/script.py",  # Change this to your Python script location
    "appArgs": [],
    "clientSparkVersion": "3.0.0",  # Set to your Spark version
    "environmentVariables": {
        "SPARK_HOME": "/path/to/your/spark/home",
        "JAVA_HOME": "/path/to/your/java/home",
    },
    "mainClass": "org.apache.spark.deploy.SparkSubmit",
    "sparkProperties": {
        "spark.submit.pyFiles": "/path/to/your/python/script.py",  # Change this to your Python script location
        "spark.driver.supervise": "false",
        "spark.app.name": "PythonScript",
    },
    "sparkConf": {},
}

# Send the job submission request to the Spark REST API
response = requests.post(f"{api_url}/v1/submissions/create", json=job_request)

# Check if the job submission was successful
if response.status_code == 200:
    submission_response = response.json()
    submission_id = submission_response["submissionId"]
    print(f"Job submitted successfully. Submission ID: {submission_id}")

    # Wait for the job to complete (you may implement this part based on your requirements)

    # Get the job result
    result_response = requests.get(f"{api_url}/v1/submissions/status/{submission_id}")
    result_json = result_response.json()
    if "driverState" in result_json and result_json["driverState"] == "FINISHED":
        result_url = result_json["driverLogs"]
        result_response = requests.get(result_url)
        result = result_response.text
        print(f"Job result: {result}")
    else:
        print(f"Job failed. Driver state: {result_json.get('driverState')}")
else:
    print(f"Job submission failed. Status code: {response.status_code}")
    print(response.text)
