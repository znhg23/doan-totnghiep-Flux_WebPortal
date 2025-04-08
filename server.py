import os
import subprocess
import json
import flux
import flask
import re
from flux.job import JobspecV1, JobInfo

PWD = os.getcwd()
INPUT_DIR = f'{PWD}/input'
FLUX_JSON = f'{PWD}/flux.json'
MODIFY_FLUX_JSON_SCRIPT = f'{PWD}/modifyNodeJson.py'
FLUX_INSTANCE_REGEX = r"flux-[a-zA-Z0-9\-]+"

def checkFluxInstanceExists(fluxInstance):
    with open(FLUX_JSON, 'r') as f:
        node_data = json.load(f)
    if fluxInstance in node_data:
        return True
    else:
        return False
    
def convertFluxInstanceToFluxUri(fluxInstance):
    return f"local:///tmp/{fluxInstance}/local-0"
    
def connectToFluxInstance(fluxInstance):
    # Connect to the flux instance
    fluxUri = convertFluxInstanceToFluxUri(fluxInstance)
    try:
        handle = flux.Flux(fluxUri)
        return handle
    except Exception as e:
        print(f"Error connecting to flux instance {fluxInstance}: {e}")
        return None

def startNewFluxInstanceThenSleep():
    
    initialProgramCommand = f"sudo chmod 777 -R {PWD}; echo $$ > {PWD}/test.txt; python3 {MODIFY_FLUX_JSON_SCRIPT} --add $FLUX_URI --pid $$; sleep inf"  # TODO: modify permission for security
    # Start the Flux shell
    process = subprocess.Popen(f"flux start '{initialProgramCommand}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    if process.stdout:
        line = process.stdout.readline()
        if line.startswith("Added:"):
            fluxInstance = re.search(FLUX_INSTANCE_REGEX, line)[0]
        print(line) 
    return fluxInstance

def getFluxInstanceInfo(fluxInstance):
    with open(FLUX_JSON, 'r') as f:
        node_data = json.load(f)
        # Get the flux instance information
        if checkFluxInstanceExists(fluxInstance):
            fluxInstanceInfo = { fluxInstance: node_data[fluxInstance] }
        else:
            return None
    return fluxInstanceInfo

def getAllFluxInstancesInfo():
    with open(FLUX_JSON, 'r') as f:
        node_data = json.load(f)
        fluxInstancesInfo = []
        for fluxInstance in node_data:
            fluxInstanceInfo = {fluxInstance: node_data[fluxInstance]}
            fluxInstancesInfo.append(fluxInstanceInfo)
    return fluxInstancesInfo
    
# Start the server at port 8080 then receive requests to process
app = flask.Flask(__name__)
@app.route('/flux', methods=['POST'])
def startFlux():
    """Start a new flux instance with optional configs and return the instance ID and details."""
    """
    output:
    {
        "fluxInstance": <flux_instance_id>,
        "status": "ready",
        "pid": <pid>
        "details": {
            "number_of_cores": 4,
            "memory": 16
        }
    }
    """
    # Start a new flux instance
    fluxInstance = startNewFluxInstanceThenSleep()
    print(fluxInstance)
    with open(FLUX_JSON, 'r') as f:
        node_data = json.load(f)
        # Get the flux instance information
        if checkFluxInstanceExists(fluxInstance):
            fluxInstanceInfo = {fluxInstance : node_data[fluxInstance]}
        else:
            return flask.jsonify({"error": "Failed to start a new instance"}), 404
    
    return flask.jsonify(fluxInstanceInfo), 200

@app.route('/flux', methods=['DELETE'])
def stopFlux():
    """Stop a flux instance and remove it from the node.json file."""
    """
    input:
    {
        "fluxInstance": <flux_instance_id> // required
    }
    """
    
    """
    output:
    {
        "message": "flux_instance_id removed successfully"
    }
    """
    jsonData = flask.request.get_json()
    
    if not jsonData:
        return flask.jsonify({"error": "Invalid request"}), 400
    
    fluxInstance = jsonData['fluxInstance']
    
    if not fluxInstance:
        return flask.jsonify({"error": "fluxInstance is required"}), 400
    
    fluxInstanceInfo = getFluxInstanceInfo(fluxInstance)
    if fluxInstanceInfo is None:
        return flask.jsonify({"error": "fluxInstance does not exist"}), 404
    
    # Stop the flux instance
    subprocess.run(f"sudo kill -9 {fluxInstanceInfo[fluxInstance]['pid']}", shell=True)
    
    # Remove the flux instance from the node.json file
    subprocess.run(f"python3 {MODIFY_FLUX_JSON_SCRIPT} --remove {fluxInstance}", shell=True)
    
    return flask.jsonify({"message": f"{fluxInstance} removed successfully"}), 200

@app.route('/flux', methods=['GET'])
def getFluxInstances():
    """Get all flux instances and their details."""
    """
    output:
    {
        "fluxInstances": {
            "flux-1": {
                "id": 0,
                "status": "ready",
                "pid": 1234,
                "details": {
                    "number_of_cores": 4,
                    "memory": 16
                }
            },
            ...
        }
    }
    """
    
    fluxInstancesInfo = getAllFluxInstancesInfo()
    
    return flask.jsonify({"fluxInstances": fluxInstancesInfo}), 200

@app.route('/flux/<fluxInstance>', methods=['GET'])
def getFluxInstance(fluxInstance):
    """Get a specific flux instance and its details."""
    """
    output:
    {
        "fluxInstance": {
            "id": 0,
            "status": "ready",
            "pid": 1234,
            "details": {
                "number_of_cores": 4,
                "memory": 16
            }
        }
    }
    """
    
    fluxInstanceInfo = getFluxInstanceInfo(fluxInstance)
    
    if fluxInstanceInfo is None:
        return flask.jsonify({"error": "fluxInstance does not exist"}), 404
    
    return flask.jsonify(fluxInstanceInfo), 200

# @app.route('/flux/<fluxInstance>/resource', methods=['GET'])
# def getFluxInstanceResource(fluxInstance):
#     """Get the resource usage of a specific flux instance."""
#     """
#     output:
#     {
#         "fluxInstance": {
#             "cpu": 10,
#             "memory": 2048
#         }
#     }
#     """
#     # Get the flux instance information
#     fluxInstanceInfo = getFluxInstanceInfo(fluxInstance)
#     return flask.jsonify(fluxInstanceInfo), 200

@app.route('/flux/<fluxInstance>/job/command', methods=['POST'])
def submitJobWithCommand(fluxInstance):
    """Submit a job to a specific flux instance."""
    """
    input:
    {
        "cores": 4,
        "requirements": "requirements.txt",
        "job": {
            "command": [
                "bash",
                "./input/test.sh"
            ],
            "script": [
                "./input/test.sh"
            ],
            "data": [
                "./input/data.csv"
            ],
        }
    }
    """
    
    # Get the flux instance information
    fluxInstanceInfo = getFluxInstanceInfo(fluxInstance)
    
    if fluxInstanceInfo is None:
        return flask.jsonify({"error": "fluxInstance does not exist"}), 404
    
    jsonData = flask.request.get_json()
    
    if not jsonData:
        return flask.jsonify({"error": "Invalid request"}), 400
    
    job = jsonData['job']
    
    if not job:
        return flask.jsonify({"error": "job is required"}), 400
    
    # Connect to the flux instance and submit the job
    handle = connectToFluxInstance(fluxInstance)
    if handle is None:
        return flask.jsonify({"error": "Failed to connect to flux instance"}), 500
    try:
        if job['command'] is not None:
            command = job['command']
            jobspec = JobspecV1.from_command(command)
            
        jobspec.cwd = PWD
        jobspec.environment = dict(os.environ)
            
        jobId = flux.job.submit(handle, jobspec)
        return flask.jsonify({"jobId": jobId}), 200
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500

@app.route('/flux/<fluxInstance>/job/script', methods=['POST'])
def submitJobWithScript(fluxInstance):
    """Submit a job to a specific flux instance."""
    """
    input:
    data type: binary
    """
    
    # Get the flux instance information
    fluxInstanceInfo = getFluxInstanceInfo(fluxInstance)
    if fluxInstanceInfo is None:
        return flask.jsonify({"error": "fluxInstance does not exist"}), 404
    # Get the job script from the request
    if 'file' not in flask.request.files:
        return flask.jsonify({'error': 'No file part'}), 400
    
    jobScript = flask.request.files['file']
    
    # Check if file was selected
    if jobScript.filename == '':
        return flask.jsonify({'error': 'No selected file'}), 400

    # Save the job script to a temporary file
    jobScriptPath = os.path.join(INPUT_DIR, 'temp_job_script.sh')
    
    if jobScript:
        jobScript.save(jobScriptPath)
    
    # Connect to the flux instance and submit the job
    handle = connectToFluxInstance(fluxInstance)
    if handle is None:
        return flask.jsonify({"error": "Failed to connect to flux instance"}), 500
    try:
        # Create a jobspec from the job script
        print(jobScriptPath)
        jobspec = JobspecV1.from_command(command=[jobScriptPath])
        jobspec.cwd = PWD
        jobspec.environment = dict(os.environ)
        
        jobId = flux.job.submit(handle, jobspec)
        return flask.jsonify({"jobId": jobId}), 200
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500

@app.route('/flux/<fluxInstance>/job/<jobID>/result', methods=['GET'])
def getJobResult(fluxInstance, jobID):
    """Get the result of a specific job."""
    """
    output:
    {
        "jobID": <job_id>,
        "result": {
            "stdout": <stdout>,
            "stderr": <stderr>
        }
    }
    """
    
    # Get the flux instance information
    fluxInstanceInfo = getFluxInstanceInfo(fluxInstance)
    
    if fluxInstanceInfo is None:
        return flask.jsonify({"error": "fluxInstance does not exist"}), 404
    
    # Connect to the flux instance and get the job result
    handle = connectToFluxInstance(fluxInstance)
    if handle is None:
        return flask.jsonify({"error": "Failed to connect to flux instance"}), 500
    
    try:
        jobResult = flux.job.result(handle, jobID).result
        return flask.jsonify({"jobID": jobID, "result": jobResult}), 200
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500

# Host the server at port 8080
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
    subprocess.run(f"sudo chmod 777 -R {PWD}", shell=True)