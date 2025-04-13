import os
import subprocess
import json
import flux
import flask
import re
import shutil
import io
import concurrent.futures
import time
import zipfile
from flux.job import JobspecV1, JobInfo

PWD = os.getcwd()
FLUX_JSON = f'{PWD}/flux.json'
MODIFY_FLUX_JSON_SCRIPT = f'{PWD}/modifyNodeJson.py'
FLUX_INSTANCE_REGEX = r"flux-[a-zA-Z0-9\-]+"

#########################################
# UTILITIES FUNCTIONS
#########################################

def saveFluxStreamOutputToFile(fluxInstance, jobID):
    """
        This function is used to save the stream output of a flux job to a file.
        For example, if the job prints "Hello, world!" to the console, this function will save "Hello, world!" to a file.
        Because the flux library does not provide a way to get the console output, we need to use a workaround by connecting
        to the flux instance via an interactive shell by exporting the FLUX_URI, then run the command `flux job attach <jobID>`.
    """
    fluxUri = convertFluxInstanceToFluxUri(fluxInstance)
    # Move to the job directory
    os.chdir(f"{PWD}/{fluxInstance}/{jobID}")
    
    newShell = subprocess.Popen(f"export FLUX_URI={fluxUri}; flux job attach {jobID}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Save the stdout to a output_stream.txt file
    with open('output_stream.txt', 'w') as f:
        for line in newShell.stdout:
            f.write(line)
            
    # Save the stderr to a error_stream.txt file
    with open('error_stream.txt', 'w') as f:
        for line in newShell.stderr:
            f.write(line)
        

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
        
    # Create a new directory for the flux instance
    os.makedirs(f"{PWD}/{fluxInstance}", exist_ok=True)
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
    


########################################
# APIs for the web portal
########################################
    
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
    """Stop a flux instance and remove it from the node.json file, also remove the directory."""
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
    os.chdir(PWD)
    subprocess.run(f"python3 {MODIFY_FLUX_JSON_SCRIPT} --remove {fluxInstance}", shell=True)
    
    # Remove the flux instance directory
    shutil.rmtree(f"{PWD}/{fluxInstance}")
    
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
            ]
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
        
    # Create a new directory for the job
    jobDirPath = f"{PWD}/{fluxInstance}/temp"
    os.makedirs(jobDirPath, exist_ok=True)
    os.chdir(jobDirPath)
    
    if handle is None:
        return flask.jsonify({"error": "Failed to connect to flux instance"}), 500
    try:
        if job['command'] is not None:
            command = job['command']
            jobspec = JobspecV1.from_command(command)
            
        jobspec.cwd = os.getcwd()
        jobspec.environment = dict(os.environ)
            
        jobId = flux.job.submit(handle, jobspec).dec

        # Rename the temp directory to the job ID
        os.rename(jobDirPath, f"{PWD}/{fluxInstance}/{jobId}")
        return flask.jsonify({"jobId": int(jobId)}), 200
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500

@app.route('/flux/<fluxInstance>/job/script', methods=['POST'])
def submitJobWithScript(fluxInstance):
    """Submit a job to a specific flux instance."""
    """
    input: arguments
    `main-script`: .sh or .py file (required)
    `requirements`: requirements.txt file (optional)
    `data`: data.csv file (optional)
    """
    
    # Get the flux instance information
    fluxInstanceInfo = getFluxInstanceInfo(fluxInstance)
    if fluxInstanceInfo is None:
        return flask.jsonify({"error": "fluxInstance does not exist"}), 404
    # Get the job script from the request
    if 'main-script' not in flask.request.files:
        return flask.jsonify({'error': 'main-script is required'}), 400
    
    mainScript = flask.request.files['main-script']
    
    # Check if file was selected
    if mainScript.filename == '':
        return flask.jsonify({'error': 'No selected file for main-script'}), 400
    
    if 'requirements' in flask.request.files:
        requirements = flask.request.files['requirements']
    else:
        requirements = None
    
    if 'data' in flask.request.files:
        data = flask.request.files['data']
    else:
        data = None

    # Save the main script/requirements/data to a temporary directory
    tempDir = f"{PWD}/{fluxInstance}/temp{time.time()}"
    os.makedirs(tempDir, exist_ok=True)
    
    mainScriptPath = f"{tempDir}/{mainScript.filename}"
    requirementsPath = f"{tempDir}/requirements.txt"
    dataPath = tempDir
    
    # Save the main script/requirements/data to the temporary directory and grant permission
    if mainScript:
        mainScript.save(mainScriptPath)
        subprocess.run(f"chmod +x {mainScriptPath}", shell=True)
    
    if requirements:
        requirements.save(requirementsPath)
        subprocess.run(f"chmod +x {requirementsPath}", shell=True)
    
    if data:
        data.save(dataPath)
    
    os.chdir(tempDir)
    
    # Connect to the flux instance and submit the job
    handle = connectToFluxInstance(fluxInstance)
    
    if handle is None:
        return flask.jsonify({"error": "Failed to connect to flux instance"}), 500
    try:
        # Install requirements if exists
        if requirements:
            subprocess.run(f"pip install -r {requirementsPath}", shell=True)
        
        # Create a jobspec from the job script

        jobspec = JobspecV1.from_command(command=[f"./{mainScript.filename}"])
        jobspec.cwd = os.getcwd()
        jobspec.environment = dict(os.environ)
        
        def submit_callback(future, handle):
            try:
                jobId = future.get_id()
                # Rename the temp directory to the job ID
                result_fut = flux.job.result_async(handle, jobId)
                # attach a callback to fire when the job finishes
                result_fut.then(rename_temp_dir)
            except Exception as e:
                print(f"Error in callback: {str(e)}")
                
        def rename_temp_dir(future):
            jobInfo = future.get_info()
            os.rename(tempDir, f"{PWD}/{fluxInstance}/{jobInfo.id.dec}")
        
        future = flux.job.submit_async(handle, jobspec)
        future.then(submit_callback, handle)
        
        handle.reactor_run()
        
        jobId = future.get_id()
        return flask.jsonify({"jobId": jobId}), 200
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500

@app.route('/flux/<fluxInstance>/job/<jobID>/status', methods=['GET'])
def getJobStatus(fluxInstance, jobID):
    """Get the status of a specific job."""
    """
    output:
    {
        "job": {
            "id": 676292747853824,
            "userid": 0,
            "urgency": 16,
            "priority": 16,
            "t_submit": 1667760398.4034982,
            "t_depend": 1667760398.4034982,
            "state": "SCHED",
            "name": "sleep",
            "ntasks": 1,
            "ncores": 1,
            "duration": 0.0
        }
    }
    """
    
    # Get the flux instance information
    fluxInstanceInfo = getFluxInstanceInfo(fluxInstance)
    if fluxInstanceInfo is None:
        return flask.jsonify({"error": "fluxInstance does not exist"}), 404
    
    # Connect to the flux instance and get the job status
    handle = connectToFluxInstance(fluxInstance)
    if handle is None:
        return flask.jsonify({"error": "Failed to connect to flux instance"}), 500
    
    # Get the job status
    try:    
        jobStatus = flux.job.get_job(handle, jobID)
        return flask.jsonify({"job": jobStatus}), 200
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500

@app.route('/flux/<fluxInstance>/job/<jobID>/result', methods=['GET'])
def getJobResult(fluxInstance, jobID):
    """Get the result of a specific job."""
    """
    output:
    {
        "jobID": <job_id>,
        "details": {
            "stdout": <stdout>,
            "stderr": <stderr>
        }
    }
    or
    A zip file containing the job results and output files
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
        # Get job result as json object
        jobResultObject = flux.job.result(handle, jobID)
        jobResultJson = {
            't_submit': jobResultObject.t_submit,
            't_remaining': jobResultObject.t_remaining,
            'result': jobResultObject.result,
            'runtime': jobResultObject.runtime
        }
        saveFluxStreamOutputToFile(fluxInstance, jobID)
        
        # Check if output directory exists and has files
        job_output_dir = f"{PWD}/{fluxInstance}/{jobID}"
        
        if flask.request.args.get('download') == 'true' and os.path.exists(job_output_dir) and os.listdir(job_output_dir):
            # Create a memory file to store the zip
            memory_file = io.BytesIO()
            
            # Create a zip file containing job results and output files
            with zipfile.ZipFile(memory_file, 'w') as zf:
                # Add job result as json file
                result_json = json.dumps({
                    "jobId": jobID,
                    "details": jobResultJson
                })
                zf.writestr('job_result.json', result_json)
                
                # Add all files from the output directory
                for root, _, files in os.walk(job_output_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arc_name = os.path.relpath(file_path, job_output_dir)
                        zf.write(file_path, arc_name)
            
            # Seek to the beginning of the memory file
            memory_file.seek(0)
            
            return flask.send_file(
                memory_file,
                mimetype='application/zip',
                as_attachment=True,
                download_name=f'job_{jobID}_results.zip'
            )
        
        # If no output files exist, return just the job result
        return flask.jsonify({
            "jobId": jobID,
            "details": jobResultJson
        }), 200
        
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500

# Host the server at port 8080
if __name__ == '__main__':
    subprocess.run(f"sudo chmod 777 -R {PWD}", shell=True)
    app.run(host='0.0.0.0', port=8080)