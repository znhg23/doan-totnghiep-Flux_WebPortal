import os
import subprocess
import json
import flask
import re
import shutil
import io
import concurrent.futures
import time
import zipfile
import pymongo
import database

PWD = os.getcwd()

#########################################
# UTILITIES FUNCTIONS
#########################################

def saveFluxStreamOutputToFile(jobID):
    """
    Save the stream output of a flux job to a file.
    """
    job = getSpecificFluxJob(jobID)
    dirName = job.get('cwd')
    
    # Move to the job directory
    os.chdir(dirName)
    
    result = subprocess.run(f"flux job attach {jobID}", shell=True, capture_output=True, text=True)
    
    # Save the stdout to a output_stream.txt file
    with open('output_stream.txt', 'w') as f:
        f.write(result.stdout)
            
    # Save the stderr to a error_stream.txt file
    with open('error_stream.txt', 'w') as f:
        f.write(result.stderr)
        
    os.chdir(PWD)
        
def getFluxOverlayStatus():
    """
    Get the overlay status of the Flux handle.
    """
    result = subprocess.run("flux overlay status", shell=True, capture_output=True, text=True)
    return result.stdout

def getFluxNodes():
    """
    Get all nodes known to the Flux handle.
    """
    try:
        data = database.get_all_flux_nodes()
            
        return data
    except Exception as e:
        print(f"Error getting nodes information: {e}")
        return []

def convertF58toDecimal(jobID):
    """
    Convert a Flux job ID from F58 format to decimal format.
    """
    conversion = subprocess.run(f"flux job id {jobID}", shell=True, capture_output=True, text=True)
    return conversion.stdout.rstrip('\n')
    
def getSpecificFluxNode(node):
    """
    Get a specific node from the Flux handle.
    """
    try:
        data = database.get_flux_node(node)
        return data
    except Exception as e:
        print(f"Error getting node information: {e}")
        return None
    
def getFluxJobs():
    """
    Get all jobs known to the Flux handle.
    """
    try:
        data = database.get_all_flux_jobs()
        return data
    except Exception as e:
        print(f"Error getting jobs information: {e}")
        return []
    
def getSpecificFluxJob(jobID):
    """
    Get a specific job from the Flux handle.
    """
    try:
        data = database.get_flux_job(jobID)
        return data
    except Exception as e:
        print(f"Error getting job information: {e}")
        return None
    
def submitFluxJob(jobName, jobCommand, dirName, options):
    """
    Submit a job to the Flux handle.
    """
    try:
        nodes = f"-N {options.get('nodes', 1)}" if options.get('nodes', None) else ""
        
        # Per resource options
        cores = f"--cores={options.get('cores', 6)}" if options.get('cores', None) else ""
        tasks_per_node = f"--tasks-per-node={options.get('tasks-per-node', 1)}" if options.get('tasks-per-node', None) else ""
        tasks_per_core = f"--tasks-per-core={options.get('tasks-per-core', 1)}" if options.get('tasks-per-core', None) else ""
        
        # Per task options
        cores_per_task = f"-c {options.get('cores_per_task', 2)}" if options.get('cores_per_task', None) else ""
        gpus_per_task = f"-g {options.get('gpus-per-task', 1)}" if options.get('gpus-per-task', None) else ""
        ntasks = f"-n {options.get('ntasks', 1)}" if options.get('ntasks', None) else ""
        
        os.makedirs(f"{PWD}/data/{dirName}", exist_ok=True)
        os.chdir(f"{PWD}/data/{dirName}")
        
        result = subprocess.run(f"flux submit --cwd=/mnt/shared --job-name={jobName} {nodes} {cores_per_task} {gpus_per_task} {ntasks} {cores} {tasks_per_node} {tasks_per_core} {jobCommand}", shell=True, capture_output=True, text=True)
        os.chdir(PWD)
        return result.stdout, result.stderr
    except Exception as e:
        print(f"Error submitting job: {e}")
        return None, str(e)
    
def uploadFiles(dirName, files):
    """
    Upload files to the /data directory.
    """
    try:
        # Create a new directory for the files
        os.makedirs(f"{PWD}/data/{dirName}", exist_ok=True)
        
        # Handle single file or multiple files
        if not isinstance(files, list):
            files = [files]
            
        # Save the files to the directory   
        for file in files:
            if file and file.filename:
                file_path = os.path.join(f"{PWD}/data/{dirName}", file.filename)
                file.save(file_path)
            
        return f"Uploaded {len(files)} files to {dirName} directory"
    except Exception as e:
        print(f"Error uploading files: {e}")
        raise Exception(f"Error uploading files: {e}")
    
def downloadFiles(jobID):
    """
    Download files from the /data/<dirName> directory as zip file.
    """
    try:
        saveFluxStreamOutputToFile(jobID)
        
        job = getSpecificFluxJob(jobID)
        dirName = job.get('cwd')
        
        # Get the base directory name
        base_dir = os.path.basename(dirName)
        
        # Create a zip file
        with zipfile.ZipFile(f"{base_dir}.zip", 'w') as zipf:
            for file in os.listdir(dirName):
                file_path = os.path.join(dirName, file)
                # Add file to zip with just the filename (no path)
                zipf.write(file_path, file)
                
        return f"{base_dir}.zip"
    except Exception as e:
        print(f"Error downloading files: {e}")
        raise Exception(f"Error downloading files: {e}")
    
def deleteFiles(dirName):
    """
    Delete files from the /data/<dirName> directory.
    """
    try:
        shutil.rmtree(f"{PWD}/data/{dirName}")
        return f"Deleted {dirName} directory"
    except Exception as e:
        print(f"Error deleting files: {e}")
        raise Exception(f"Error deleting files: {e}")
    
def showTree(dirName):
    """
    Show the tree of the cwd of a directory.
    """
    try:
        result = subprocess.run(f"tree -lah {PWD}/data/{dirName}", shell=True, capture_output=True, text=True)
        
        if result.stderr:
            raise Exception(f"Error showing tree: {result.stderr}")
        
        return result.stdout
    except Exception as e:
        raise Exception(f"Error showing tree: {e}")
    
def showFluxOverlayStatus():
    """
    Show the overlay status of the Flux handle.
    """
    try:
        result = subprocess.run("flux overlay status", shell=True, capture_output=True, text=True)
        return result.stdout
    except Exception as e:
        raise Exception(f"Error showing overlay status: {e}")
    
########################################
# APIs for the web portal
########################################
    
# Start the server at port 8080 then receive requests to process
app = flask.Flask(__name__)
@app.route('/flux/drain', methods=['PUT'])
def drainFlux():
    """Drain (Disable) a flux node."""
    """
    output:
    {
        "message": "Drained node successfully"
    }
    """
    
    node = flask.request.get_json().get('hostname')
    
    result = subprocess.run(f"flux resource drain {node}", shell=True, capture_output=True, text=True)
    
    if result.stderr:
        return flask.jsonify({"error": result.stderr}), 500
    
    return flask.jsonify({"message": "Drained node successfully"}), 200

@app.route('/flux/undrain', methods=['PUT'])
def undrainFlux():
    """Undrain (Enable) a flux node."""
    """
    output:
    {
        "message": "Undrained node successfully"
    }
    """
    
    node = flask.request.get_json().get('hostname')
    
    result = subprocess.run(f"flux resource undrain {node}", shell=True, capture_output=True, text=True)
    
    if result.stderr:
        return flask.jsonify({"error": result.stderr}), 500
    
    return flask.jsonify({"message": "Undrained node successfully"}), 200

@app.route('/flux/nodes', methods=['GET'])
def getFluxInstances():
    """Get all flux instances and their details."""
    """
    output:
    {
        "nodes": [
            {
                "hostname": "flux-1",
                "role": "ready",
                ...
            },
            ...
        ]
    }
    """
    
    fluxNodesInfo = getFluxNodes()
    
    if fluxNodesInfo is []:
        return flask.jsonify({"error": "No flux nodes found"}), 500
    
    return flask.jsonify({"nodes": fluxNodesInfo}), 200

@app.route('/flux/nodes/<hostname>', methods=['GET'])
def getFluxNodeAPI(hostname):
    """Get a specific flux node and its details."""
    """
    output:
    {
        "node": {
            "hostname": "flux-1",
            "role": "ready",
            ...
        }
    }
    """
    
    # Get the flux instance information
    fluxNodeInfo = getSpecificFluxNode(hostname)
    
    if fluxNodeInfo is None:
        return flask.jsonify({"error": "fluxNode does not exist"}), 404
    
    return flask.jsonify(fluxNodeInfo), 200

@app.route('/flux/jobs', methods=['POST'])
def submitJob():
    """Submit a job to the Flux handle."""
    """
    input:
    {
        "jobName": "job-1",
        "jobCommand": "echo 'Hello, World!'",
        "dirName": "job-1",
        "options": {
            "nodes": 1,
            "cores": 4,
            "tasks-per-node": 1,
            "tasks-per-core": 1,
        }
    }
    """
    
    jobName = flask.request.get_json().get('jobName')
    if jobName is None:
        return flask.jsonify({"error": "Job name is required"}), 400
    
    jobCommand = flask.request.get_json().get('jobCommand')
    if jobCommand is None:
        return flask.jsonify({"error": "Job command is required"}), 400
    
    dirName = flask.request.get_json().get('dirName')
    if dirName is None:
        return flask.jsonify({"error": "Directory name is required"}), 400
    
    options = flask.request.get_json().get('options', {})
    
    stdout, stderr = submitFluxJob(jobName, jobCommand, dirName, options)
    
    if stderr:
        return flask.jsonify({"error": stderr}), 500
    
    jobid = convertF58toDecimal(stdout.rstrip('\n'))
    
    return flask.jsonify({"message": "Job submitted successfully", "id": jobid}), 200

@app.route('/flux/jobs/<jobID>/cancel', methods=['PUT'])
def cancelJob(jobID):
    """Cancel a specific job."""
    """
    output:
    {
        "message": "Job cancelled successfully"
    }
    """
    
    result = subprocess.run(f"flux job cancel {jobID}", shell=True, capture_output=True, text=True)
    
    if result.stderr:
        return flask.jsonify({"error": result.stderr}), 500
    
    return flask.jsonify({"message": "Job cancelled successfully"}), 200

@app.route('/flux/jobs', methods=['GET'])
def getJobs():
    """Get all jobs."""
    """
    output:
    {
        "jobs": [
            {
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
        },
        ...
        ]
    }
    """
    
    jobs = getFluxJobs()
    
    if jobs is []:
        return flask.jsonify({"error": "No jobs found"}), 500
    
    return flask.jsonify({"jobs": jobs}), 200

@app.route('/flux/jobs/<jobID>', methods=['GET'])
def getJob(jobID):
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
    
    if jobID is None:
        return flask.jsonify({"error": "Job ID is required"}), 400
    
    job = getSpecificFluxJob(jobID)
    
    if job is None:
        return flask.jsonify({"error": "Job not found"}), 404
    
    return flask.jsonify({"job": job}), 200

@app.route('/flux/jobs/<jobID>/output', methods=['GET'])
def getJobOutput(jobID):
    """Get the output of a specific job."""
    """
    output:
    {
        "result": {
            "status": "success",
            "stdout": <stdout>,
            "stderr": <stderr>
        }
    }
    or
    A zip file containing the job results and output files
    """
    
    job = getSpecificFluxJob(jobID)
    
    if job is None:
        return flask.jsonify({"error": "Job not found"}), 404
    
    result = subprocess.run(f"flux job attach {jobID}", shell=True, capture_output=True, text=True)
    
    if flask.request.args.get('download') == 'true':
        try:
            # Download the job results and output files
            zipFile = downloadFiles(jobID)
            return flask.send_file(zipFile, as_attachment=True), 200
        except Exception as e:
            return flask.jsonify({"error": str(e)}), 500
    
    return flask.jsonify({"result": {"status": job.get('state'), "stdout": result.stdout, "stderr": result.stderr}}), 200

@app.route('/flux/tree', methods=['GET'])
def getJobTree():
    """Get the tree of the cwd of a job."""
    
    dirName = flask.request.args.get('dirName')
    if dirName is None:
        return flask.jsonify({"error": "Directory name is required"}), 400
    
    try:
        tree = showTree(dirName)
        return tree, 200
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500

@app.route('/flux/files', methods=['POST'])
def uploadFilesAPI():
    """Upload files to the /data directory."""
    
    dirName = flask.request.args.get('dirName')
    files = flask.request.files.getlist('files')  # Use getlist to handle multiple files
    
    if dirName is None:
        return flask.jsonify({"error": "Directory name is required"}), 400
    
    if not files:
        return flask.jsonify({"error": "No files uploaded"}), 400
    
    try:
        message = uploadFiles(dirName, files)
        return flask.jsonify({"message": message}), 200
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500
    
@app.route('/flux/files', methods=['DELETE'])
def deleteFilesAPI():
    """Delete files from the /data directory."""
    
    dirName = flask.request.args.get('dirName')
    try:
        message = deleteFiles(dirName)
        return flask.jsonify({"message": message}), 200
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500

@app.route('/flux/overlay', methods=['GET'])
def getFluxOverlayStatus():
    """Get the overlay status of the Flux handle."""
    try:
        status = showFluxOverlayStatus()
        return status, 200
    except Exception as e:
        return flask.jsonify({"error": str(e)}), 500

# Host the server at port 8080
if __name__ == '__main__':
    subprocess.run(f"sudo chmod 777 -R {PWD}", shell=True)
    
    app.run(host='0.0.0.0', port=8080)
    database.init_db()