import pymongo
import subprocess
import json
from bson.objectid import ObjectId

# Initialize MongoDB client and database
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["flux_db"]

def parse_flux_resource_list_to_json(output):
    """
    Parse the Flux resource list output into a JSON object.
    Input format:
    STATE NNODES NCORES NGPUS NODELIST
    down      1      2     0    hpqc-master
    """
    lines = output.strip().split('\n')
    if len(lines) < 2:
        return {
            "nodes": {"free": 0, "allocated": 0, "down": 0},
            "cores": {"free": 0, "allocated": 0, "down": 0},
            "gpus": {"free": 0, "allocated": 0, "down": 0}
        }

    # Initialize the result structure
    result = {
        "nodes": {"free": 0, "allocated": 0, "down": 0},
        "cores": {"free": 0, "allocated": 0, "down": 0},
        "gpus": {"free": 0, "allocated": 0, "down": 0}
    }

    # Skip header line and process each line
    for line in lines[1:]:
        parts = line.strip().split()
        if len(parts) >= 5:
            state = parts[0]
            nnodes = int(parts[1])
            ncores = int(parts[2])
            ngpus = int(parts[3])

            # Update counts based on state
            if state in ["free", "allocated", "down"]:
                result["nodes"][state] += nnodes
                result["cores"][state] += ncores
                result["gpus"][state] += ngpus

    return result

def load_flux_nodes(flux_nodes_collection):
    flux_nodes_collection.delete_many({})
    # Query all nodes from Flux
    result = subprocess.run("flux hostlist -e instance", shell=True, capture_output=True, text=True)
    if result.stderr:
        print(f"Error getting hostlist: {result.stderr}")
        return
    
    nodes = result.stdout.strip().split()
    if not nodes:
        print("No nodes found in hostlist")
        return
    
    # Insert the first node as the leader of the cluster
    # Parse the resource info of the first node
    result = subprocess.run(f"flux resource list -i {nodes[0]}", shell=True, capture_output=True, text=True)
    if result.stderr:
        print(f"Error getting resource info for {nodes[0]}: {result.stderr}")
        return
    
    resource_info = parse_flux_resource_list_to_json(result.stdout)
    state = subprocess.run(f"flux resource status --no-header -i {nodes[0]}", shell=True, capture_output=True, text=True)
    if state.stderr:
        print(f"Error getting resource status for {nodes[0]}: {state.stderr}")
        return
    
    state_info = state.stdout.strip().split()[0]
    
    query = {
        "hostname": nodes[0]
    }
    
    update = {
        "hostname": nodes[0],
        "role": "leader",
        "resource_info": resource_info,
        "status": state_info
    }
    
    flux_nodes_collection.update_one(query, {"$set": update}, upsert=True)
    
    # Insert the rest of the nodes
    for node in nodes[1:]:
        result = subprocess.run(f"flux resource list -i {node}", shell=True, capture_output=True, text=True)
        if result.stderr:
            print(f"Error getting resource info for {node}: {result.stderr}")
            continue
            
        resource_info = parse_flux_resource_list_to_json(result.stdout)
        state = subprocess.run(f"flux resource status --no-header -i {node}", shell=True, capture_output=True, text=True)
        if state.stderr:
            print(f"Error getting resource status for {node}: {state.stderr}")
            continue
        
        state_info = state.stdout.strip().split()[0]
        
        query = {
            "hostname": node
        }
        
        update = {
            "hostname": node,
            "role": "worker",
            "resource_info": resource_info,
            "status": state_info
        }
        flux_nodes_collection.update_one(query, {"$set": update}, upsert=True)

def load_flux_jobs(flux_jobs_collection):
    flux_jobs_collection.delete_many({})
    result = subprocess.run("flux jobs -a --json", shell=True, capture_output=True, text=True)
    if result.stderr:
        print(f"Error getting jobs: {result.stderr}")
        return
        
    jsonOutput = json.loads(result.stdout)
    
    # Add the json output to the database
    for job in jsonOutput["jobs"]:
        flux_jobs_collection.update_one(job, {"$set": job}, upsert=True)

def get_all_flux_nodes():
    """
    Query all data from the flux_nodes_collection.
    Returns a list of all nodes with their information.
    """
    flux_nodes_collection = db["flux_nodes"]
    load_flux_nodes(flux_nodes_collection)
    nodes = list(flux_nodes_collection.find({}, {'_id': 0}))
    return nodes

def get_flux_node(hostname):
    """
    Query a specific node from the flux_nodes_collection.
    Returns the node with their information.
    """
    flux_nodes_collection = db["flux_nodes"]
    load_flux_nodes(flux_nodes_collection)
    node = flux_nodes_collection.find_one({"hostname": hostname}, {'_id': 0})
    return node

def get_all_flux_jobs():
    """
    Query all data from the flux_jobs_collection.
    Returns a list of all jobs with their information.
    """
    flux_jobs_collection = db["flux_jobs"]
    load_flux_jobs(flux_jobs_collection)
    jobs = list(flux_jobs_collection.find({}, {'_id': 0}))
    return jobs

def get_flux_job(job_id):
    """
    Query a specific job from the flux_jobs_collection.
    Returns the job with their information.
    """
    flux_jobs_collection = db["flux_jobs"]
    load_flux_jobs(flux_jobs_collection)
    
    # Convert job_id to integer and find the job
    job = flux_jobs_collection.find_one({"id": int(job_id)}, {'_id': 0})
    
    if job:
        # Convert any remaining ObjectId fields to strings
        job = {k: str(v) if isinstance(v, ObjectId) else v for k, v in job.items()}
    
    return job

def init_db():
    # Create a collection for the flux nodes
    flux_nodes_collection = db["flux_nodes"]
    flux_nodes_collection.create_index([("flux_node", pymongo.ASCENDING)])
    load_flux_nodes(flux_nodes_collection)
    
    flux_jobs_collection = db["flux_jobs"]
    flux_jobs_collection.create_index([("flux_job", pymongo.ASCENDING)])
    load_flux_jobs(flux_jobs_collection)
    