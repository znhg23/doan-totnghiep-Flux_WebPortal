import json
import os
import re
# import flux

FLUX_JSON = f'/src/flux.json'

# takes in arguments from the command line
# --add <fluxInstance> --remove <fluxInstance>

def convertFluxInstanceToFluxUri(fluxInstance):
    return f"local:///tmp/{fluxInstance}/local-0"

def convertFluxUriToFluxInstance(fluxUri):
    # Convert the flux URI to a flux instance
    regex = r"flux-[a-zA-Z0-9\-]+"
    fluxInsance = re.findall(regex, fluxUri)
    return fluxInsance[0] if fluxInsance else None

def addJobToFluxInstance(fluxInstance, jobID):
    with open(FLUX_JSON, 'r') as f:
        node_data = json.load(f)
    
    # Check if the flux instance exists
    if fluxInstance in node_data:
        # Check if "jobs" array exists, if not create it
        if "jobs" not in node_data[fluxInstance]:
            node_data[fluxInstance]["jobs"] = []
        
        # Add the job ID to the jobs array
        node_data[fluxInstance]["jobs"].append(jobID)
        
        with open(FLUX_JSON, 'w') as f:
            json.dump(node_data, f, indent=4)
        print(f"Added job {jobID} to {fluxInstance}.")
    else:
        print(f"Error: {fluxInstance} does not exist in node.json.")

def removeFluxInstance(fluxInstance):
    with open(FLUX_JSON, 'r') as f:
        node_data = json.load(f)
    # Check if the flux instance exists
    if fluxInstance in node_data:
        # Remove the flux instance from the node.json file
        del node_data[fluxInstance]
        with open(FLUX_JSON, 'w') as f:
            json.dump(node_data, f, indent=4)
        print(f"Removed {fluxInstance} from node.json.")
    else:
        print(f"Error: {fluxInstance} does not exist in node.json.")
        
def addFluxInstance(fluxInstance, pid, nodes):
    with open(FLUX_JSON, 'r') as f:
        node_data = json.load(f)
    
    # Check if the flux instance already exists
    if fluxInstance in node_data:
        return f"Flux instance {fluxInstance} already exists in node.json."
    else:
        # Add the flux instance to the node.json file
        int_id = len(node_data)
        node_data[fluxInstance] = {
            "id": int_id,
            "pid": pid,
        }
        with open(FLUX_JSON, 'w') as f:
            json.dump(node_data, f, indent=4)
        print(f"Added: {fluxInstance} to node.json.")
        
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Modify node.json file.")
    parser.add_argument('--add', type=str, help='Add a flux instance to node.json')
    parser.add_argument('--add-job', type=str, help='Add a job to a flux instance')
    parser.add_argument('--job-id', type=str, help='Add a job id to a flux instance')
    parser.add_argument('--remove', type=str, help='Remove a flux instance from node.json')
    parser.add_argument('--pid', type=int, help='Specify process ID of the flux instance')
    parser.add_argument('--nodes', type=int, help='Specify number of nodes for the flux instance')
    args = parser.parse_args()
    if args.add and args.pid:
        fluxInstance = convertFluxUriToFluxInstance(args.add)
        if args.nodes is not None:
            addFluxInstance(fluxInstance, args.pid, args.nodes)
        else:
            addFluxInstance(fluxInstance, args.pid, 1)
    elif args.add_job and args.job_id:
        addJobToFluxInstance(args.add_job, args.job_id)
    elif args.remove:
        fluxInstance = convertFluxUriToFluxInstance(args.remove)
        removeFluxInstance(fluxInstance)
    else:
        print("Please provide --add or --remove argument.")
#