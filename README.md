# Flux Web Portal

A web portal for managing Flux clusters, providing APIs for job submission, monitoring, and node management.

## API Documentation

### Node Management APIs

#### Get All Nodes
- **Endpoint**: `GET /flux/nodes`
- **Description**: Get all Flux nodes and their details
- **Response**: List of nodes with hostname, role, resource info, and status
- **Example Response**:
```json
{
    "nodes": [
        {
            "hostname": "node1",
            "role": "leader",
            "resource_info": {
                "nodes": {"free": 1, "allocated": 0, "down": 0},
                "cores": {"free": 4, "allocated": 0, "down": 0},
                "gpus": {"free": 1, "allocated": 0, "down": 0}
            },
            "status": "ready"
        }
    ]
}
```

#### Get Specific Node
- **Endpoint**: `GET /flux/nodes/<hostname>`
- **Description**: Get details of a specific node
- **Response**: Node information
- **Example Response**:
```json
{
    "hostname": "node1",
    "role": "leader",
    "resource_info": {
        "nodes": {"free": 1, "allocated": 0, "down": 0},
        "cores": {"free": 4, "allocated": 0, "down": 0},
        "gpus": {"free": 1, "allocated": 0, "down": 0}
    },
    "status": "ready"
}
```

#### Drain Node
- **Endpoint**: `PUT /flux/drain`
- **Description**: Drain (disable) a Flux node
- **Request Body**:
```json
{
    "hostname": "node1"
}
```
- **Response**: Success/error message

#### Undrain Node
- **Endpoint**: `PUT /flux/undrain`
- **Description**: Undrain (enable) a Flux node
- **Request Body**:
```json
{
    "hostname": "node1"
}
```
- **Response**: Success/error message

### Job Management APIs

#### Submit Job
- **Endpoint**: `POST /flux/jobs`
- **Description**: Submit a new job to Flux
- **Request Body**:
```json
{
    "jobName": "job-1",
    "jobCommand": "echo 'Hello, World!'",
    "dirName": "job-1",
    "options": {
        "nodes": 1,
        "cores": 4,
        "tasks-per-node": 1,
        "tasks-per-core": 1
    }
}
```
- **Response**: Job ID and success message

#### Get All Jobs
- **Endpoint**: `GET /flux/jobs`
- **Description**: Get all jobs
- **Response**: List of all jobs with their details

#### Get Specific Job
- **Endpoint**: `GET /flux/jobs/<jobID>`
- **Description**: Get details of a specific job
- **Response**: Job information

#### Cancel Job
- **Endpoint**: `PUT /flux/jobs/<jobID>/cancel`
- **Description**: Cancel a specific job
- **Response**: Success/error message

#### Get Job Output
- **Endpoint**: `GET /flux/jobs/<jobID>/output`
- **Description**: Get job output
- **Response**: Job output (stdout/stderr) or downloadable zip file

### File Management APIs

#### Upload Files
- **Endpoint**: `POST /flux/files`
- **Description**: Upload files
- **Parameters**: 
  - `dirName`: Directory name
- **Request**: Multipart form data with files
- **Response**: Success/error message

#### Delete Files
- **Endpoint**: `DELETE /flux/files`
- **Description**: Delete files from a directory
- **Parameters**: 
  - `dirName`: Directory name
- **Response**: Success/error message

#### Get Directory Tree
- **Endpoint**: `GET /flux/tree`
- **Description**: Show directory tree structure
- **Parameters**:
  - `dirName`: Directory name
- **Response**: Directory tree structure

### System Status APIs

#### Get Overlay Status
- **Endpoint**: `GET /flux/overlay`
- **Description**: Get Flux overlay status
- **Response**: Overlay status information

## Features

- Comprehensive job management (submit, monitor, cancel)
- Node management and resource control
- File operations and directory management
- Real-time job output monitoring
- Persistent storage using MongoDB
- RESTful API design
- Error handling with appropriate HTTP status codes

## Requirements

- Python 3.x
- Flask
- pymongo
- Flux cluster
- MongoDB

## Setup

1. Install dependencies:
```bash
pip install flask pymongo
```

2. Start MongoDB:
```bash
sudo systemctl start mongod
```

3. Run the server:
```bash
python server.py
```

The server will start on port 8080.