# Data Engineering Zoomcamp

Link to Data Engineerin Zoomcamp 2025 playlist: https://www.youtube.com/playlist?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb
Link to homework w2: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw2
Course management: https://courses.datatalks.club/de-zoomcamp-2025/

## Week 1 - Docker

### 1.2.1 Intro to Docker

Why should we care about Docker?
- Local experiments
- Integration tests (CI/CD)
- Reproducibility
- Running pipelines on the cloud (take Docker image and run on AWS Batch, Kubernetes jobs)
- Spark
- Serverless (AWS Lambda, Google functions)

Docker commands:
- `-it` means interactive mode, so you can interact with your Docker container
- `docker run -it --entrypoint=bash python:3.9` to run a python image
- `docker build -t test:pandas .`
    - `.` means it will build it in this directory

### 1.2.2 Ingesting data to Postgres

To setup a Postgres Docker container:
```bash
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13
```

SQLAlchamy is a Python library to deal with SQL. PGAdmin is a web-based GUI tool to interact with Postgres databases.

To setup a PGAdmin Docker container:
```bash
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    dpage/pgadmin4
```

With a Docker Network we can connect two Docker containers with each other. You can say, this container should run in this network.

To setup this, use:
```bash
docker network create pg-network

docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13
```

### 1.2.3 - Connecting pgAdmin and Postgres

To run PgAdmin in Docker:
```bash
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    dpage/pgadmin4
```

To run Docker container inside a Docker Network:
```bash
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name pg-admin \
    dpage/pgadmin4
```

### 1.2.4 Dockerizing the Ingestion Script

```bash
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name==yellow_taxi_trips \
    --url=${URL}
```

```
docker build -t taxi_ingest:v001 .
```


```bash
docker run -it \
  --network=pg-network \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name==yellow_taxi_trips \
    --url=${URL}
```

- `${URL}` takes the environment variable URL. To check this run: `echo $URL`. To set a environment variable for the current session, use ` export URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz"`.
- `it` means interactive mode


`docker-compose up` to run the `docker-compose.yaml` file. 

## WEEK 1 - TERRAFORM

### 1.3.1. TERRAFORM PRIMER

Terraform = an infrastructure as code tool that lets you  define both cloud and on-prem resouces in human-readable configuration files that you can version, reuse and share.

Why use Terraform?
- Simplicity in keeping track of infrastructure (by having human-readbale configuration files)
- Easier collaboration (part of development cycle)
- Reproducibility
- Ensure resources are removed (once you're done you can easily delete resources so you're not charged for it)

What Terraform is not:
- Does not manage and update code on infra
- Does not give you the ability to change immutable resources
- Not used to manage resources not defined in your terraform files

Key Terraform commands:
- Init - Get me the provider I need
- Plan - What am I about to do?
- Apply - Do what is in the tf files (build it)
- Destroy - Remove everything defined in the tf files

LEFT AT VIDEO 1.3.2: Terraform basics

DEADLINE FOR HOMEWORK 2: THURSDAY 6 OF FEBRUARY 00:00

### 1.4.1. Setting up the Environment on Google Cloud

To connect with Google Cloud VM:
- Go to Google Cloud and turn [VM instance](https://console.cloud.google.com/compute/instances?onCreate=true&hl=nl&inv=1&invt=Abo8UQ&project=sandbox-450016) on
- Open iTerm and enter the command: `ssh -i ~/.ssh/gcp lars@34.34.153.74`
    - You are connected with your VM. To check the properties of the VM, use: `htop`
    - The VM already has the Google Cloud SDK (check with: `gcloud --version`)
- The VM is empty. We have to configure it:
    - Download Anaconda: `wget https://repo.anaconda.com/archive/Anaconda3-2024.10-1-Linux-x86_64.sh`
    - Install it with: `bash Anaconda3-2024.10-1-Linux-x86_64.sh`
    - Next, install Docker with: `sudo apt-get install docker.io`
        - To run Docker without `sudo`:
            - `sudo groupadd docker`
            - `sudo gpasswd -a $USER docker`
            - `sudo service docker restart`
            - Logout of the VM
    - Get the course repo: `git clone https://github.com/DataTalksClub/data-engineering-zoomcamp.git`
    - Install docker-compose:
        - Make new directory for executions files: `mkdir bin`
        - Move to new directory: `cd bin`
        - Get latest docker-compose (checkout the repo): `wget wget https://github.com/docker/compose/releases/download/v2.32.4/docker-compose-linux-x86_64`
        - Make the file executable with: `chmod +x docker-compose`
        - Make it visible from any directory by adding it to the path variable:
            - Open bash startup file: `nano .bashrc`
            - Go to the end and add: `export PATH="${HOME}/bin:${PATH}"`
            - Press Ctrl+O to save it, Enter, and Ctrl-X to exit the screen
    - Install PGcli with `pip install pgcli`
    - Install Terraform in the `bin` directory: `wget https://releases.hashicorp.com/terraform/1.10.5/terraform_1.10.5_linux_386.zip`


Setting a config to connect with the VM:
- Go to the `~/.ssh` folder
- Make a SSH config file with the command: `touch config`
- Open the config file: `code config` and put the following content:

```bash
Host de-zoomcamp
    HostName 34.34.153.74
    User lars
    IdentityFile ~/.ssh/gcp
```

Now, when you type: `ssh de-zoomcamp` it uses the identifyfile specified above to connect with your VM.

Usefull commands in Ubuntu VM:
- `less .bashrc` - To watch the startup file the VM runs when it starts up
- `logout` - To logout of your VM (ctrl-D)
- `sudo apt-get update` - To fetch the latest packages
- `mkdir bin` - To create a folder with all the executable files
- `-O` - Flag to specify the output name
- `chmod +x docker-compose` - To give the docker-compose execution permissions (green means its executable)

## WEEK 2 - Workflow Orchestration

### 2.2.1 - Workflow Orchestration Introduction

What is Kestra?
- All-in-one orchestration platform
- ETL / API orchestration / scheduled & event-driven workflows / batch data pipelines
- Options for no code, low code or full code
- Mutiple programming languages
- Monitor all your workflows and executions

What we cover:
- Introduction to Kestra
- ETL: Extra data and load it to Postgress
- ETL: Extra data and load it to Google Cloud
- Parameterizing Execution
- Scheduling and backfills

dbt = data build tool


