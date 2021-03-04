# Swarm Federated

This is a Python script that allows federation of batch jobs across the following Cloud Providers:
* AWS
* Azure
* GCP

## Pre-requisites

- Make a copy of the provider's config file location in `conf/examples` directory.
- Populate the requires fields and place in the `conf/` directory.
- Python3

## Install Dependencies

It is recommended to use a Python Virtualenv. See instructions [here](https://docs.python.org/3/library/venv.html)

```bash
python3 install setup.py
```

## Run

Here is an example of how to run it across GCP (Platform 1) and Azure (Platform 2)

```bash
python3 main.py \
    --platform1 gcp \
    --image1 federatedswarm/batch:latest \
    --input1 gs://logging-swarm/input \
    --output1 gs://logging-swarm/output/Height.QC.Transformed \
    --logging1 gs://logging-swarm/logs/1 \
    --platform2 azure \
    --input2 swarm/input \
    --output2 swarm/output \
    --image2 federatedswarm/batch:latest \
    --verbose
```

## Docker

There is a Docker image available for use `federatedswarm/batch` butif you wish to build your custom image,
there is a Makefile in the [`.docker` directory](.docker/Makefile) that allows for that.
```bash
cd .docker
DOCKER_ORG=my_dockerhub_account make build push
```
