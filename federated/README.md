# Swarm Federated

This is a Python script that allows federation of batch jobs across the following Cloud Providers:
* AWS
* Azure
* GCP

## Pre-requisites

- Make a copy of the provider's config file location in [conf/examples](conf/examples) directory.
- Populate the requires fields and place in the [conf](conf/) directory.
- Python3

## Install Dependencies

It is recommended to use a Python Virtualenv. See instructions [here](https://docs.python.org/3/library/venv.html)

```bash
python3 setup.py install
```

## Run

Here is an example of how to run it across GCP (Platform 1) and Azure (Platform 2)
```bash
python3 main.py \
    --platform1 gcp \
    --image1 federatedswarm/batch:latest \
    --input1 gs://logging-swarm/input \
    --output1 gs://logging-swarm/output \
    --logging1 gs://logging-swarm/logs/1 \
    --platform2 azure \
    --input2 swarm/input \
    --output2 swarm/output \
    --image2 federatedswarm/batch:latest \
    --verbose
```

Here is an example of how to run it across GCP (Platform 1) and AWS (Platform 2)
```bash
python3 main.py \
    --platform1 gcp \
    --image1 federatedswarm/batch:latest \
    --input1 gs://logging-swarm/input \
    --output1 gs://logging-swarm/output \
    --logging1 gs://logging-swarm/logs/1 \
    --platform2 aws \
    --input2 s3://swarm-federated/input \
    --output2 s3://swarm-federated/output \
    --image2 federatedswarm/batch:latest \
    --verbose
```


Here is an example of how to run it across GCP (Platform 1) and GCP (Platform 2)
```bash
python3 main.py \
    --platform1 gcp \
    --image1 federatedswarm/batch:latest \
    --input1 gs://logging-swarm/input \
    --output1 gs://logging-swarm/output/Height.QC.Transformed \
    --logging1 gs://logging-swarm/a7i/1 \
    --platform2 gcp \
    --input2 gs://logging-swarm/input/ \
    --output2 gs://logging-swarm/output \
    --image2 federatedswarm/batch:latest \
    --logging2 gs://logging-swarm/a7i/2 \
    --verbose
```

Additional Guides:
- [Getting Started on Cloud Providers](docs/getting-started.md)
- [Cloud Provider Requirements](docs/cloud-requirements.md)

## Docker

There is a Docker image available for use `federatedswarm/batch` but if you wish to build your custom image,
there is a Makefile in the [.docker directory](.docker/Makefile) that allows for that.
```bash
cd .docker
DOCKER_ORG=my_dockerhub_account make build push
```


## Experiment Results

| Platform1 -- Platform2 | Platform 1 Start  | Platform 1 Finish   | Platform 1 Duration | Platform 2 Start    | Platform 2 Finish   | Platform 2 Duration | Threshold R2 P BETA SE                                                      |
|------------------------|-------------------|---------------------|---------------------|---------------------|---------------------|---------------------|-----------------------------------------------------------------------------|
| GCP -- GCP             | 3/5/2021 15:53:06 |   3/5/2021 15:56:00 |             0:02:54 | 2021-03-05 15:56:12 | 2021-03-05 15:59:08 |             0:02:56 | 0.3 0.163846822223426 1.0258269251213e-25 47343.3224462967 4248.15235007587 |
| GCP -- AWS             | 3/5/2021 15:37:37 |   3/5/2021 15:40:24 |             0:02:47 | 2021-03-05 15:40:34 | 2021-03-05 15:46:41 |             0:06:07 | 0.3 0.163846822223426 1.0258269251213e-25 47343.3224462967 4248.15235007587 |
| GCP -- Azure           | 3/5/2021 16:27:55 | 2021-03-05 16:30:49 |             0:02:54 | 2021-03-05 16:33:03 | 2021-03-05 16:37:36 |             0:04:33 | 0.3 0.163846822223426 1.0258269251213e-25 47343.3224462967 4248.15235007587 |
