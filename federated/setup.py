from setuptools import setup, find_packages

setup(
    name='federated',
    packages=find_packages(),
    install_requires=[
        'dsub==0.3.6',
        'future==0.18.2',
        'configparser==5.0.0',
        'google-cloud-storage==1.30.0',
        'boto3==1.14.38',
        'azure-storage-blob==12.13.0',
        'azure-identity==1.16.1',
        'azure-batch==10.0.0',
        'azure-mgmt-compute==18.0.0',
        'retry==0.9.2',
    ]
)
