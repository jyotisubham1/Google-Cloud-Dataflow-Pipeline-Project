from setuptools import find_packages, setup

setup(
    name='dataflow_project',
    version='0.0.1',
    description='A Dataflow pipeline project for GCP Data Engineer',
    author='Jyotisubham Panda',
    author_email='jyotisubham.panda@gmail.com',
    install_requires=[
        'apache-beam[gcp]',
        'google-cloud-bigquery'
    ],
    packages=find_packages(),
    include_package_data=True,
)
