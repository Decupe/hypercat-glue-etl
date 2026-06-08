from setuptools import setup, find_packages

setup(
    name="hypercat-glue-etl",
    version="0.1.0",
    packages=find_packages(
        include=["pipelines*", "shared*"]
    ),
)