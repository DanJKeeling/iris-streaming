"""
Setup script for IRIS PySpark Streaming Connector.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="iris-pyspark-connector",
    version="0.1.0",
    author="Dan Keeling",
    author_email="danjkeeling@gmail.com",
    description="PySpark streaming connector for Elexon's IRIS AMQP service",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/iris-pyspark-connector",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.9",
    install_requires=[
        "azure-servicebus>=7.11.0",
        "azure-identity>=1.14.0",
        "pyspark>=3.4.0",
        "pydantic>=2.0.0",
        "pydantic-settings>=2.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "structlog>=23.1.0",
        ],
    },
)

