#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-kafka",
    version="0.0.1+epoch8.2",
    description="Singer.io tap for extracting data from kafka",
    author="Stitch",
    url="https://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        "python-snappy",
        "lz4",
        "certifi",
        "kafka-python",
        "thriftrw",
        "singer-python==5.8.1",
        "requests==2.12.4",
        "strict-rfc3339==0.7",
        "nose==1.3.7",
        "jsonschema==2.6.0",
    ],
    entry_points="""
        [console_scripts]
        tap-kafka=tap_kafka:main
    """,
    packages=["tap_kafka"],
)
