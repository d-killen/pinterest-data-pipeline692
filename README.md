# pinterest-data-pipeline692

## Contents

 1. Description
 1. Skills Covered
 1. Installation Instructions
 1. Usage Instructions
 1. Project File Structure
 1. License Information

## Description

This project emulates a data stream from pinterest of pins, their geolocation data and the users posting them.

It then processes this data via Databricks notebooks both as a batch and as a stream.

## Skills Covered

Learning achieved through this project includes:
- Experience in AWS
    - EC2 instances
    - MSK
    - S3 Buckets
    - API setup
    - Kinesis
    - Plugins
    - Connectors
- Experience with Python: 
    - function definition
    - class definitions
    - class attributes
    - SQLAlchemy module
- Experience with SQL
- Experience with git & GitHub:
    - Making local changes
    - Committing and pushing changes back to the cloud repository
- README creation and editing

## Installation Instructions

1. Download the project directory
1. Create the necessary infrastructure on AWS and update the scripts accordingly:
    - EC2 Instance
    - S3 Bucket
    - MSK Connector
    - Kinesis Streams
    - APIs
1. Upload both notebooks to Databricks
1. Create and store the required .pem file in the lcoal project directory

## Usage Instructions

1. Via the EC2 instance start the API
1. Run the emulation scripts
1. Run the processing notebooks

## Project File Structure

- Project Directory
    - README.md
    - user_posting_emulation.py
    - user_posting_emulation_streaming.py
    - 0a25072a5e0f_dag.py
    - batch_processing_notebook.ipynb
    - stream_processing_notebook.ipynb

## License Information

pinterest-data-pipeline692
Copyright (C) 2023 Daniel Killen

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
